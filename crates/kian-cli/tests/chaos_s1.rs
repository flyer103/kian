//! S1 crash-recovery chaos suite (card C15): scripted `kill -9` fault
//! injection over the REAL assembled `kian serve` process at every
//! drain stage — after admit, after claim, mid-provider-turn,
//! mid-tool, before the end write, after the end write — then restart
//! and assert recovery.
//!
//! This is the resident proof of NoLostWork / NoHalfTerminated /
//! NoSplitBrainWrite under real process death.
//!
//! ## Determinism
//!
//! Kill staging is **marker-driven**, not timing-driven. Five of the
//! six points are structurally deterministic:
//!
//! - *after admit* — the drain is held mid-provider-turn (gated mock)
//!   on input A, then input B is queued: B's durable row with its wake
//!   folded is exactly the "admitted, not claimed-for" state;
//! - *mid-provider-turn* — the gated request holds the drain in the
//!   turn until killed;
//! - *mid-tool* — the scripted `exec sleep 60` tool call holds the
//!   drain between `tool/call` and `tool/result` for a minute;
//! - *before the end write* — the harness holds a sqlite
//!   `BEGIN IMMEDIATE` write lock, then releases the mock gate: the
//!   final answer is *delivered* to the serve process (the mock's
//!   `delivered` counter proves it) but every further append — the
//!   assistant message, `turn/end`, the end write — blocks on the
//!   lock. Death lands strictly between "model answered" and "answer
//!   recorded";
//! - *after the end write* — the terminal stream is frozen by
//!   construction.
//!
//! The remaining race-y boundary (*after claim*, between the claim
//! append and the completion of the input-take sweep) is widened by
//! admitting 64 inputs in one concurrent batch; the harness polls the
//! durable store, freezes the server with SIGSTOP, verifies the exact
//! stream shape (a frozen process cannot advance) and kills — an
//! overshoot is detected and re-run on a fresh session with bounded
//! attempts, never silently accepted. Seeds identify runs (temp-dir
//! tags, panic context) for reproducibility.
//!
//! ## Timing constants (relationship documented, per the card)
//!
//! `lease_ttl_ms = 2000`, `heartbeat_interval_ms = 400`, and the serve
//! recovery-sweep tick equals the heartbeat interval (see
//! `Server::serve`), so recovery detection is bounded by
//! `last_heartbeat + TTL + one tick ≤ kill + heartbeat + TTL + tick`
//! (~2.8 s here) plus restart/scheduling slack; the asserted bound is
//! 8.8 s. These constants live only in the test config — production
//! defaults are untouched.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{
    ConfigTuning, MockProvider, ScriptedResponse, force_no_proxy, spawn_scripted_provider,
    temp_dir, write_config_with,
};
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_core::state::{SessionState, project};

const LEASE_TTL_MS: u64 = 2_000;
const HEARTBEAT_MS: u64 = 400;
/// kill → takeover-claim bound: heartbeat skew + TTL + one sweep tick
/// (see module docs) + restart/scheduling slack.
const DETECTION_BOUND_MS: u128 = LEASE_TTL_MS as u128 + 2 * HEARTBEAT_MS as u128 + 6_000;
/// The after-claim take-sweep width: this many inputs are admitted in
/// one concurrent batch so the claim→take window spans tens of ms.
const AFTER_CLAIM_INPUTS: usize = 64;
/// Attempts for the race-y poll-catch boundary (fresh session each).
/// The claim→first-turn window is scheduler-timing-dependent (its width
/// is the first take-sweep, sub-millisecond; kian-61's binary-layout
/// shift alone moved wake timing enough to miss it at 20×3ms polling),
/// so: poll fast and retry generously — a miss is cheap, a false
/// catch is impossible (the stage predicate is exact).
const CATCH_ATTEMPTS: u32 = 60;
const SEED_BASE: u64 = 0xC15;

fn tuning() -> ConfigTuning {
    ConfigTuning {
        lease_ttl_ms: LEASE_TTL_MS,
        heartbeat_interval_ms: HEARTBEAT_MS,
        tools: vec!["exec".into()],
        exec_timeout_ms: 120_000,
    }
}

// ---------------------------------------------------------------------------
// Kill points
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillPoint {
    AfterAdmit,
    AfterClaim,
    MidProviderTurn,
    MidTool,
    BeforeEndWrite,
    AfterEndWrite,
}

const POINTS: [KillPoint; 6] = [
    KillPoint::AfterAdmit,
    KillPoint::AfterClaim,
    KillPoint::MidProviderTurn,
    KillPoint::MidTool,
    KillPoint::BeforeEndWrite,
    KillPoint::AfterEndWrite,
];

impl KillPoint {
    fn name(self) -> &'static str {
        match self {
            Self::AfterAdmit => "after-admit",
            Self::AfterClaim => "after-claim",
            Self::MidProviderTurn => "mid-provider-turn",
            Self::MidTool => "mid-tool",
            Self::BeforeEndWrite => "before-end-write",
            Self::AfterEndWrite => "after-end-write",
        }
    }

    fn index(self) -> u64 {
        POINTS.iter().position(|p| *p == self).expect("known") as u64
    }

    /// The mock script (request N gets entry N-1) and how many requests
    /// are gated (held) — request #1 gated = the mid-provider-turn hold.
    fn script(self) -> (Vec<ScriptedResponse>, usize) {
        let text = || ScriptedResponse::Text("All done.".into());
        match self {
            // A's request (#1) is gated; B is queued behind it; the
            // post-restart re-drive gets request #2.
            Self::AfterAdmit => (vec![text(), text()], 1),
            // One ungated final answer; the post-restart re-drive
            // re-asks (request #2).
            Self::AfterClaim => (vec![text(), text()], 0),
            Self::MidProviderTurn => (vec![text(), text()], 1),
            // Request #1 answers with a 60 s `exec sleep` tool call; the
            // post-restart re-drive gets the final answer.
            Self::MidTool => (
                vec![
                    ScriptedResponse::ToolCall {
                        id: "chaos-sleep".into(),
                        name: "exec".into(),
                        arguments: serde_json::json!({"argv": ["sleep", "60"]}),
                    },
                    text(),
                ],
                0,
            ),
            // The final answer is delivered while the harness freezes
            // the store — death lands before it is recorded; the
            // post-restart re-drive asks again.
            Self::BeforeEndWrite => (vec![text(), text()], 1),
            Self::AfterEndWrite => (vec![text()], 0),
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    dir: PathBuf,
    sqlite: PathBuf,
    base1: String,
    base2: String,
    config1: PathBuf,
    config2: PathBuf,
    mock: Arc<MockProvider>,
    store: kian_store::EventStore,
    client: reqwest::Client,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis()
}

async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    port
}

impl Harness {
    async fn new(point: KillPoint, seed: u64) -> Self {
        let (script, gated) = point.script();
        let (provider_base, mock) = spawn_scripted_provider(script, gated).await;
        let dir = temp_dir(&format!("c15-{}-{seed}", point.name()));
        let sqlite = dir.join("kian.sqlite");
        let port1 = free_port().await;
        let port2 = free_port().await;
        let config1 = write_config_with(
            &dir,
            "kian-boot1.yaml",
            &sqlite,
            &provider_base,
            &format!("127.0.0.1:{port1}"),
            &tuning(),
        );
        // boot #2 binds its own port (the killed process's socket may
        // still linger); same store.
        let config2 = write_config_with(
            &dir,
            "kian-boot2.yaml",
            &sqlite,
            &provider_base,
            &format!("127.0.0.1:{port2}"),
            &tuning(),
        );
        let store = kian_store::EventStore::open(&sqlite)
            .await
            .expect("harness store");
        Self {
            dir,
            sqlite,
            base1: format!("http://127.0.0.1:{port1}"),
            base2: format!("http://127.0.0.1:{port2}"),
            config1,
            config2,
            mock,
            store,
            client: reqwest::Client::new(),
        }
    }

    fn spawn_serve(&self, config: &Path, tag: &str) -> Child {
        let stderr =
            std::fs::File::create(self.dir.join(format!("{tag}.stderr"))).expect("stderr file");
        Command::new(env!("CARGO_BIN_EXE_kian"))
            .args(["serve", "--config", config.to_str().unwrap()])
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .env(common::KEY_ENV, "test-key")
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn kian serve")
    }

    async fn wait_ready(&self, base: &str) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let resp = self
                .client
                .post(format!("{base}/rpc"))
                .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "projection", "params": {"session": "00000000-0000-0000-0000-000000000000"}}))
                .timeout(Duration::from_secs(2))
                .send()
                .await;
            // Any HTTP answer (even the unknown-session JSON-RPC error)
            // means serving.
            if let Ok(r) = resp
                && r.status().as_u16() == 200
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn rpc(&self, base: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.client
            .post(format!("{base}/rpc"))
            .json(
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
            )
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("rpc send")
            .json()
            .await
            .expect("rpc json")
    }

    async fn create_session(&self, base: &str) -> SessionId {
        let out = self
            .rpc(base, "create_session", serde_json::json!({}))
            .await;
        SessionId(
            out["result"]["session"]
                .as_str()
                .expect("session id")
                .parse()
                .expect("uuid"),
        )
    }

    async fn submit(&self, base: &str, sid: SessionId, content: &str, key: Option<&str>) -> bool {
        let out = self
            .rpc(
                base,
                "submit",
                serde_json::json!({
                    "session": sid.0.to_string(),
                    "content": content,
                    "idempotency_key": key,
                }),
            )
            .await;
        out["result"]["deduplicated"].as_bool().expect("dedup flag")
    }

    async fn events(&self, sid: SessionId) -> Vec<Event> {
        self.store.rebuild(sid).await.expect("rebuild")
    }

    /// Waits until the mock has received (and holds, when gated) its
    /// request number `n`.
    async fn wait_mock_requests(&self, n: usize, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while self.mock.requests.load(Ordering::SeqCst) < n {
            assert!(
                tokio::time::Instant::now() < deadline,
                "mock provider never saw request {n} ({what})"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn cleanup(self) {
        // Best effort: killed processes have released their sqlite fds.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn kill9(child: &mut Child) {
    child.kill().expect("kill -9");
    let _ = child.wait();
}

fn sigstop(child: &Child) {
    let ok = Command::new("/bin/kill")
        .arg("-STOP")
        .arg(child.id().to_string())
        .status()
        .expect("run kill -STOP")
        .success();
    assert!(ok, "SIGSTOP failed");
}

fn sigcont(child: &Child) {
    let _ = Command::new("/bin/kill")
        .arg("-CONT")
        .arg(child.id().to_string())
        .status();
}

// ---------------------------------------------------------------------------
// The sqlite write lock (the before-end-write freeze)
// ---------------------------------------------------------------------------

/// A `BEGIN IMMEDIATE` transaction held on a dedicated connection: the
/// single WAL writer slot is taken, so the serve process's appends
/// block (busy-timeout) until [`WriteLock::release`]. Used to freeze a
/// drain deterministically between "provider response delivered" and
/// its next durable write.
struct WriteLock {
    conn: Option<sqlx::pool::PoolConnection<sqlx::Sqlite>>,
}

impl WriteLock {
    async fn acquire(sqlite: &Path) -> Self {
        let url = format!("sqlite:{}", sqlite.display());
        let pool = sqlx::SqlitePool::connect(&url).await.expect("lock pool");
        let mut conn = pool.acquire().await.expect("lock conn");
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&mut *conn)
            .await
            .expect("busy_timeout");
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .expect("BEGIN IMMEDIATE");
        Self { conn: Some(conn) }
    }

    async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            conn.detach();
        }
    }
}

// ---------------------------------------------------------------------------
// Stage predicates (pure, over the durable stream)
// ---------------------------------------------------------------------------

fn has(events: &[Event], kind: EventKind) -> bool {
    events.iter().any(|e| e.kind == kind)
}

fn count(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

/// `session/ended` carries a reason (struct variant) — count by pattern.
fn count_ended(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .count()
}

fn kinds_of(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|e| e.kind.as_str()).collect()
}

/// Poll outcome for one attempt of the after-claim boundary.
enum Catch {
    NotYet,
    Got,
    Overshot,
}

fn after_claim_status(events: &[Event]) -> Catch {
    let ended = count_ended(events) > 0;
    // Claim durable, input-take sweep still appending (or just
    // started): some inputs taken, the turn not yet driven.
    if has(events, EventKind::TurnStart) || ended {
        Catch::Overshot
    } else if has(events, EventKind::DrainClaimed)
        && count(events, EventKind::InputAdmitted) < AFTER_CLAIM_INPUTS
    {
        Catch::Got
    } else {
        Catch::NotYet
    }
}

// ---------------------------------------------------------------------------
// Drive to the kill point and kill -9 there
// ---------------------------------------------------------------------------

/// Returns (session, inputs submitted, wall-clock ms at the kill).
async fn kill_at_stage(
    h: &Harness,
    server: &mut Child,
    point: KillPoint,
    seed: u64,
) -> (SessionId, Vec<String>, u128) {
    match point {
        // Structurally deterministic: A holds the drain mid-provider-
        // turn (gated request #1); B is queued behind it — B's durable
        // row with its wake folded IS the after-admit state.
        KillPoint::AfterAdmit => {
            let sid = h.create_session(&h.base1).await;
            assert!(!h.submit(&h.base1, sid, "chaos task A", Some("a")).await);
            h.wait_mock_requests(1, "gated turn on A").await;
            let queued = h
                .rpc(
                    &h.base1,
                    "queue",
                    serde_json::json!({
                        "session": sid.0.to_string(),
                        "content": "chaos task B",
                        "idempotency_key": "b",
                    }),
                )
                .await;
            assert_eq!(queued["result"]["deduplicated"], false, "B admitted fresh");
            // B's row is durable and unconsumed; its wake folded onto
            // the in-flight drain.
            let eligible = h
                .store
                .eligible_inputs(sid, now_ms() as u64)
                .await
                .expect("eligible");
            assert_eq!(eligible.len(), 1, "exactly B pending: {eligible:?}");
            let kill_wall = now_ms();
            kill9(server);
            (
                sid,
                vec!["chaos task A".into(), "chaos task B".into()],
                kill_wall,
            )
        }

        // Deterministic hold: the gated request keeps the drain inside
        // the provider turn until killed.
        KillPoint::MidProviderTurn => {
            let sid = h.create_session(&h.base1).await;
            assert!(!h.submit(&h.base1, sid, "chaos task", None).await);
            h.wait_mock_requests(1, "gated provider request").await;
            let kill_wall = now_ms();
            kill9(server);
            (sid, vec!["chaos task".into()], kill_wall)
        }

        // Deterministic hold: the `exec sleep 60` tool keeps the drain
        // between `tool/call` and `tool/result` for a minute.
        KillPoint::MidTool => {
            let sid = h.create_session(&h.base1).await;
            assert!(!h.submit(&h.base1, sid, "chaos tool task", None).await);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                let events = h.events(sid).await;
                assert!(
                    !has(&events, EventKind::ToolResult),
                    "sleep tool finished too fast"
                );
                if has(&events, EventKind::ToolCall) {
                    let kill_wall = now_ms();
                    kill9(server);
                    return (sid, vec!["chaos tool task".into()], kill_wall);
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "tool/call never landed: {:?}",
                    kinds_of(&events)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        // Deterministic freeze: hold the WAL writer slot, then release
        // the gated final answer — it is delivered (mock `delivered`
        // ticks) but every subsequent append, the end write included,
        // blocks on our lock. Death lands between answer and record.
        KillPoint::BeforeEndWrite => {
            let sid = h.create_session(&h.base1).await;
            assert!(!h.submit(&h.base1, sid, "chaos final task", None).await);
            h.wait_mock_requests(1, "gated final answer").await;
            let lock = WriteLock::acquire(&h.sqlite).await;
            h.mock.released.store(true, Ordering::SeqCst);
            // The response flies; the serve process then blocks trying
            // to persist it (or fails on the lock — either way nothing
            // further can commit).
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            while h.mock.delivered.load(Ordering::SeqCst) < 1 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "final answer never delivered"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            let kill_wall = now_ms();
            kill9(server);
            lock.release().await;
            (sid, vec!["chaos final task".into()], kill_wall)
        }

        // Deterministic: the terminal stream is frozen by construction.
        KillPoint::AfterEndWrite => {
            let sid = h.create_session(&h.base1).await;
            assert!(!h.submit(&h.base1, sid, "chaos done task", None).await);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                let events = h.events(sid).await;
                if count_ended(&events) == 1 {
                    let kill_wall = now_ms();
                    kill9(server);
                    return (sid, vec!["chaos done task".into()], kill_wall);
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "session never ended: {:?}",
                    kinds_of(&events)
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        // The race-y boundary: 64 inputs admitted in one concurrent
        // batch widen the claim→take-sweep window to tens of ms;
        // poll-catch, SIGSTOP-freeze, verify, kill. Overshoots retry
        // on fresh sessions with bounded attempts.
        KillPoint::AfterClaim => {
            let mut last_note = String::new();
            for attempt in 1..=CATCH_ATTEMPTS {
                let sid = h.create_session(&h.base1).await;
                let mut jobs = Vec::new();
                for n in 0..AFTER_CLAIM_INPUTS as u32 {
                    let params = serde_json::json!({
                        "session": sid.0.to_string(),
                        "content": format!("after-claim-input-{n}"),
                        "idempotency_key": format!("ac-{n}"),
                    });
                    jobs.push(h.rpc(&h.base1, "submit", params));
                }
                futures::future::join_all(jobs).await;

                let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                loop {
                    let events = h.events(sid).await;
                    match after_claim_status(&events) {
                        Catch::NotYet => {}
                        Catch::Got => {
                            sigstop(server);
                            tokio::time::sleep(Duration::from_millis(60)).await;
                            let frozen = h.events(sid).await;
                            if let Catch::Got = after_claim_status(&frozen) {
                                let kill_wall = now_ms();
                                kill9(server);
                                eprintln!(
                                    "c15: killed after-claim (seed {seed}, attempt {attempt}) \
                                     with {} of {} inputs taken",
                                    count(&frozen, EventKind::InputAdmitted),
                                    AFTER_CLAIM_INPUTS
                                );
                                let contents: Vec<String> = (0..AFTER_CLAIM_INPUTS as u32)
                                    .map(|n| format!("after-claim-input-{n}"))
                                    .collect();
                                return (sid, contents, kill_wall);
                            }
                            eprintln!(
                                "c15: after-claim freeze race: frozen has {} events ({:?})",
                                frozen.len(),
                                kinds_of(&frozen)
                            );
                            sigcont(server);
                            break;
                        }
                        Catch::Overshot => {
                            last_note =
                                format!("attempt {attempt} overshot: {:?}", kinds_of(&events));
                            break;
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        last_note = format!("attempt {attempt} timed out: {:?}", kinds_of(&events));
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                // Stage not captured on this session: let it settle and
                // retry on a fresh one.
                sigcont(server);
                let _ = h
                    .rpc(
                        &h.base1,
                        "projection",
                        serde_json::json!({"session": sid.0.to_string()}),
                    )
                    .await;
            }
            panic!("c15: never captured stage after-claim (seed {seed}): {last_note}");
        }
    }
}

// ---------------------------------------------------------------------------
// Post-kill stage verification
// ---------------------------------------------------------------------------

/// The exact durable shape of the killed stage (plus, where relevant,
/// the live process state observed at the kill).
async fn verify_killed_stage(h: &Harness, point: KillPoint, sid: SessionId, events: &[Event]) {
    let kinds = kinds_of(events);
    // NoHalfTerminated at the kill: every kill leaves a legal stream.
    project(events).unwrap_or_else(|e| panic!("{}: killed stream corrupt: {e}", point.name()));
    match point {
        KillPoint::AfterAdmit => {
            // A was claimed and driven into the gated turn; B's admit
            // is the LAST durable fact: exactly A consumed, B pending.
            assert!(kinds.contains(&"drain/claimed"), "{kinds:?}");
            assert!(kinds.contains(&"turn/start"), "A mid-turn: {kinds:?}");
            assert_eq!(
                count(events, EventKind::InputAdmitted),
                1,
                "only A consumed: {kinds:?}"
            );
            let eligible = h
                .store
                .eligible_inputs(sid, now_ms() as u64)
                .await
                .expect("eligible");
            assert_eq!(eligible.len(), 1, "B still pending: {eligible:?}");
        }
        KillPoint::AfterClaim => {
            assert!(kinds.contains(&"drain/claimed"), "claim durable: {kinds:?}");
            assert!(!kinds.contains(&"turn/start"), "no turn yet: {kinds:?}");
            assert!(
                count(events, EventKind::InputAdmitted) < AFTER_CLAIM_INPUTS,
                "take sweep incomplete: {kinds:?}"
            );
        }
        KillPoint::MidProviderTurn => {
            assert!(kinds.contains(&"turn/start"), "{kinds:?}");
            assert!(
                kinds.contains(&"step/request"),
                "A2 audit copy durable before send: {kinds:?}"
            );
            assert!(!kinds.contains(&"assistant/message"), "{kinds:?}");
        }
        KillPoint::MidTool => {
            assert!(
                kinds.contains(&"tool/call"),
                "log-before-execute: {kinds:?}"
            );
            assert!(
                !kinds.contains(&"tool/result"),
                "tool still executing: {kinds:?}"
            );
        }
        KillPoint::BeforeEndWrite => {
            // The final answer was DELIVERED (mock counter) but is not
            // recorded and the end write never landed.
            assert!(
                h.mock.delivered.load(Ordering::SeqCst) >= 1,
                "the final answer must have been delivered"
            );
            assert!(kinds.contains(&"turn/start"), "{kinds:?}");
            assert!(
                kinds.contains(&"step/request"),
                "request persisted before it flew: {kinds:?}"
            );
            assert!(
                !kinds.contains(&"session/ended"),
                "end write not landed: {kinds:?}"
            );
        }
        KillPoint::AfterEndWrite => {
            assert_eq!(count_ended(events), 1, "{kinds:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// Shared recovery assertions
// ---------------------------------------------------------------------------

/// The S1 recovery contract, checked on the final stream:
/// - zero lost work: every submitted input consumed (at-least-once —
///   a kill between the `input/admitted` append and the C6
///   `mark_consumed` binding re-admits that one input into the
///   context on recovery; the designed event-driven-consumption
///   semantics, never a loss);
/// - exactly one end write, honestly `completed`;
/// - epochs monotone; when recovery happened (every point except the
///   frozen after-end-write stream) a takeover claim with a strictly
///   higher epoch exists;
/// - seqs contiguous; every prefix projects legally and no observable
///   half-terminated state exists (terminal ⇔ terminal end is last);
/// - takeover within the documented lease TTL + heartbeat bound.
fn assert_recovered(
    point: KillPoint,
    events: &[Event],
    input_contents: &[String],
    pre_kill_epoch_max: u64,
    takeover_epoch_ts: Option<u64>,
    kill_wall_ms: u128,
) {
    let kinds = kinds_of(events);
    let ctx = |what: &str| format!("{}: {what}: {kinds:?}", point.name());

    // Exactly one end write, honest reason, last event.
    assert_eq!(
        count_ended(events),
        1,
        "{}",
        ctx("exactly one session/ended")
    );
    let last = events.last().expect("non-empty");
    assert!(
        matches!(
            last.kind,
            EventKind::SessionEnded {
                reason: EndReason::Completed
            }
        ),
        "{}",
        ctx("end write is last, reason completed")
    );

    // NoLostWork: every submitted input consumed at least once (a
    // duplicate consumption event is legal — see the doc comment).
    let mut consumed: std::collections::BTreeMap<String, usize> = Default::default();
    for ev in events.iter().filter(|e| e.kind == EventKind::InputAdmitted) {
        let content = ev.bytes["content"].as_str().expect("content").to_owned();
        *consumed.entry(content).or_default() += 1;
    }
    for expected in input_contents {
        let seen = consumed.get(expected).copied().unwrap_or(0);
        assert!(
            seen == 1 || seen == 2,
            "{}",
            ctx(&format!(
                "input `{expected}` consumed {seen} times (at-least-once, bounded duplicates)"
            ))
        );
    }
    assert!(
        consumed.keys().all(|c| input_contents.contains(c)),
        "{}",
        ctx("no unexpected input contents consumed")
    );

    // Epochs monotone; the takeover claim when recovery happened.
    let epochs: Vec<u64> = events.iter().map(|e| e.epoch.0).collect();
    assert!(
        epochs.windows(2).all(|w| w[0] <= w[1]),
        "{}",
        ctx("epochs monotone across the kill window")
    );
    if takeover_epoch_ts.is_some() {
        assert!(
            events
                .iter()
                .any(|e| e.kind == EventKind::DrainClaimed && e.epoch.0 > pre_kill_epoch_max),
            "{}",
            ctx("takeover claim with a higher epoch")
        );
    }

    // Contiguous seqs and prefix legality (NoHalfTerminated by
    // construction, verified on every prefix of the recovered stream).
    for w in events.windows(2) {
        assert_eq!(w[0].seq + 1, w[1].seq, "{}", ctx("contiguous seqs"));
    }
    for n in 1..=events.len() {
        let state =
            project(&events[..n]).unwrap_or_else(|e| panic!("{}: prefix {n}: {e}", point.name()));
        let last = &events[n - 1];
        let terminal_end = matches!(
            last.kind,
            EventKind::SessionEnded {
                reason: EndReason::Completed | EndReason::Cancelled
            }
        );
        assert_eq!(
            state.is_terminal(),
            terminal_end,
            "{}",
            ctx(&format!("prefix {n}: terminal iff terminal end is last"))
        );
    }

    // Recovery detection within the documented bound.
    if let Some(ts) = takeover_epoch_ts {
        let detection = u128::from(ts).saturating_sub(kill_wall_ms);
        assert!(
            detection <= DETECTION_BOUND_MS,
            "{}: takeover took {detection} ms (bound {DETECTION_BOUND_MS})",
            point.name()
        );
    }
}

// ---------------------------------------------------------------------------
// One chaos run
// ---------------------------------------------------------------------------

async fn run_kill_point(point: KillPoint, seed: u64) {
    let h = Harness::new(point, seed).await;
    let mut server = h.spawn_serve(&h.config1, "boot1");
    assert!(
        h.wait_ready(&h.base1).await,
        "boot1 never became ready (seed {seed})"
    );

    // Drive to the kill point and kill -9 there.
    let (sid, inputs, kill_wall) = kill_at_stage(&h, &mut server, point, seed).await;

    // The killed process is dead: verify the durable stage shape.
    let killed_events = h.events(sid).await;
    verify_killed_stage(&h, point, sid, &killed_events).await;
    let pre_kill_epoch_max = killed_events.iter().map(|e| e.epoch.0).max().unwrap_or(0);
    let pre_restart_len = killed_events.len();

    // A killed-mid-drain session is never mislabeled: before recovery
    // it projects running (recoverable via the lease overlay), never
    // completed/failed.
    if point != KillPoint::AfterEndWrite {
        let state = project(&killed_events).expect("killed stream projects");
        assert_eq!(
            state,
            SessionState::Running,
            "{}: killed-mid-drain projects running (recoverable), got {state:?}",
            point.name()
        );
    }

    // Release the mock gate so post-restart traffic flows.
    h.mock.released.store(true, Ordering::SeqCst);

    // -- restart on the same store -----------------------------------
    let mut server2 = h.spawn_serve(&h.config2, "boot2");
    assert!(
        h.wait_ready(&h.base2).await,
        "boot2 never became ready (seed {seed})"
    );

    // Poll recovery, sampling the stream: every sample must project
    // legally and the projection must never mislabel (no failed/
    // cancelled during recovery). NoLostWork checked at the end.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut samples = 0u32;
    let final_events = loop {
        let events = h.events(sid).await;
        samples += 1;
        for n in 1..=events.len() {
            project(&events[..n]).unwrap_or_else(|e| {
                panic!(
                    "{}: recovery sample corrupt at prefix {n}: {e}",
                    point.name()
                )
            });
        }
        if let Some(last) = events.last()
            && matches!(
                last.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Completed
                }
            )
        {
            break events;
        }
        let out = h
            .rpc(
                &h.base2,
                "projection",
                serde_json::json!({"session": sid.0.to_string()}),
            )
            .await;
        let state = out["result"]["state"].as_str().unwrap_or_default();
        assert!(
            state == "running" || state == "completed",
            "{}: honest labeling during recovery, saw {state:?}",
            point.name()
        );
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "{}: never recovered to completed; last kinds {:?}",
                point.name(),
                kinds_of(&events)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(samples >= 1);

    // The takeover claim: first drain/claimed with an epoch above the
    // killed owner's. (AfterEndWrite has none — the stream is frozen.)
    let takeover = final_events
        .iter()
        .find(|e| e.kind == EventKind::DrainClaimed && e.epoch.0 > pre_kill_epoch_max);

    match point {
        KillPoint::AfterEndWrite => {
            // Terminal streams freeze: the restart appended NOTHING.
            assert!(takeover.is_none(), "terminal stream must not re-claim");
            assert_eq!(
                final_events.len(),
                pre_restart_len,
                "{}: terminal stream frozen across restart",
                point.name()
            );
            assert_recovered(
                point,
                &final_events,
                &inputs,
                pre_kill_epoch_max,
                None,
                kill_wall,
            );
        }
        _ => {
            let takeover = takeover.expect("takeover claim exists");
            // NoLostWork: nothing eligible remains anywhere.
            let now = now_ms() as u64;
            let eligible = h.store.eligible_inputs(sid, now).await.expect("eligible");
            assert!(
                eligible.is_empty(),
                "{}: inputs left unconsumed: {eligible:?}",
                point.name()
            );
            let scan = h.store.startup_scan(now).await.expect("scan");
            assert!(
                scan.iter().all(|i| i.session != sid),
                "{}: session still in the startup inbox scan",
                point.name()
            );
            assert_recovered(
                point,
                &final_events,
                &inputs,
                pre_kill_epoch_max,
                Some(takeover.ts),
                kill_wall,
            );
        }
    }

    eprintln!(
        "c15: {} recovered (seed {seed}): {} events, {} samples",
        point.name(),
        final_events.len(),
        samples
    );
    kill9(&mut server2);
    h.cleanup();
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// S1 acceptance: all six kill points × repeated runs, all green.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_s1_six_kill_points_repeated_runs_all_green() {
    force_no_proxy();
    const REPEATS: u64 = 2;
    for point in POINTS {
        for repeat in 0..REPEATS {
            let seed = SEED_BASE + point.index() * 100 + repeat;
            run_kill_point(point, seed).await;
        }
    }
}

/// NoSplitBrainWrite across the kill window: a lingering zombie
/// process holding the old epoch (SIGSTOPped mid-provider-turn, then
/// resumed AFTER the restart completed the session under a new epoch)
/// has every write rejected by the store fence. Its attempted
/// `assistant/message` / `turn/end` / end write all bounce: the stream
/// is byte-identical to the takeover's completion, exactly one end
/// write exists, and no event below the takeover epoch lands after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_s1_zombie_old_epoch_writes_all_rejected_across_kill_window() {
    force_no_proxy();
    let point = KillPoint::MidProviderTurn;
    let seed = SEED_BASE + 0xF00;
    let h = Harness::new(point, seed).await;
    let mut zombie = h.spawn_serve(&h.config1, "zombie");
    assert!(h.wait_ready(&h.base1).await, "zombie never became ready");

    let sid = h.create_session(&h.base1).await;
    assert!(!h.submit(&h.base1, sid, "zombie task", None).await);

    // Mid-provider-turn (gated request #1): the zombie holds epoch e1.
    h.wait_mock_requests(1, "zombie mid-turn").await;
    let events = h.events(sid).await;
    let zombie_epoch = events.iter().map(|e| e.epoch.0).max().unwrap_or(0);
    assert!(has(&events, EventKind::StepRequest), "zombie is mid-turn");

    // The "kill": the process lingers (STOP), heartbeats stop.
    sigstop(&zombie);
    let kill_wall = now_ms();

    // The successor boots on the same store (its own port).
    let mut successor = h.spawn_serve(&h.config2, "successor");
    assert!(h.wait_ready(&h.base2).await, "successor never became ready");
    h.mock.released.store(true, Ordering::SeqCst);

    // Recovery within the documented bound; the successor takes over
    // with a strictly higher epoch and completes the session.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let (takeover_ts, takeover_epoch, completed) = loop {
        let events = h.events(sid).await;
        let takeover = events
            .iter()
            .find(|e| e.kind == EventKind::DrainClaimed && e.epoch.0 > zombie_epoch);
        let completed = events.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Completed
                }
            )
        });
        if let (Some(t), true) = (takeover, completed) {
            break (t.ts, t.epoch.0, events);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "successor never completed the takeover: {:?}",
            kinds_of(&events)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let detection = u128::from(takeover_ts).saturating_sub(kill_wall);
    assert!(
        detection <= DETECTION_BOUND_MS,
        "takeover took {detection} ms (bound {DETECTION_BOUND_MS})"
    );
    assert!(takeover_epoch > zombie_epoch);

    // Snapshot the completed stream; resume the zombie INSIDE the kill
    // window's aftermath. Every write it attempts must be rejected.
    let snapshot_len = completed.len();
    let snapshot_last_seq = completed.last().expect("ended").seq;
    sigcont(&zombie);
    // Give the zombie ample time to finish its held request and have
    // its fenced writes bounce (its drain abandons on stale epoch).
    tokio::time::sleep(Duration::from_secs(4)).await;

    let after = h.events(sid).await;
    let kinds = kinds_of(&after);
    assert_eq!(
        after.len(),
        snapshot_len,
        "zombie appended nothing: {kinds:?}"
    );
    assert_eq!(
        after.last().expect("ended").seq,
        snapshot_last_seq,
        "stream head unchanged by the zombie"
    );
    // Exactly one end write; nothing below the takeover epoch after the
    // takeover claim (NoSplitBrainWrite across the kill window).
    assert_eq!(count_ended(&after), 1, "{kinds:?}");
    let takeover_seq = after
        .iter()
        .find(|e| e.kind == EventKind::DrainClaimed && e.epoch.0 == takeover_epoch)
        .expect("takeover claim")
        .seq;
    for ev in after.iter().filter(|e| e.seq > takeover_seq) {
        assert_eq!(
            ev.epoch.0, takeover_epoch,
            "event below the takeover epoch landed after it: {kinds:?}"
        );
    }

    kill9(&mut zombie);
    kill9(&mut successor);
    h.cleanup();
}
