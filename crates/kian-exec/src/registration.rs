//! The registration listener (KiandProto v1 worker-dial accept path,
//! R12-03): binds a **private** address, accepts worker connections,
//! validates the `register` handshake (version, token constant-time,
//! duplicate/`duplicate_static` rules), emits `welcome` or the typed
//! refusal, records the member online, and hands the live connection to
//! the exec layer as a route for that worker id (the sink seam; R12-04
//! wires it to `attach_route`).
//!
//! Security boundary (I9): the token is compared constant-time, never
//! logged; failed attempts are bounded per connection (one) and per peer
//! (5 refusals / 60 s window → close without reading a frame); every
//! audit line carries only the endpoint id and the peer address.
//!
//! Registration commits in the spec §9.1 order: (1) per-id slot,
//! (2) directory entry (no live connection), (3) durable projection
//! hook, (4) route attach hook, (5) mark live + flush welcome,
//! (6) audit line.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::membership::{MemberSnapshot, WorkerDirectory, valid_description, valid_worker_id};
use crate::proto::{
    EndpointFrame, PROTOCOL_VERSION, RefusalReason, ServerFrame, decode_endpoint_frame,
    encode_server_frame,
};
use crate::watchdog::Clock;

/// A worker must send its `register` within this deadline after connect.
pub const REGISTER_DEADLINE: Duration = Duration::from_secs(10);
/// How long a registration waits on a busy per-id slot before answering
/// `id_conflict` (so the worker retries with backoff instead of hanging).
pub const REGISTER_SLOT_WAIT: Duration = Duration::from_secs(5);
/// Failed-auth window bound per peer address (P9).
pub const REFUSAL_WINDOW: Duration = Duration::from_secs(60);
/// Refusals allowed per peer inside the window before rate-limiting.
pub const REFUSAL_WINDOW_MAX: u32 = 5;

/// Why a member went offline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveReason {
    Bye,
    TransportLoss,
    Replaced,
    Shutdown,
}

/// The live connection handed to the exec layer (R12-04's route input):
/// the writer sends frames to the worker; `inbound` yields every frame
/// the worker sends (heartbeats included — the route's ping machinery
/// needs them; liveness bookkeeping is already peeled off by the
/// listener).
pub struct RegisteredWorker {
    pub id: String,
    pub generation: u64,
    pub peer_addr: SocketAddr,
    pub version: u32,
    /// The validated description from this registration (ADR-025). The
    /// sink caches it on the registry; `None` means the declaration
    /// omitted it (or it was dropped as invalid).
    pub description: Option<String>,
    pub max_concurrent: u32,
    pub writer: mpsc::Sender<ServerFrame>,
    pub inbound: mpsc::Receiver<EndpointFrame>,
}

/// The handoff seam: called by the listener on membership edges. The
/// default impl is a no-op (R12-03 tests record events through a
/// capturing sink; the assembly routes them — R12-04's
/// `attach_route`/`detach_route` wiring).
///
/// Both methods return boxed futures the listener **awaits**, so the
/// route attach (and its durable registry projection) commits before
/// the handshake's `welcome` flushes — the spec §9.1 ordering.
pub trait RegistrationSink: Send + Sync {
    /// A worker completed registration (route-attach point).
    fn member_online(&self, _worker: RegisteredWorker) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
    /// A worker left (route-detach point). `generation` scopes the
    /// detach (A15): a teardown for generation *N* must never touch the
    /// route attached by generation *N+1*.
    fn member_offline(
        &self,
        _id: &str,
        _generation: u64,
        _reason: LeaveReason,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// The no-op sink (default).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl RegistrationSink for NoopSink {}

/// Membership-edge counters (the audit surface's numeric twin): one
/// per audited transition, deduped per generation by construction (a
/// generation-guarded teardown fires at most once; a replaced
/// incumbent's late EOF is not an owner and counts nothing).
#[derive(Debug, Default)]
pub struct RegistrationMetrics {
    /// Accepted registrations (join + replace).
    pub joined: AtomicU64,
    /// Replacements of a live duplicate id.
    pub replaced: AtomicU64,
    /// Graceful (`bye`) leaves.
    pub left_bye: AtomicU64,
    /// Transport-loss leaves.
    pub left_loss: AtomicU64,
    /// Refused registration attempts (every closed reason).
    pub refused: AtomicU64,
    /// Peers closed by the per-window rate bound.
    pub rate_limited: AtomicU64,
}

/// The server-side shared worker token (C4): read once at startup from
/// the named env var; redacting like the wire type; never logged.
#[derive(Clone, PartialEq, Eq)]
pub struct WorkerToken(String);

impl WorkerToken {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for WorkerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl std::fmt::Display for WorkerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Constant-time equality: the loop always scans `max(a.len(), b.len())`
/// bytes (zero-padded) and folds the length mismatch into the accumulator
/// as a boolean, so neither content nor length is an early-exit oracle.
/// The mismatch must fold as `u8::from(a.len() != b.len())` — a byte-wise
/// fold like `(a.len() ^ b.len()) as u8` truncates and vanishes whenever
/// the lengths differ by a multiple of 256 (a JSON `\u0000`-padded token
/// would then compare equal to the real one).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len = a.len().max(b.len());
    let mut diff: u8 = u8::from(a.len() != b.len());
    for i in 0..len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

/// The private bind set (C3): loopback, RFC1918, CGNAT, IPv6 loopback,
/// IPv6 ULA. Wildcards and link-local count as **public** (fail-closed).
pub fn is_private_bind_addr(addr: SocketAddr) -> bool {
    let ip = addr.ip();
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            // Wildcard is public: binding 0.0.0.0 exposes every iface.
            v4.is_loopback()
                || o[0] == 10
                || (o[0] == 172 && (16..=31).contains(&o[1]))
                || (o[0] == 192 && o[1] == 168)
                || (o[0] == 100 && (64..=127).contains(&o[1]))
        }
        IpAddr::V6(v6) => {
            // `::` and `::1` share is_unspecified/is_loopback cleanly;
            // link-local (fe80::/10) is deliberately NOT private here.
            v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// Fail-closed bind validation (C2): a non-private `listen` without an
/// explicit `allow_public` refuses startup.
pub fn validate_bind_addr(addr: SocketAddr, allow_public: bool) -> Result<(), String> {
    if allow_public || is_private_bind_addr(addr) {
        return Ok(());
    }
    Err(format!(
        "workers.listen {addr} is a public address; the registration listener accepts commands \
         and must bind a private range (127.0.0.0/8, 10/8, 172.16/12, 192.168/16, \
         100.64.0.0/10, ::1, fc00::/7) reached over an SSH tunnel or Tailscale — or set \
         allow_public: true explicitly (this exposes a command-execution surface)"
    ))
}

/// Per-peer failed-auth bookkeeping (P9), clock-injected for tests.
#[derive(Default)]
struct PeerRefusals {
    /// peer ip → refusal timestamps inside the window (ms).
    window: HashMap<IpAddr, Vec<u64>>,
    /// peers currently rate-limited + when the window resets.
    limited: HashMap<IpAddr, u64>,
}

struct ListenerState {
    /// Per-id registration slots: held across a registration commit so
    /// concurrent registrations for one id serialize (M6).
    slots: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    refusals: Mutex<PeerRefusals>,
    /// Set while the listener is shutting down: new registrations are
    /// refused `disabled`.
    disabled: std::sync::atomic::AtomicBool,
}

/// Configuration of the registration listener.
pub struct RegistrationConfig {
    pub bind: SocketAddr,
    pub token: WorkerToken,
    /// Liveness bound; the welcome cadence is `max(1, offline_after_ms/3)`
    /// (P6).
    pub offline_after_ms: u64,
    /// Ids owned by static `endpoints:` pins — a dynamic registration of
    /// one of these is refused `duplicate_static` (C5).
    pub static_ids: Vec<String>,
    /// Explicit public-bind override (never set in docs examples).
    pub allow_public: bool,
    /// Test knobs: the spec constants by default.
    pub register_deadline: Duration,
    pub slot_wait: Duration,
}

impl RegistrationConfig {
    pub fn new(bind: SocketAddr, token: WorkerToken, offline_after_ms: u64) -> Self {
        Self {
            bind,
            token,
            offline_after_ms,
            static_ids: Vec::new(),
            allow_public: false,
            register_deadline: REGISTER_DEADLINE,
            slot_wait: REGISTER_SLOT_WAIT,
        }
    }

    pub fn with_static_ids(mut self, ids: Vec<String>) -> Self {
        self.static_ids = ids;
        self
    }

    /// Fail-closed validation before any socket opens.
    pub fn validate(&self) -> Result<(), String> {
        validate_bind_addr(self.bind, self.allow_public)?;
        if self.offline_after_ms == 0 {
            return Err("workers.offline_after_ms must be > 0".into());
        }
        if self.token.as_str().trim().is_empty() {
            return Err("the worker token is absent or blank".into());
        }
        Ok(())
    }
}

/// A running registration listener. Dropping it stops the accept loop;
/// [`RegistrationListener::shutdown`] is the graceful path (live members
/// get a server `bye`).
pub struct RegistrationListener {
    state: Arc<ListenerState>,
    /// The runtime membership directory (M1) — shared with the exec
    /// assembly (the backend's dynamic default resolution and the
    /// `WorkerDirectoryPort` read the same handle).
    directory: Arc<WorkerDirectory>,
    config: RegistrationConfig,
    clock: Arc<dyn Clock>,
    sink: Arc<dyn RegistrationSink>,
    metrics: Arc<RegistrationMetrics>,
    local_addr: Mutex<Option<SocketAddr>>,
    conns: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl RegistrationListener {
    /// Build (not yet started). `validate` is enforced by [`Self::run`].
    /// The directory is caller-created so the exec backend can share it
    /// *before* the listener exists (the backend is the sink's target).
    pub fn new(
        config: RegistrationConfig,
        clock: Arc<dyn Clock>,
        sink: Arc<dyn RegistrationSink>,
        directory: Arc<WorkerDirectory>,
    ) -> Self {
        Self {
            state: Arc::new(ListenerState {
                slots: Mutex::new(HashMap::new()),
                refusals: Mutex::new(PeerRefusals::default()),
                disabled: std::sync::atomic::AtomicBool::new(false),
            }),
            directory,
            config,
            clock,
            sink,
            metrics: Arc::new(RegistrationMetrics::default()),
            local_addr: Mutex::new(None),
            conns: Mutex::new(HashMap::new()),
        }
    }

    /// The shared membership directory (snapshots + `member_state`
    /// computation live there — M3).
    pub fn directory(&self) -> Arc<WorkerDirectory> {
        Arc::clone(&self.directory)
    }

    /// The membership-edge counters (the `/metrics` surface reads this).
    pub fn metrics(&self) -> Arc<RegistrationMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The bound address (after [`Self::run`] started).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.local_addr.lock().unwrap()
    }

    /// Bind and serve until dropped or shut down. One tokio task per
    /// connection; the accept loop itself never panics on a peer error.
    pub async fn run(self: &Arc<Self>) -> std::io::Result<()> {
        self.config.validate().map_err(std::io::Error::other)?;
        let listener = TcpListener::bind(self.config.bind).await?;
        *self.local_addr.lock().unwrap() = listener.local_addr().ok();
        tracing::info!(
            bind = ?self.config.bind,
            "registration listener serving (KiandProto v{PROTOCOL_VERSION})"
        );
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.handle_connection(stream, peer).await;
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "registration accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Snapshot of every member (states are computed by callers through
    /// [`crate::membership::member_state`] — M3).
    pub fn snapshot(&self) -> BTreeMap<String, MemberSnapshot> {
        self.directory.snapshot()
    }

    /// State of one member, computed.
    pub fn member_state(&self, id: &str, now_ms: u64) -> Option<crate::membership::MemberState> {
        self.directory.member(id).map(|snapshot| {
            crate::membership::member_state(&snapshot, now_ms, self.config.offline_after_ms)
        })
    }

    /// Stop admitting new registrations: further peers are refused
    /// `disabled` (retryable). The graceful bye path is
    /// [`Self::shutdown`].
    pub fn disable(&self) {
        self.state
            .disabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Graceful shutdown: refuse new registrations (`disabled`), say bye
    /// to every live member, record the shutdown leave, join the
    /// connection tasks.
    pub async fn shutdown(&self) {
        self.disable();
        let writers: Vec<(String, mpsc::Sender<ServerFrame>)> = self
            .directory
            .snapshot()
            .into_iter()
            .filter_map(|(id, member)| {
                self.directory
                    .writer_of(&id)
                    .filter(|_| member.connected)
                    .map(|writer| (id, writer))
            })
            .collect();
        for (id, writer) in writers {
            tracing::info!(endpoint_id = %id, "shutdown: sending bye");
            let _ = writer.send(ServerFrame::Bye).await;
            self.mark_left(&id, LeaveReason::Shutdown).await;
        }
        let tasks: Vec<JoinHandle<()>> =
            self.conns.lock().unwrap().drain().map(|(_, t)| t).collect();
        for task in tasks {
            let _ = task.await;
        }
    }

    /// Per-connection pipeline: rate bound → disabled check → handshake →
    /// steady state.
    async fn handle_connection(self: Arc<Self>, stream: TcpStream, peer: SocketAddr) {
        if self.refuse_if_rate_limited(peer) {
            drop(stream);
            return;
        }
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader);

        // Listener shutdown: `disabled`, retryable on the worker side.
        if self
            .state
            .disabled
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.refuse(
                &mut writer,
                peer,
                RefusalReason::Disabled,
                Some("the registration listener is shutting down; retry shortly".into()),
            )
            .await;
            return;
        }

        // P7: exactly one register, first decodable frame, within the
        // deadline.
        let first =
            tokio::time::timeout(self.config.register_deadline, next_frame(&mut lines)).await;
        let (endpoint_id, description, token, version, max_concurrent) = match first {
            Ok(Some(EndpointFrame::Register {
                endpoint_id,
                description,
                token,
                version,
                max_concurrent,
            })) => (endpoint_id, description, token, version, max_concurrent),
            _ => {
                tracing::warn!(peer = %peer, "protocol: first frame must be register; closing");
                return; // no reply frame, no half-registered state
            }
        };

        // Validation order: version → token → identity → static pin.
        if version != PROTOCOL_VERSION {
            self.refuse(
                &mut writer,
                peer,
                RefusalReason::VersionMismatch,
                Some(format!(
                    "server speaks KiandProto {PROTOCOL_VERSION}; worker announced {version}"
                )),
            )
            .await;
            return;
        }
        if !constant_time_eq(
            token.as_str().as_bytes(),
            self.config.token.as_str().as_bytes(),
        ) {
            self.count_refusal(peer);
            self.refuse(&mut writer, peer, RefusalReason::BadToken, None)
                .await;
            return;
        }
        if !valid_worker_id(&endpoint_id) {
            self.refuse(
                &mut writer,
                peer,
                RefusalReason::IdConflict,
                Some("endpoint_id must be 1..=200 bytes after trim, no control characters".into()),
            )
            .await;
            return;
        }
        if self.config.static_ids.iter().any(|id| id == &endpoint_id) {
            self.refuse(
                &mut writer,
                peer,
                RefusalReason::DuplicateStatic,
                Some(format!(
                    "endpoint_id `{endpoint_id}` is pinned by a static endpoints entry; \
                     remove the pin or register another id"
                )),
            )
            .await;
            return;
        }

        // P11: an invalid description is dropped (audited), never fatal.
        let description = match description {
            Some(description) if valid_description(&description) => Some(description),
            Some(_) => {
                tracing::warn!(
                    endpoint_id = %endpoint_id,
                    peer = %peer,
                    "dropped invalid registration description"
                );
                None
            }
            None => None,
        };

        // P8: clamp the announced cap.
        let max_concurrent = if max_concurrent == 0 {
            tracing::warn!(endpoint_id = %endpoint_id, "announced max_concurrent=0; clamping to 1");
            1
        } else {
            max_concurrent
        };

        // (1) per-id slot, with the honest wait bound.
        let slot = {
            let mut slots = self.state.slots.lock().unwrap();
            slots.entry(endpoint_id.clone()).or_default().clone()
        };
        let _guard = match tokio::time::timeout(self.config.slot_wait, slot.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                self.refuse(
                    &mut writer,
                    peer,
                    RefusalReason::IdConflict,
                    Some("a registration for this id is still settling; retry with backoff".into()),
                )
                .await;
                return;
            }
        };

        // Replace: close the incumbent with a server bye first.
        let incumbent_writer = self.directory.writer_of(&endpoint_id);
        let replaced = incumbent_writer.is_some();
        if let Some(writer_tx) = incumbent_writer {
            tracing::info!(endpoint_id = %endpoint_id, peer = %peer, "worker replaced: closing incumbent");
            let _ = writer_tx.send(ServerFrame::Bye).await;
            let incumbent_generation = self
                .directory
                .begin_teardown(&endpoint_id)
                .unwrap_or_default();
            self.sink
                .member_offline(&endpoint_id, incumbent_generation, LeaveReason::Replaced)
                .await;
            self.directory.end_teardown(&endpoint_id);
            self.metrics.replaced.fetch_add(1, Ordering::Relaxed);
        }

        let now = self.clock.now_ms();
        // (2) directory entry, no live connection yet (§9.1 ordering).
        let generation = self.directory.register(
            &endpoint_id,
            description.clone(),
            version,
            max_concurrent,
            peer,
            now,
        );

        // Frame channels: the route gets the writer + inbound frames.
        let (writer_tx, mut writer_rx) = mpsc::channel::<ServerFrame>(64);
        let (inbound_tx, inbound_rx) = mpsc::channel::<EndpointFrame>(64);
        // (3)+(4) durable projection + route attach seam (the backend's
        // `attach_route` upserts the registry row before the route
        // becomes visible — the §9.1 step-3-before-step-4 ordering).
        self.sink
            .member_online(RegisteredWorker {
                id: endpoint_id.clone(),
                generation,
                peer_addr: peer,
                version,
                description: description.clone(),
                max_concurrent,
                writer: writer_tx.clone(),
                inbound: inbound_rx,
            })
            .await;

        // (5) mark live + flush the welcome (server-directed cadence,
        // P6). The write pump serializes frames ahead of the socket.
        self.directory.set_live(&endpoint_id, writer_tx.clone());
        let heartbeat_ms = (self.config.offline_after_ms / 3).max(1);
        let mut line = encode_server_frame(&ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(heartbeat_ms),
        });
        line.push('\n');
        if writer.write_all(line.as_bytes()).await.is_err() {
            // The worker died mid-handshake: fall through to teardown.
            self.mark_left(&endpoint_id, LeaveReason::TransportLoss)
                .await;
            return;
        }
        // The write pump owns the socket from here.
        tokio::spawn(async move {
            while let Some(frame) = writer_rx.recv().await {
                let mut out = encode_server_frame(&frame);
                out.push('\n');
                if writer.write_all(out.as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        // (6) audit line — after the state is durable-consistent.
        if replaced {
            self.metrics.joined.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                endpoint_id = %endpoint_id,
                peer = %peer,
                generation,
                "worker replaced"
            );
        } else {
            self.metrics.joined.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                endpoint_id = %endpoint_id,
                peer = %peer,
                generation,
                "worker joined"
            );
        }

        // Steady state: heartbeats refresh liveness; every frame is
        // forwarded to the route; bye/loss tears down (generation-guarded
        // so a replaced incumbent's EOF cannot touch its successor).
        let this = self.clone();
        let id_for_task = endpoint_id.clone();
        let task = tokio::spawn(async move {
            let endpoint_id = id_for_task;
            while let Some(frame) = next_frame(&mut lines).await {
                if let EndpointFrame::Heartbeat { .. } = frame {
                    let now = this.clock.now_ms();
                    this.directory.note_seen(&endpoint_id, now);
                }
                if matches!(
                    frame,
                    EndpointFrame::Register { .. } | EndpointFrame::Hello { .. }
                ) {
                    tracing::warn!(
                        endpoint_id = %endpoint_id,
                        peer = %peer,
                        "protocol: handshake frame during steady state; closing"
                    );
                    this.metrics.left_loss.fetch_add(1, Ordering::Relaxed);
                    this.mark_left_if_owner(&endpoint_id, generation, LeaveReason::TransportLoss)
                        .await;
                    return;
                }
                if matches!(frame, EndpointFrame::Bye) {
                    tracing::info!(endpoint_id = %endpoint_id, peer = %peer, "worker left (bye)");
                    this.metrics.left_bye.fetch_add(1, Ordering::Relaxed);
                    this.mark_left_if_owner(&endpoint_id, generation, LeaveReason::Bye)
                        .await;
                    return;
                }
                if inbound_tx.send(frame).await.is_err() {
                    return; // route went away
                }
            }
            tracing::info!(
                endpoint_id = %endpoint_id,
                peer = %peer,
                "worker left (transport loss)"
            );
            this.metrics.left_loss.fetch_add(1, Ordering::Relaxed);
            this.mark_left_if_owner(&endpoint_id, generation, LeaveReason::TransportLoss)
                .await;
        });
        self.conns.lock().unwrap().insert(endpoint_id.clone(), task);
    }

    /// Teardown bookkeeping shared by bye / loss / replace / shutdown.
    /// The `teardown` flag spans the drain window only: it is raised, the
    /// detach seam fires (awaited, generation-scoped), then it clears
    /// (draining → offline).
    async fn mark_left(&self, id: &str, reason: LeaveReason) {
        if let Some(generation) = self.directory.begin_teardown(id) {
            self.sink.member_offline(id, generation, reason).await;
        }
        self.directory.end_teardown(id);
    }

    /// [`Self::mark_left`] guarded by ownership: no-op unless the
    /// directory still holds this generation with a live connection.
    /// A replaced (or shutdown-drained) incumbent's EOF therefore cannot
    /// tear down its successor.
    async fn mark_left_if_owner(&self, id: &str, generation: u64, reason: LeaveReason) {
        if self.directory.is_owner(id, generation) {
            self.mark_left(id, reason).await;
        }
    }

    /// Emit one typed refusal + the audit line, then close. Never logs
    /// the token; the line carries the id (when known) and the peer.
    async fn refuse(
        &self,
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        peer: SocketAddr,
        reason: RefusalReason,
        detail: Option<String>,
    ) {
        self.metrics.refused.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            reason = ?reason,
            peer = %peer,
            "register refused"
        );
        let mut line = encode_server_frame(&ServerFrame::Rejected { reason, detail });
        line.push('\n');
        let _ = writer.write_all(line.as_bytes()).await;
        let _ = writer.shutdown().await;
    }

    /// P9: count one failed auth; true ⇒ close without reading a frame.
    fn refuse_if_rate_limited(&self, peer: SocketAddr) -> bool {
        let now = self.clock.now_ms();
        let window = REFUSAL_WINDOW.as_millis() as u64;
        let mut refusals = self.state.refusals.lock().unwrap();
        if let Some(&reset_at) = refusals.limited.get(&peer.ip()) {
            if now < reset_at {
                return true;
            }
            refusals.limited.remove(&peer.ip());
            refusals.window.remove(&peer.ip());
        }
        let entries = refusals.window.entry(peer.ip()).or_default();
        entries.retain(|&ts| now.saturating_sub(ts) <= window);
        // Limited once the window is full: close without reading a frame.
        entries.len() >= REFUSAL_WINDOW_MAX as usize
    }

    fn count_refusal(&self, peer: SocketAddr) {
        let now = self.clock.now_ms();
        let window = REFUSAL_WINDOW.as_millis() as u64;
        let mut refusals = self.state.refusals.lock().unwrap();
        let entries = refusals.window.entry(peer.ip()).or_default();
        entries.retain(|&ts| now.saturating_sub(ts) <= window);
        entries.push(now);
        if entries.len() as u32 >= REFUSAL_WINDOW_MAX {
            refusals.limited.insert(peer.ip(), now + window);
            self.metrics.rate_limited.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(peer = %peer, "rate_limited: too many refused registrations");
        }
    }
}

/// Read one decoded endpoint frame line (None on EOF).
async fn next_frame(
    lines: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Option<EndpointFrame> {
    loop {
        let mut line = String::new();
        match lines.read_line(&mut line).await {
            Ok(0) => return None,
            Ok(_) => match decode_endpoint_frame(line.trim_end()) {
                Ok(frame) => return Some(frame),
                Err(error) => {
                    tracing::warn!(%error, "skipping malformed KiandProto line");
                }
            },
            Err(_) => return None,
        }
    }
}
