//! The bridge's HTTP ABI (CHANNEL-IMESSAGE §3.2, `protocol: 1`).
//!
//! Three routes on a loopback listener, all bearer-authenticated:
//! `GET /v1/health`, `GET /v1/updates` (bounded long poll over an
//! exclusive `rowid` cursor), `POST /v1/send` (with the caller's
//! `client_id` as the delivery idempotency key). No route reads
//! arbitrary chats, lists chats, or sends to a target the caller did not
//! name: the surface is exactly the contract.
//!
//! The bridge applies the **privacy filter** (`watch.participants`) to
//! every page before it leaves the Mac, and nothing else: the sender
//! allowlist, the drop rules and the output policy stay in the adapter
//! (CHANNEL-IMESSAGE §4), so the contract has one home.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::imsg::{Backend, RpcError};
use crate::state::CursorStore;

/// The ABI version this bridge serves.
pub const PROTOCOL: u32 = 1;

/// The `/v1/updates` wait cap (the adapter's contract: ≤ 25 s).
pub const MAX_WAIT_MS: u64 = 25_000;

/// Body cap for every route.
pub const BODY_LIMIT_BYTES: usize = 2 * 1024 * 1024;

/// How long a `client_id`'s observed result is served instead of a
/// resend (the delivery idempotency window, §3.2).
pub const CLIENT_ID_TTL: Duration = Duration::from_secs(600);

/// How many `client_id` results are remembered.
pub const CLIENT_ID_CACHE: usize = 128;

/// The result of one observed send (the delivery-idempotency cache).
#[derive(Debug, Clone)]
enum Cached {
    Ok(Value),
    Uncertain { kind: String, detail: String },
}

struct SendCache {
    order: VecDeque<String>,
    map: HashMap<String, (Cached, Instant)>,
}

impl SendCache {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(key) = self.order.front() {
            let expired = self
                .map
                .get(key)
                .is_none_or(|(_, at)| now.duration_since(*at) > CLIENT_ID_TTL);
            if !expired {
                break;
            }
            let key = self.order.pop_front();
            if let Some(key) = key {
                self.map.remove(&key);
            }
        }
    }

    fn get(&mut self, key: &str, now: Instant) -> Option<Cached> {
        self.prune(now);
        self.map.get(key).map(|(cached, _)| cached.clone())
    }

    fn insert(&mut self, key: String, cached: Cached, now: Instant) {
        self.prune(now);
        if self.map.insert(key.clone(), (cached, now)).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > CLIENT_ID_CACHE {
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
            }
        }
    }
}

/// Shared counters (the health snapshot's `counters` block).
#[derive(Debug, Default)]
pub struct Counters {
    pub updates_served: AtomicU64,
    pub sends: AtomicU64,
    pub uncertain_sends: AtomicU64,
}

/// The bridge application state.
pub struct App<B: Backend> {
    backend: Arc<B>,
    token: String,
    instance_id: String,
    cursor: CursorStore,
    participants: Vec<String>,
    poll_interval: Duration,
    catchup_rows: u32,
    page_limit: u32,
    /// `watch.attachments`: ask `imsg` for attachment metadata.
    attachments: bool,
    cache: Mutex<SendCache>,
    /// Sends are serialized (one at a time, §3.2): order is preserved and
    /// a duplicate `client_id` arriving mid-flight joins the first.
    send_lock: tokio::sync::Mutex<()>,
    /// The last observed Automation state: `unknown` | `granted` |
    /// `denied` (never probed proactively — a probe would raise a TCC
    /// prompt).
    automation: Mutex<String>,
    counters: Counters,
}

impl<B: Backend> App<B> {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend: B,
        token: impl Into<String>,
        cursor: CursorStore,
        participants: Vec<String>,
        poll_interval: Duration,
        catchup_rows: u32,
        page_limit: u32,
        attachments: bool,
    ) -> Self {
        Self {
            backend: Arc::new(backend),
            token: token.into(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            cursor,
            participants,
            poll_interval,
            catchup_rows,
            page_limit,
            attachments,
            cache: Mutex::new(SendCache::new()),
            send_lock: tokio::sync::Mutex::new(()),
            automation: Mutex::new("unknown".to_string()),
            counters: Counters::default(),
        }
    }

    /// The axum router over this state.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/v1/health", get(health::<B>))
            .route("/v1/updates", get(updates::<B>))
            .route("/v1/send", post(send::<B>))
            .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT_BYTES))
            .with_state(Arc::clone(self))
    }
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Constant-time bearer-token check.
fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(header) = headers.get(axum::http::header::AUTHORIZATION) else {
        return false;
    };
    let Ok(header) = header.to_str() else {
        return false;
    };
    let Some(presented) = header.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(presented.as_bytes(), token.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn unauthorized() -> Response {
    error_response(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "missing or wrong bearer token",
        true,
    )
}

fn error_response(status: StatusCode, kind: &str, detail: &str, retry_safe: bool) -> Response {
    (
        status,
        Json(json!({
            "protocol": PROTOCOL,
            "ok": false,
            "kind": kind,
            "detail": detail,
            "retry_safe": retry_safe,
        })),
    )
        .into_response()
}

/// Maps an RPC failure to the ABI's status code + kind.
fn rpc_error_response(error: &RpcError) -> Response {
    let (kind, retry_safe) = error.kind();
    let status = match kind.as_str() {
        "database_unreadable" | "bridge_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        "bad_request" => StatusCode::BAD_REQUEST,
        "bridge_busy" => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };
    error_response(status, &kind, &error.to_string(), retry_safe)
}

// ---------------------------------------------------------------------------
// /v1/health
// ---------------------------------------------------------------------------

async fn health<B: Backend>(State(app): State<Arc<App<B>>>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &app.token) {
        return unauthorized();
    }
    let status = match app.backend.status().await {
        Ok(status) => status,
        Err(error) => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.kind().0,
                &error.to_string(),
                true,
            );
        }
    };
    let rpc = status
        .supported_methods
        .iter()
        .any(|method| method == "messages.after")
        && status
            .supported_methods
            .iter()
            .any(|method| method == "send");
    let automation = app.automation.lock().expect("automation lock").clone();
    Json(json!({
        "protocol": PROTOCOL,
        "bridge_version": env!("CARGO_PKG_VERSION"),
        "instance_id": app.instance_id,
        "imsg": {
            "version": status.version,
            "rpc": rpc,
            "private_api": status.private_api,
        },
        "database": {
            "ready": status.db_ready,
            "path": status.db_path,
            "generation": app.cursor.generation(),
        },
        "automation": automation,
        "watch": {
            "active": true,
            "last_rowid": app.cursor.rowid(),
            "buffered": 0,
            "participants": app.participants,
        },
        "counters": {
            "updates_served": app.counters.updates_served.load(Ordering::Relaxed),
            "sends": app.counters.sends.load(Ordering::Relaxed),
            "uncertain_sends": app.counters.uncertain_sends.load(Ordering::Relaxed),
        },
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// /v1/updates
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct UpdatesQuery {
    #[serde(default)]
    since_rowid: u64,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    wait_ms: Option<u64>,
}

async fn updates<B: Backend>(
    State(app): State<Arc<App<B>>>,
    headers: HeaderMap,
    Query(query): Query<UpdatesQuery>,
) -> Response {
    if !authorized(&headers, &app.token) {
        return unauthorized();
    }
    let limit = query
        .limit
        .unwrap_or(app.page_limit)
        .clamp(1, app.page_limit.min(500));
    let wait = Duration::from_millis(query.wait_ms.unwrap_or(0).min(MAX_WAIT_MS));
    let mut gap = false;
    // `since_rowid: 0` = the bridge's bounded tail replay: walk back
    // `catchup_rows` from the persisted cursor so an adapter restart
    // cannot lose the rows the bridge already handed out.
    let mut start = if query.since_rowid == 0 {
        app.cursor
            .rowid()
            .saturating_sub(u64::from(app.catchup_rows))
    } else {
        query.since_rowid
    };
    if start > app.cursor.rowid() {
        // A cursor ahead of this database (a stale generation): reset.
        start = app.cursor.rowid();
        gap = true;
    }
    let deadline = Instant::now() + wait;
    let mut collected: Vec<Value> = Vec::new();
    let mut next_rowid = start;
    let mut has_more = false;
    for _ in 0..100 {
        let page = match app.backend.after(start, limit, app.attachments).await {
            Ok(page) => page,
            Err(error) => return rpc_error_response(&error),
        };
        if page.next_rowid < start {
            // The database was replaced under us (ROWIDs went backwards).
            app.cursor.rotate_generation();
            gap = true;
            next_rowid = page.next_rowid;
        } else {
            next_rowid = page.next_rowid;
        }
        app.cursor.observe(next_rowid);
        has_more = page.has_more;
        for raw in &page.messages {
            if let Some(reduced) = reduce_message(raw, &app.participants) {
                collected.push(reduced);
            }
        }
        start = next_rowid;
        if !collected.is_empty() {
            break;
        }
        if !has_more && Instant::now() >= deadline {
            break;
        }
        if !has_more {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            tokio::time::sleep(app.poll_interval.min(remaining)).await;
        }
    }
    app.counters.updates_served.fetch_add(1, Ordering::Relaxed);
    Json(json!({
        "protocol": PROTOCOL,
        "generation": app.cursor.generation(),
        "messages": collected,
        "next_rowid": next_rowid,
        "has_more": has_more,
        "gap": gap,
    }))
    .into_response()
}

/// Reduces one raw `imsg` message to the ABI's documented shape and
/// applies the bridge's **privacy** filter. Mutation notifications
/// (`type: edited|unsent`), rows outside the participant filter and
/// malformed rows are dropped here — the bridge is not the policy
/// owner, it just does not let them leave the Mac.
fn reduce_message(raw: &Value, participants: &[String]) -> Option<Value> {
    if raw.get("type").is_some() {
        return None; // an edit/unsend update, not a new row
    }
    let rowid = raw.get("id").and_then(Value::as_u64)?;
    if !participants.is_empty() {
        let sender = raw.get("sender").and_then(Value::as_str).unwrap_or("");
        let normalized_sender = normalize_handle(sender);
        let allowed = participants.iter().any(|participant| {
            normalize_handle(participant)
                .zip(normalized_sender.clone())
                .is_some_and(|(a, b)| a == b)
        });
        if !allowed {
            return None;
        }
    }
    let created_at = raw
        .get("created_at")
        .and_then(Value::as_str)
        .map(str::to_string);
    let created_at_ms = created_at.as_deref().and_then(parse_rfc3339_ms);
    let attachments = project_attachments(rowid, raw.get("attachments"));
    let text = text_with_attachment_markers(raw.get("text").and_then(Value::as_str), &attachments);
    Some(json!({
        "rowid": rowid,
        "guid": raw.get("guid").and_then(Value::as_str).unwrap_or(""),
        "chat_id": raw.get("chat_id").cloned().unwrap_or(Value::Null),
        "chat_guid": raw.get("chat_guid").cloned().unwrap_or(Value::Null),
        "chat_identifier": raw.get("chat_identifier").cloned().unwrap_or(Value::Null),
        "sender": raw.get("sender").cloned().unwrap_or(Value::Null),
        "text": text.map(Value::String).unwrap_or(Value::Null),
        "attachments": attachments,
        "created_at": created_at,
        "created_at_ms": created_at_ms,
        "service": raw.get("service").cloned().unwrap_or(Value::Null),
        "is_from_me": raw.get("is_from_me").and_then(Value::as_bool).unwrap_or(false),
        "is_group": raw.get("is_group").and_then(Value::as_bool).unwrap_or(false),
        "is_reaction": raw.get("is_reaction").and_then(Value::as_bool).unwrap_or(false),
    }))
}

/// What Messages writes into `text` for one item it could not put in
/// words (an attachment, a sticker, a poll).
const OBJECT_REPLACEMENT: char = '\u{FFFC}';

/// The attachment fields the bridge lets off the Mac, and nothing else
/// — `imsg` attachment objects also carry absolute paths and internal
/// bookkeeping (CHANNEL-IMESSAGE §3.2: the bridge reduces, the adapter
/// decides). `filename` is reduced to its basename for that reason; the
/// reading side resolves bytes by `attachment_id` (`{rowid}:{index}`),
/// never by a path the Mac exported.
fn project_attachments(rowid: u64, raw: Option<&Value>) -> Vec<Value> {
    let Some(items) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            json!({
                "attachment_id": format!("{rowid}:{index}"),
                "filename": item
                    .get("filename")
                    .and_then(Value::as_str)
                    .map(basename)
                    .map(Value::from)
                    .unwrap_or(Value::Null),
                "mime_type": item.get("mime_type").cloned().unwrap_or(Value::Null),
                "transfer_name": item.get("transfer_name").cloned().unwrap_or(Value::Null),
                "total_bytes": item.get("total_bytes").cloned().unwrap_or(Value::Null),
                "is_sticker": item.get("is_sticker").and_then(Value::as_bool).unwrap_or(false),
            })
        })
        .collect()
}

/// `/a/b/IMG_0001.heic` -> `IMG_0001.heic` (no directory layout leaves
/// the Mac).
fn basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

/// Replaces every U+FFFC with `[image]`/`[attachment]` so a photo is not
/// silently dropped downstream as "no text", and gives an
/// attachment-only row (no `text` at all) the same honest marker.
fn text_with_attachment_markers(text: Option<&str>, attachments: &[Value]) -> Option<String> {
    match text {
        Some(text) if text.contains(OBJECT_REPLACEMENT) => {
            let mut index = 0usize;
            let mut out = String::with_capacity(text.len() + 8);
            for character in text.chars() {
                if character == OBJECT_REPLACEMENT {
                    out.push_str(attachment_marker(attachments.get(index)));
                    index += 1;
                } else {
                    out.push(character);
                }
            }
            Some(out)
        }
        Some(text) if text.trim().is_empty() && !attachments.is_empty() => {
            Some(attachment_marker(attachments.first()).to_string())
        }
        Some(text) => Some(text.to_string()),
        None if !attachments.is_empty() => Some(attachment_marker(attachments.first()).to_string()),
        None => None,
    }
}

fn attachment_marker(attachment: Option<&Value>) -> &'static str {
    let is_image = attachment
        .and_then(|item| item.get("mime_type"))
        .and_then(Value::as_str)
        .is_some_and(|mime| mime.to_ascii_lowercase().starts_with("image/"));
    if is_image { "[image]" } else { "[attachment]" }
}

fn parse_rfc3339_ms(text: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|stamp| stamp.timestamp_millis().max(0) as u64)
}

/// The bridge's copy of the handle normalizer (CHANNEL-IMESSAGE §6) —
/// used only for the privacy filter, never for a policy decision.
#[must_use]
pub fn normalize_handle(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix('+') {
        let digits: String = rest
            .chars()
            .filter(|c| !matches!(c, ' ' | '-' | '.' | '(' | ')'))
            .collect();
        if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            return Some(format!("+{digits}"));
        }
        return Some(trimmed.to_string());
    }
    if trimmed.contains('@') {
        return Some(trimmed.to_ascii_lowercase());
    }
    Some(trimmed.to_string())
}

// ---------------------------------------------------------------------------
// /v1/send
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SendBody {
    #[serde(default)]
    protocol: Option<u32>,
    client_id: String,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    chat_guid: Option<String>,
    text: String,
    #[serde(default = "default_service")]
    service: String,
}

fn default_service() -> String {
    "auto".into()
}

async fn send<B: Backend>(
    State(app): State<Arc<App<B>>>,
    headers: HeaderMap,
    Json(body): Json<SendBody>,
) -> Response {
    if !authorized(&headers, &app.token) {
        return unauthorized();
    }
    if body.protocol.is_some_and(|protocol| protocol != PROTOCOL) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "unsupported protocol version",
            true,
        );
    }
    if body.client_id.trim().is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "client_id is required (the delivery idempotency key)",
            true,
        );
    }
    if body.text.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "text must not be empty",
            true,
        );
    }
    if body.to.is_none() && body.chat_guid.is_none() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "send requires `to` or `chat_guid`",
            true,
        );
    }
    if !matches!(body.service.as_str(), "auto" | "imessage") {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "service must be `auto` or `imessage` (sms is phase 2)",
            true,
        );
    }
    // Serialize sends: order is preserved and a duplicate `client_id`
    // arriving mid-flight waits, then hits the cache.
    let _send = app.send_lock.lock().await;
    let now = Instant::now();
    if let Some(cached) = app
        .cache
        .lock()
        .expect("send cache lock")
        .get(&body.client_id, now)
    {
        return match cached {
            Cached::Ok(mut ok) => {
                ok["deduplicated"] = Value::Bool(true);
                (StatusCode::OK, Json(ok)).into_response()
            }
            Cached::Uncertain { kind, detail } => {
                error_response(StatusCode::UNPROCESSABLE_ENTITY, &kind, &detail, false)
            }
        };
    }
    app.counters.sends.fetch_add(1, Ordering::Relaxed);
    let outcome = app
        .backend
        .send(
            body.to.clone(),
            body.chat_guid.clone(),
            body.text.clone(),
            body.service.clone(),
        )
        .await;
    match outcome {
        Ok(result) => {
            *app.automation.lock().expect("automation lock") = "granted".to_string();
            let guid = result
                .get("guid")
                .and_then(Value::as_str)
                .map(str::to_string);
            let rowid = result.get("id").and_then(Value::as_u64);
            let service = result
                .get("service")
                .and_then(Value::as_str)
                .unwrap_or(&body.service)
                .to_string();
            let verified = guid.is_some() || rowid.is_some();
            let message_id = guid
                .clone()
                .or_else(|| rowid.map(|rowid| format!("imessage:{rowid}")))
                .unwrap_or_else(|| format!("kian/{}", body.client_id));
            let ok = json!({
                "protocol": PROTOCOL,
                "ok": true,
                "message_id": message_id,
                "rowid": rowid,
                "guid": guid,
                "service": service,
                "verified": verified,
                "deduplicated": false,
            });
            app.cache.lock().expect("send cache lock").insert(
                body.client_id.clone(),
                Cached::Ok(ok.clone()),
                now,
            );
            (StatusCode::OK, Json(ok)).into_response()
        }
        Err(error) => {
            let (kind, retry_safe) = error.kind();
            let detail = error.to_string();
            if kind == "not_started"
                && (detail.contains("-1743")
                    || detail.to_ascii_lowercase().contains("not authorized"))
            {
                *app.automation.lock().expect("automation lock") = "denied".to_string();
            }
            if kind == "may_have_completed" || kind == "still_in_flight" {
                app.counters.uncertain_sends.fetch_add(1, Ordering::Relaxed);
                app.cache.lock().expect("send cache lock").insert(
                    body.client_id.clone(),
                    Cached::Uncertain {
                        kind: kind.clone(),
                        detail: detail.clone(),
                    },
                    now,
                );
            }
            let status = match kind.as_str() {
                "database_unreadable" | "bridge_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
                "bad_request" => StatusCode::BAD_REQUEST,
                "bridge_busy" => StatusCode::TOO_MANY_REQUESTS,
                _ => StatusCode::UNPROCESSABLE_ENTITY,
            };
            error_response(status, &kind, &detail, retry_safe)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imsg::{AfterPage, Status};
    use axum::body::to_bytes;
    use axum::http::header::AUTHORIZATION;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    /// (to, chat_guid, text, service) of every backend send.
    type SendArgs = (Option<String>, Option<String>, String, String);

    struct Fake {
        status: Mutex<Result<Status, RpcError>>,
        pages: Mutex<VecDeque<Result<AfterPage, RpcError>>>,
        sends: AtomicU64,
        send_result: Mutex<Result<Value, RpcError>>,
        send_args: Mutex<Vec<SendArgs>>,
    }

    impl Fake {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                status: Mutex::new(Ok(Status {
                    version: "0.11.1".into(),
                    protocol_version: 1,
                    db_ready: true,
                    db_path: "/tmp/chat.db".into(),
                    supported_methods: vec![
                        "status".into(),
                        "messages.after".into(),
                        "send".into(),
                    ],
                    private_api: false,
                })),
                pages: Mutex::new(VecDeque::new()),
                sends: AtomicU64::new(0),
                send_result: Mutex::new(Ok(json!({"ok": true, "id": 9, "guid": "G-9"}))),
                send_args: Mutex::new(Vec::new()),
            })
        }
        fn push_page(&self, page: AfterPage) {
            self.pages.lock().unwrap().push_back(Ok(page));
        }
    }

    impl Backend for Arc<Fake> {
        async fn status(&self) -> Result<Status, RpcError> {
            self.status.lock().unwrap().clone()
        }
        async fn after(
            &self,
            since_rowid: u64,
            _limit: u32,
            _with_attachments: bool,
        ) -> Result<AfterPage, RpcError> {
            self.pages
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(AfterPage {
                    messages: Vec::new(),
                    next_rowid: since_rowid,
                    has_more: false,
                }))
        }
        async fn send(
            &self,
            to: Option<String>,
            chat_guid: Option<String>,
            text: String,
            service: String,
        ) -> Result<Value, RpcError> {
            self.sends.fetch_add(1, Ordering::Relaxed);
            self.send_args
                .lock()
                .unwrap()
                .push((to, chat_guid, text, service));
            self.send_result.lock().unwrap().clone()
        }
    }

    fn app(fake: Arc<Fake>, participants: Vec<String>) -> Arc<App<Arc<Fake>>> {
        let dir = std::env::temp_dir().join(format!("kian-bridge-app-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = PathBuf::from(&dir).join("chat.db");
        std::fs::write(&db, b"x").unwrap();
        let cursor = CursorStore::load(&dir, &db);
        cursor.observe(100);
        Arc::new(App::new(
            fake,
            "token-0123456789",
            cursor,
            participants,
            Duration::from_millis(1),
            10,
            100,
            true,
        ))
    }

    fn bearer() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer token-0123456789".parse().unwrap());
        headers
    }

    fn message(rowid: u64, sender: &str) -> Value {
        json!({
            "id": rowid,
            "guid": format!("G-{rowid}"),
            "chat_id": 3,
            "chat_guid": format!("iMessage;-;{sender}"),
            "chat_identifier": sender,
            "sender": sender,
            "text": format!("hi {rowid}"),
            "created_at": "2026-11-19T15:31:55Z",
            "service": "iMessage",
            "is_from_me": false,
            "is_group": false
        })
    }

    async fn body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn photo_message(rowid: u64, sender: &str) -> Value {
        json!({
            "id": rowid,
            "guid": format!("G-{rowid}"),
            "chat_id": 3,
            "chat_guid": format!("iMessage;-;{sender}"),
            "chat_identifier": sender,
            "sender": sender,
            "text": "\u{FFFC}",
            "created_at": "2026-11-19T15:31:55Z",
            "service": "iMessage",
            "is_from_me": false,
            "is_group": false,
            "attachments": [{
                "id": 7,
                "filename": "/Users/someone/Library/Messages/Attachments/aa/bb/IMG_0001.heic",
                "transfer_name": "IMG_0001.heic",
                "mime_type": "image/heic",
                "total_bytes": 2_345_678_u64,
                "is_sticker": false,
                "internal_hint": "must not leave the Mac",
            }],
        })
    }

    #[test]
    fn attachments_are_projected_to_a_whitelist_without_paths() {
        let reduced = reduce_message(&photo_message(41, "+8618521321872"), &[])
            .expect("an attachment row is reduced, not dropped");
        let attachments = reduced["attachments"].as_array().unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0]["attachment_id"], json!("41:0"));
        assert_eq!(attachments[0]["filename"], json!("IMG_0001.heic"));
        assert_eq!(attachments[0]["mime_type"], json!("image/heic"));
        assert_eq!(attachments[0]["transfer_name"], json!("IMG_0001.heic"));
        assert_eq!(attachments[0]["total_bytes"], json!(2_345_678_u64));
        assert_eq!(attachments[0]["is_sticker"], json!(false));
        let wire = reduced.to_string();
        assert!(
            !wire.contains("/Users/"),
            "an absolute path left the Mac: {wire}"
        );
        assert!(
            !wire.contains("internal_hint"),
            "an unknown field left the Mac: {wire}"
        );
    }

    #[test]
    fn the_object_replacement_character_becomes_a_marker() {
        let reduced = reduce_message(&photo_message(42, "+8618521321872"), &[]).unwrap();
        assert_eq!(reduced["text"], json!("[image]"));
        let mut raw = photo_message(43, "+8618521321872");
        raw["text"] = json!("look \u{FFFC} at this \u{FFFC}");
        raw["attachments"] = json!([
            {"mime_type": "image/jpeg"},
            {"mime_type": "application/pdf", "transfer_name": "a.pdf"},
        ]);
        let reduced = reduce_message(&raw, &[]).unwrap();
        assert_eq!(reduced["text"], json!("look [image] at this [attachment]"));
    }

    #[test]
    fn a_text_only_row_keeps_its_text_and_has_no_attachments() {
        let reduced = reduce_message(&message(44, "+8618521321872"), &[]).unwrap();
        assert_eq!(reduced["text"], json!("hi 44"));
        assert_eq!(reduced["attachments"], json!([]));
    }

    #[tokio::test]
    async fn every_route_requires_the_bearer_token() {
        let app = app(Fake::new(), vec![]);
        assert_eq!(
            health(State(app.clone()), HeaderMap::new()).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            updates(
                State(app.clone()),
                HeaderMap::new(),
                Query(UpdatesQuery {
                    since_rowid: 0,
                    limit: None,
                    wait_ms: None
                })
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn health_reports_the_contract_shape() {
        let app = app(Fake::new(), vec!["+14155551212".into()]);
        let response = health(State(app), bearer()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body(response).await;
        assert_eq!(body["protocol"], 1);
        assert_eq!(body["imsg"]["rpc"], true);
        assert_eq!(body["database"]["ready"], true);
        assert_eq!(body["automation"], "unknown");
        assert_eq!(body["watch"]["participants"][0], "+14155551212");
        assert!(
            body["instance_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
    }

    #[tokio::test]
    async fn updates_reduce_and_filter_rows() {
        let fake = Fake::new();
        fake.push_page(AfterPage {
            messages: vec![message(101, "+14155551212"), message(102, "+19995550000")],
            next_rowid: 102,
            has_more: false,
        });
        let app = app(fake, vec!["+14155551212".into()]);
        let response = updates(
            State(app),
            bearer(),
            Query(UpdatesQuery {
                since_rowid: 99,
                limit: Some(50),
                wait_ms: Some(0),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body(response).await;
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        assert_eq!(body["messages"][0]["rowid"], 101);
        assert_eq!(body["messages"][0]["guid"], "G-101");
        assert_eq!(body["messages"][0]["created_at_ms"], 1_795_102_315_000u64);
        assert_eq!(body["next_rowid"], 102);
        assert_eq!(body["gap"], false);
    }

    #[tokio::test]
    async fn mutation_updates_never_leave_the_bridge() {
        let fake = Fake::new();
        let mut edited = message(103, "+14155551212");
        edited["type"] = json!("edited");
        fake.push_page(AfterPage {
            messages: vec![edited],
            next_rowid: 103,
            has_more: false,
        });
        let app = app(fake, vec![]);
        let body = body(
            updates(
                State(app),
                bearer(),
                Query(UpdatesQuery {
                    since_rowid: 102,
                    limit: None,
                    wait_ms: Some(0),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["messages"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn a_cursor_ahead_of_the_bridge_is_a_reset_with_a_gap() {
        let fake = Fake::new();
        let app = app(fake, vec![]);
        let body = body(
            updates(
                State(app.clone()),
                bearer(),
                Query(UpdatesQuery {
                    since_rowid: 9_999,
                    limit: None,
                    wait_ms: Some(0),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(body["gap"], true);
        assert_eq!(body["next_rowid"], 100);
    }

    #[tokio::test]
    async fn send_returns_the_observed_guid_and_is_idempotent() {
        let fake = Fake::new();
        let app = app(fake.clone(), vec![]);
        let body_json = |client_id: &str| SendBody {
            protocol: Some(1),
            client_id: client_id.into(),
            to: Some("+14155551212".into()),
            chat_guid: None,
            text: "hello".into(),
            service: "imessage".into(),
        };
        let first = send(State(app.clone()), bearer(), Json(body_json("cid-1"))).await;
        assert_eq!(first.status(), StatusCode::OK);
        let first = body(first).await;
        assert_eq!(first["ok"], true);
        assert_eq!(first["message_id"], "G-9");
        assert_eq!(first["verified"], true);
        assert_eq!(first["deduplicated"], false);

        // The same client_id is answered from the observed-result cache.
        let second = send(State(app.clone()), bearer(), Json(body_json("cid-1"))).await;
        assert_eq!(second.status(), StatusCode::OK);
        let second = body(second).await;
        assert_eq!(second["deduplicated"], true);
        assert_eq!(fake.sends.load(Ordering::Relaxed), 1);

        // A different client_id really sends.
        let third = send(State(app), bearer(), Json(body_json("cid-2"))).await;
        assert_eq!(third.status(), StatusCode::OK);
        assert_eq!(fake.sends.load(Ordering::Relaxed), 2);
        let args = fake.send_args.lock().unwrap().clone();
        assert_eq!(args[0].0.as_deref(), Some("+14155551212"));
        assert_eq!(args[0].3, "imessage");
    }

    #[tokio::test]
    async fn an_uncertain_send_is_honest_and_cached() {
        let fake = Fake::new();
        *fake.send_result.lock().unwrap() = Err(RpcError::Rejected {
            code: -32001,
            message: "ambiguous receipt".into(),
            disposition: Some("may_have_completed".into()),
            retry_safe: Some(false),
        });
        let app = app(fake.clone(), vec![]);
        let request = SendBody {
            protocol: Some(1),
            client_id: "cid-u".into(),
            to: Some("+14155551212".into()),
            chat_guid: None,
            text: "hello".into(),
            service: "auto".into(),
        };
        let response = send(State(app.clone()), bearer(), Json(request)).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body(response).await;
        assert_eq!(body["ok"], false);
        assert_eq!(body["kind"], "may_have_completed");
        assert_eq!(body["retry_safe"], false);
        assert_eq!(app.counters.uncertain_sends.load(Ordering::Relaxed), 1);

        // A duplicate of the uncertain attempt does not resend.
        let again = send(
            State(app),
            bearer(),
            Json(SendBody {
                protocol: Some(1),
                client_id: "cid-u".into(),
                to: Some("+14155551212".into()),
                chat_guid: None,
                text: "hello".into(),
                service: "auto".into(),
            }),
        )
        .await;
        assert_eq!(again.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(fake.sends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_not_started_failure_is_not_cached_so_a_retry_is_real() {
        let fake = Fake::new();
        *fake.send_result.lock().unwrap() = Err(RpcError::Rejected {
            code: -32603,
            message: "Not authorized to send Apple events to Messages. (-1743)".into(),
            disposition: None,
            retry_safe: None,
        });
        let app = app(fake.clone(), vec![]);
        let request = || SendBody {
            protocol: Some(1),
            client_id: "cid-1".into(),
            to: Some("+14155551212".into()),
            chat_guid: None,
            text: "hello".into(),
            service: "auto".into(),
        };
        let first = send(State(app.clone()), bearer(), Json(request())).await;
        assert_eq!(first.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body(first).await["kind"], "not_started");
        assert_eq!(app.automation.lock().unwrap().as_str(), "denied");
        // The retry really calls the backend again.
        let _ = send(State(app), bearer(), Json(request())).await;
        assert_eq!(fake.sends.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn send_validation_is_fail_closed() {
        let app = app(Fake::new(), vec![]);
        let missing_target = SendBody {
            protocol: Some(1),
            client_id: "c".into(),
            to: None,
            chat_guid: None,
            text: "hi".into(),
            service: "auto".into(),
        };
        assert_eq!(
            send(State(app.clone()), bearer(), Json(missing_target))
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let bad_service = SendBody {
            protocol: Some(1),
            client_id: "c".into(),
            to: Some("+1".into()),
            chat_guid: None,
            text: "hi".into(),
            service: "sms".into(),
        };
        assert_eq!(
            send(State(app), bearer(), Json(bad_service)).await.status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn normalize_handle_matches_the_adapter_contract() {
        assert_eq!(
            normalize_handle("+1 (415) 555-1212"),
            Some("+14155551212".into())
        );
        assert_eq!(
            normalize_handle("Me@Example.com"),
            Some("me@example.com".into())
        );
        assert_eq!(normalize_handle(""), None);
    }
}
