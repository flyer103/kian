//! Inbound webhook receiver plane (ADR-011) — the fourth
//! server surface, beside the control plane, the ops plane
//! and the metrics route.
//!
//! `POST /webhooks/github` is the only inbound HTTP surface: it speaks
//! for the internet, not for an authenticated operator or client, so
//! it is untrusted-by-default in the strongest sense — **nothing is
//! parsed before its bytes are cryptographically authenticated** (the
//! discipline). The module is trait-isolated exactly like
//! [`crate::api`] and [`crate::ops`]: handlers speak JSON and call
//! **nothing but** the [`WebhookPlane`] trait, whose sole capability
//! is "verify, then hand a verified delivery to the subscription
//! pipeline" — an inbox write, the same control-plane legality as the
//! subscription service. The resident iron-rule test greps this
//! module with the same forbidden-token table (no execution or
//! store-write symbol is nameable here).
//!
//! Verification order (each step rejects honestly before the next):
//!
//! 1. **Size cap + content-type gate** — the body cap rides the
//!    router layer (`DefaultBodyLimit`), so an oversized body is a
//!    413 **before any signature work**; a non-JSON content type is a
//!    415.
//! 2. **HMAC-SHA256** over the raw body bytes against
//!    `X-Hub-Signature-256` (`sha256=<hex>`), compared in constant
//!    time. The secret is env-only (`KIAN_GITHUB_WEBHOOK_SECRET`) and
//!    fail-closed at assembly when the route is configured — secrets
//!    never live in `kian.yaml`.
//! 3. **Per-source token-bucket rate limit** — in-memory,
//!    per-process (the v1 honest bound; a multi-replica deployment
//!    gets N buckets — documented in ADR-011, not hidden).
//! 4. **Delivery-id dedupe** — deliberately *not* here: the delivery
//!    id is the channel event id, so the subscription pipeline's
//!    existing `(session, event_id)` idempotency is the durable
//!    guard, exactly like every other channel.
//!
//! Every rejection is an honest 4xx response with an audit log line;
//! no event, no row, no parse.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ConnectInfo;
use axum::extract::DefaultBodyLimit;
use axum::extract::Request;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::future::BoxFuture;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;

/// The env var holding the webhook secret (env-only, fail-closed at
/// assembly when the route is configured).
pub const GITHUB_WEBHOOK_SECRET_ENV: &str = "KIAN_GITHUB_WEBHOOK_SECRET";

/// GitHub's event-type header.
pub const GITHUB_EVENT_HEADER: &str = "x-github-event";
/// GitHub's delivery-id header (the idempotency key downstream).
pub const GITHUB_DELIVERY_HEADER: &str = "x-github-delivery";
/// GitHub's HMAC header (`sha256=<hex>`).
pub const SIGNATURE_HEADER: &str = "x-hub-signature-256";

/// Default body cap: 1 MiB — generous for CI-train push payloads,
/// still bounded before any cryptographic work.
pub const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;
/// Default per-source bucket capacity.
pub const DEFAULT_RATE_CAPACITY: u64 = 60;
/// Default bucket refill, tokens per second per source.
pub const DEFAULT_RATE_REFILL_PER_SECOND: u64 = 2;

/// How many distinct sources the per-process bucket map tracks before
/// idle (full) buckets are evicted — the honest in-memory bound.
const MAX_TRACKED_SOURCES: usize = 8192;

// ---------------------------------------------------------------------------
// The plane (the ONLY thing webhook handlers may touch)
// ---------------------------------------------------------------------------

/// What the webhook layer may ask of the server: hand one
/// **cryptographically verified** GitHub delivery to the subscription
/// pipeline (an inbox write). Everything else is structurally out of
/// reach — no execution surface, no store write, no projection read.
pub trait WebhookPlane: Send + Sync {
    /// Surfaces the verified delivery; the returned count is how many
    /// channel events surfaced (0 = an honest downstream drop such as
    /// an unsupported event type, a filtered type, a duplicate
    /// delivery id, or a repo outside the mapping table — the
    /// pipeline's audit lines say which).
    fn ingest_verified_github(
        &self,
        event_type: String,
        delivery_id: String,
        body: Value,
    ) -> BoxFuture<'_, Result<usize, WebhookError>>;
}

/// Webhook-plane failures (internal hand-off problems only; every
/// *input* problem is an honest 4xx in the handler, never this).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct WebhookError(pub String);

/// The receiver limits (from `kian.yaml`, defaults above).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookLimits {
    /// Maximum raw body bytes (413 above, enforced before signatures).
    pub max_body_bytes: usize,
    /// Per-source token-bucket capacity.
    pub rate_capacity: u64,
    /// Per-source bucket refill, tokens per second.
    pub rate_refill_per_second: u64,
}

impl Default for WebhookLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            rate_capacity: DEFAULT_RATE_CAPACITY,
            rate_refill_per_second: DEFAULT_RATE_REFILL_PER_SECOND,
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    plane: Arc<dyn WebhookPlane>,
    secret: Arc<Vec<u8>>,
    limits: WebhookLimits,
    /// Per-source token buckets (source = the connection's peer IP
    /// when the serve layer provides it, else one shared
    /// "unattributed" bucket — the tests' in-process stand-).
    buckets: Arc<Mutex<BTreeMap<String, Bucket>>>,
}

/// Builds the webhook router (`POST /webhooks/github`). Mounted
/// by the serve assembly **only** when the route is configured (a
/// `type: github` channel exists); otherwise the path 404s.
pub fn router(plane: Arc<dyn WebhookPlane>, secret: String, limits: WebhookLimits) -> Router {
    let cap = limits.max_body_bytes;
    Router::new()
        .route("/webhooks/github", post(github_delivery))
        // Step 1a: the size cap rides the router layer — an oversized
        // body is a 413 before the handler (and any signature work)
        // runs.
        .layer(DefaultBodyLimit::max(cap))
        .with_state(AppState {
            plane,
            secret: Arc::new(secret.into_bytes()),
            limits,
            buckets: Arc::new(Mutex::new(BTreeMap::new())),
        })
}

/// One rejection's audit line (honest observability: every 4xx is
/// explained, nothing is silently swallowed).
fn audit(reason: &'static str, source: &str) {
    tracing::warn!(target: "kian::webhook", source = %source, reason, "webhook audit: delivery rejected");
}

fn rejection(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"status": status.as_u16(), "message": message}})),
    )
        .into_response()
}

async fn github_delivery(State(state): State<AppState>, req: Request) -> Response {
    // The per-source bucket key: the connection peer when the serve
    // layer supplies connect info, else a single shared bucket.
    let source = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip().to_string())
        .unwrap_or_else(|| "unattributed".to_string());

    // -- 1b. content-type gate (before any body work) ----------------
    let content_type = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if mime != "application/json" {
        audit("content type is not application/json", &source);
        return rejection(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content type must be application/json",
        );
    }

    let (parts, body) = req.into_parts();
    // Defense in depth: the router layer already enforces the cap
    // (413); to_bytes carries the same bound so a mid-layer change
    // can never smuggle an unbounded body into the signature step.
    let Ok(bytes) = axum::body::to_bytes(body, state.limits.max_body_bytes).await else {
        audit("body exceeds the configured cap", &source);
        return rejection(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body exceeds the configured cap",
        );
    };

    // -- 2. HMAC-SHA256 over the raw body (precedes any parsing) -----
    let Some(signature) = parts
        .headers
        .get(SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        audit("missing X-Hub-Signature-256", &source);
        return rejection(
            StatusCode::UNAUTHORIZED,
            "missing X-Hub-Signature-256 header",
        );
    };
    if !verify_signature(&state.secret, &bytes, signature) {
        audit("invalid signature", &source);
        return rejection(StatusCode::UNAUTHORIZED, "invalid webhook signature");
    }

    // -- 3. per-source token bucket ----------------------------------
    if !take_token(&state, &source) {
        audit("rate limited", &source);
        return rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded, retry later",
        );
    }

    // -- 4. parse (now authenticated) + hand off to the pipeline -----
    let Some(event_type) = header_string(&parts, GITHUB_EVENT_HEADER) else {
        audit("missing X-GitHub-Event", &source);
        return rejection(StatusCode::BAD_REQUEST, "missing X-GitHub-Event header");
    };
    let Some(delivery_id) = header_string(&parts, GITHUB_DELIVERY_HEADER) else {
        audit("missing X-GitHub-Delivery", &source);
        return rejection(StatusCode::BAD_REQUEST, "missing X-GitHub-Delivery header");
    };
    let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
        // Authenticated garbage is still garbage — an honest 400, not
        // a best-effort parse.
        audit("authenticated body is not valid JSON", &source);
        return rejection(StatusCode::BAD_REQUEST, "body is not valid JSON");
    };
    match state
        .plane
        .ingest_verified_github(event_type, delivery_id, body)
        .await
    {
        Ok(surfaced) => (StatusCode::OK, Json(json!({"surfaced": surfaced}))).into_response(),
        Err(e) => {
            tracing::error!(
                target: "kian::webhook",
                source = %source,
                error = %e,
                "webhook audit: verified hand-off failed"
            );
            rejection(
                StatusCode::INTERNAL_SERVER_ERROR,
                "verified delivery could not be handed to the subscription pipeline",
            )
        }
    }
}

fn header_string(parts: &axum::http::request::Parts, name: &str) -> Option<String> {
    parts
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 verification (constant-time compare)
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// Verifies `sha256=<hex>` HMAC-SHA256 over `body` with `secret`.
/// The comparison is constant time (`verify_slice` rides `subtle`'s
/// `CtEq`); malformed prefixes or hex are plain rejections.
fn verify_signature(secret: &[u8], body: &[u8], header: &str) -> bool {
    let Some(hex) = header.strip_prefix("sha256=") else {
        return false;
    };
    let Some(digest) = decode_hex(hex) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&digest).is_ok()
}

/// Lowercase-hex decode (no external crate: the webhook surface keeps
/// its dependency surface to hmac + sha2).
fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.is_empty() || !text.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    for pair in bytes.chunks(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        out.push(high << 4 | low);
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Per-source token bucket (in-memory, per-process — the v1 bound)
// ---------------------------------------------------------------------------

struct Bucket {
    tokens: f64,
    updated_ms: u128,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Takes one token for `source`, lazily refilling first. Returns
/// false when the bucket is empty (the caller rejects with 429).
fn take_token(state: &AppState, source: &str) -> bool {
    let now = now_ms();
    let capacity = state.limits.rate_capacity as f64;
    let per_ms = state.limits.rate_refill_per_second as f64 / 1000.0;
    let mut buckets = state.buckets.lock().expect("webhook bucket lock");
    // Bounded map: past the tracking bound, evict idle (full) buckets
    // so a source sweep cannot grow the map without limit.
    if buckets.len() > MAX_TRACKED_SOURCES {
        buckets.retain(|key, bucket| key == source || bucket.tokens < capacity);
    }
    let bucket = buckets.entry(source.to_string()).or_insert(Bucket {
        tokens: capacity,
        updated_ms: now,
    });
    let elapsed = now.saturating_sub(bucket.updated_ms);
    bucket.tokens = (bucket.tokens + elapsed as f64 * per_ms).min(capacity);
    bucket.updated_ms = now;
    if bucket.tokens >= 1.0 {
        bucket.tokens -= 1.0;
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::{WebhookError, WebhookLimits, WebhookPlane, router, verify_signature};
    use hmac::Mac;
    use serde_json::json;

    /// The macOS system proxy intercepts localhost (AGENTS.md gotcha).
    fn force_no_proxy() {
        unsafe {
            std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
            std::env::set_var("no_proxy", "127.0.0.1,localhost");
        }
    }

    /// The shared log buffer: one global capturing subscriber for the
    /// whole test binary (thread-local defaults do not follow async
    /// tasks onto whatever thread services the connection; the repo's
    /// established pattern — see kian-superagent's audit tests — is a
    /// global subscriber over a shared buffer).
    fn logs() -> std::sync::Arc<Mutex<Vec<u8>>> {
        static BUF: std::sync::OnceLock<std::sync::Arc<Mutex<Vec<u8>>>> =
            std::sync::OnceLock::new();
        BUF.get_or_init(|| {
            let buf = std::sync::Arc::new(Mutex::new(Vec::new()));
            let subscriber = tracing_subscriber::fmt()
                .with_writer(MakeCapture(std::sync::Arc::clone(&buf)))
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
            buf
        })
        .clone()
    }

    const SECRET: &str = "whsec_test";

    fn sign(body: &[u8]) -> String {
        let mut mac = super::HmacSha256::new_from_slice(SECRET.as_bytes()).expect("key");
        mac.update(body);
        format!("sha256={}", hex_encode(&mac.finalize().into_bytes()))
    }

    /// Lowercase-hex encode (the shape the header carries).
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A recording fake: proves the handler layer speaks only
    /// WebhookPlane and that nothing is handed off on rejection.
    #[derive(Default)]
    struct FakePlane {
        calls: Mutex<Vec<(String, String, serde_json::Value)>>,
        surfaced: usize,
    }

    impl WebhookPlane for FakePlane {
        fn ingest_verified_github(
            &self,
            event_type: String,
            delivery_id: String,
            body: serde_json::Value,
        ) -> futures::future::BoxFuture<'static, Result<usize, WebhookError>> {
            self.calls
                .lock()
                .unwrap()
                .push((event_type, delivery_id, body));
            let surfaced = self.surfaced;
            Box::pin(async move { Ok(surfaced) })
        }
    }

    async fn spawn_router(plane: std::sync::Arc<FakePlane>, limits: WebhookLimits) -> String {
        let app = router(plane, SECRET.into(), limits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind webhook test router");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        format!("http://{addr}/webhooks/github")
    }

    fn delivery_body() -> serde_json::Value {
        json!({
            "action": "created",
            "issue": {"number": 9, "title": "build broken", "body": "panic in CI"},
            "comment": {"id": 99, "body": "please take a look"},
            "repository": {"id": 1, "full_name": "flyer103/kian-runtime"},
            "sender": {"login": "reporter", "type": "User"}
        })
    }

    fn request(
        url: &str,
        body: &str,
        signature: Option<&str>,
        event: Option<&str>,
        delivery: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let client = reqwest::Client::new();
        let mut builder = client
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string());
        if let Some(signature) = signature {
            builder = builder.header("x-hub-signature-256", signature);
        }
        if let Some(event) = event {
            builder = builder.header("x-github-event", event);
        }
        if let Some(delivery) = delivery {
            builder = builder.header("x-github-delivery", delivery);
        }
        builder
    }

    #[tokio::test]
    async fn valid_signature_hands_the_delivery_to_the_plane() {
        force_no_proxy();
        let plane = std::sync::Arc::new(FakePlane {
            surfaced: 1,
            ..FakePlane::default()
        });
        let url = spawn_router(plane.clone(), WebhookLimits::default()).await;
        let body = serde_json::to_string(&delivery_body()).unwrap();
        let response = request(
            &url,
            &body,
            Some(&sign(body.as_bytes())),
            Some("issue_comment"),
            Some("d-1"),
        )
        .send()
        .await
        .expect("send");
        assert_eq!(response.status(), 200);
        let json: serde_json::Value = response.json().await.expect("json");
        assert_eq!(json["surfaced"], 1);
        let calls = plane.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "exactly one hand-off");
        assert_eq!(calls[0].0, "issue_comment");
        assert_eq!(calls[0].1, "d-1");
        assert_eq!(calls[0].2, delivery_body());
    }

    /// Invalid *and* missing signatures are 401 with zero admissions
    /// and zero parses — the body is deliberately unparseable bytes,
    /// so any parse-before-verify would be observable as a call (the
    /// fake plane would have been asked) or a different status.
    #[tokio::test]
    async fn invalid_or_missing_signature_is_401_with_zero_parses() {
        force_no_proxy();
        for signature in [
            None,
            Some("sha256=deadbeef"),
            Some("not-a-sig"),
            Some("sha256=zz"),
        ] {
            let plane = std::sync::Arc::new(FakePlane::default());
            let url = spawn_router(plane.clone(), WebhookLimits::default()).await;
            // Garbage bytes: not JSON. A parse-first implementation
            // would 400 here instead of 401.
            let response = request(&url, "{{{ not json", signature, Some("push"), Some("d-2"))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), 401, "signature {signature:?}");
            assert!(
                plane.calls.lock().unwrap().is_empty(),
                "no channel event constructed (plane never called) for {signature:?}"
            );
        }
    }

    /// Oversize bodies are 413 *before signature work*: the request
    /// carries no signature at all, so a signature-first order would
    /// answer 401 instead.
    #[tokio::test]
    async fn oversize_body_is_413_before_signature_work() {
        force_no_proxy();
        let plane = std::sync::Arc::new(FakePlane::default());
        let limits = WebhookLimits {
            max_body_bytes: 64,
            ..WebhookLimits::default()
        };
        let url = spawn_router(plane.clone(), limits).await;
        let huge = "x".repeat(256);
        // No signature: 413 proves the size gate precedes the HMAC.
        let response = request(&url, &huge, None, Some("push"), Some("d-3"))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 413);
        assert!(plane.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn non_json_content_type_is_415() {
        force_no_proxy();
        let plane = std::sync::Arc::new(FakePlane::default());
        let url = spawn_router(plane.clone(), WebhookLimits::default()).await;
        let body = serde_json::to_string(&delivery_body()).unwrap();
        let response = reqwest::Client::new()
            .post(&url)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-hub-signature-256", sign(body.as_bytes()))
            .body(body)
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 415);
        assert!(plane.calls.lock().unwrap().is_empty());
    }

    /// The rate limit trips with 429 and an audit line (captured
    /// through the shared global subscriber — no log files).
    #[tokio::test]
    async fn rate_limit_trips_with_429_and_an_audit_line() {
        force_no_proxy();
        let captured = logs();

        let plane = std::sync::Arc::new(FakePlane::default());
        let limits = WebhookLimits {
            rate_capacity: 2,
            rate_refill_per_second: 0,
            ..WebhookLimits::default()
        };
        let url = spawn_router(plane.clone(), limits).await;
        let body = serde_json::to_string(&delivery_body()).unwrap();
        let signature = sign(body.as_bytes());

        // Snapshot the buffer: only the NEW lines count (the global
        // subscriber serves the whole test binary).
        let start = captured.lock().unwrap().len();
        for delivery in ["d-a", "d-b"] {
            let response = request(&url, &body, Some(&signature), Some("push"), Some(delivery))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), 200, "delivery {delivery} admitted");
        }
        // The third take finds an empty bucket (no refill).
        let response = request(&url, &body, Some(&signature), Some("push"), Some("d-c"))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 429);
        let text = response
            .json::<serde_json::Value>()
            .await
            .expect("429 body json");
        assert!(
            text["error"]["message"]
                .as_str()
                .unwrap()
                .contains("rate limit")
        );

        // The audit line exists.
        let logged =
            String::from_utf8(captured.lock().unwrap()[start..].to_vec()).expect("utf8 log");
        assert!(
            logged.contains("rate limited") && logged.contains("webhook audit"),
            "expected a rate-limit audit line, got: {logged}"
        );
    }

    #[tokio::test]
    async fn missing_headers_or_invalid_json_are_400() {
        force_no_proxy();
        let plane = std::sync::Arc::new(FakePlane::default());
        let url = spawn_router(plane.clone(), WebhookLimits::default()).await;
        let body = serde_json::to_string(&delivery_body()).unwrap();
        let signature = sign(body.as_bytes());

        // Missing event header.
        let response = request(&url, &body, Some(&signature), None, Some("d-4"))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 400);

        // Missing delivery header.
        let response = request(&url, &body, Some(&signature), Some("push"), None)
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), 400);

        // Authenticated but invalid JSON.
        let garbage = "authenticated garbage";
        let response = request(
            &url,
            garbage,
            Some(&sign(garbage.as_bytes())),
            Some("push"),
            Some("d-5"),
        )
        .send()
        .await
        .expect("send");
        assert_eq!(response.status(), 400);
        assert!(plane.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn signature_verification_matrix() {
        let secret = SECRET.as_bytes();
        let body = br#"{"ok":true}"#;
        let good = sign(body);
        assert!(verify_signature(secret, body, &good));
        // Wrong secret / body / prefix / hex / truncation all fail.
        assert!(!verify_signature(b"other", body, &good));
        assert!(!verify_signature(secret, b"{}", &good));
        assert!(!verify_signature(secret, body, "md5=abcd"));
        assert!(!verify_signature(secret, body, "sha256=not-hex!"));
        assert!(!verify_signature(secret, body, "sha256=abcd"));
        // Uppercase hex still decodes (GitHub sends lowercase; the
        // decoder accepts both).
        let upper = format!(
            "sha256={}",
            good.strip_prefix("sha256=").unwrap().to_ascii_uppercase()
        );
        assert!(verify_signature(secret, body, &upper));
    }

    /// The bucket refills over time: after exhausting a no-refill
    /// bucket, backdating its timestamp lets exactly one more take
    /// through (the lazy-refill math is the unit under test).
    #[test]
    fn bucket_refills_with_elapsed_time() {
        force_no_proxy();
        let plane: std::sync::Arc<FakePlane> = std::sync::Arc::new(FakePlane::default());
        let state = super::AppState {
            plane,
            secret: std::sync::Arc::new(SECRET.as_bytes().to_vec()),
            limits: WebhookLimits {
                rate_capacity: 1,
                rate_refill_per_second: 10,
                ..WebhookLimits::default()
            },
            buckets: std::sync::Arc::new(Mutex::new(BTreeMap::new())),
        };
        assert!(super::take_token(&state, "src"));
        assert!(!super::take_token(&state, "src"), "capacity 1, empty now");
        // Backdate 200ms → 2 tokens refill, capped at capacity 1.
        {
            let mut buckets = state.buckets.lock().unwrap();
            let bucket = buckets.get_mut("src").expect("tracked source");
            bucket.updated_ms -= 200;
        }
        assert!(super::take_token(&state, "src"));
        assert!(!super::take_token(&state, "src"), "capped at capacity");
        // A different source has its own bucket.
        assert!(super::take_token(&state, "other"));
    }

    /// A capturing writer pair for the audit-line assertion (the
    /// repo's established MakeWriter pattern).
    struct CaptureWriter(std::sync::Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct MakeCapture(std::sync::Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeCapture {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(std::sync::Arc::clone(&self.0))
        }
    }
}
