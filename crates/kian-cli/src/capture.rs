//! The loopback request-capture proxy (HARNESS-BRIDGE §5.4, [ADR-046](design/decisions/ADR-046-harness-request-capture.md)).
//!
//! Claude Code (`ANTHROPIC_BASE_URL`) and Codex (`model_providers.<id>.base_url`) can be pointed at
//! this listener. It records each request body **before** forwarding it to the configured upstream,
//! then streams the upstream response back verbatim. Recording is fail-closed: an oversized body or
//! a missing/closed sink answers `502` **without forwarding**, so an unrecorded model-visible input
//! is never sent (A2). Auth headers are forwarded but never persisted or logged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use kian_core::model::SessionId;
use kian_harness::{CaptureSource, CaptureStream};
use tokio::sync::mpsc;

use crate::config::CaptureSettings;

/// The one capture listener per server.
pub struct CaptureHub {
    base: String,
    max_body_bytes: usize,
    sinks: Mutex<HashMap<SessionId, mpsc::Sender<Vec<u8>>>>,
    client: reqwest::Client,
    anthropic_upstream: String,
    openai_upstream: String,
}

impl CaptureHub {
    /// Binds a loopback listener and serves it for the process lifetime.
    pub async fn start(settings: CaptureSettings) -> Result<Arc<Self>, String> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|e| format!("capture proxy bind: {e}"))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("capture proxy addr: {e}"))?;
        let hub = Arc::new(Self {
            base: format!("http://{addr}"),
            max_body_bytes: settings.max_body_bytes,
            sinks: Mutex::new(HashMap::new()),
            client: reqwest::Client::new(),
            anthropic_upstream: settings.anthropic_upstream,
            openai_upstream: settings.openai_upstream,
        });
        let app = Router::new()
            .route("/{session}/{*rest}", any(handle))
            .layer(DefaultBodyLimit::max(settings.max_body_bytes))
            .with_state(hub.clone());
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!(%e, "capture proxy stopped");
            }
        });
        Ok(hub)
    }

    /// The loopback base URL (`http://127.0.0.1:<port>`); adapters append `/<session>`.
    #[must_use]
    pub fn base_url(&self) -> String {
        self.base.clone()
    }

    fn upstream_for(&self, rest: &str) -> Option<&str> {
        if rest.starts_with("v1/messages") {
            Some(&self.anthropic_upstream)
        } else if rest.starts_with("v1/responses") {
            Some(&self.openai_upstream)
        } else {
            None
        }
    }
}

impl CaptureSource for CaptureHub {
    fn open(&self, session: SessionId) -> Option<Box<dyn CaptureStream>> {
        let (tx, rx) = mpsc::channel(64);
        self.sinks
            .lock()
            .expect("capture sinks")
            .insert(session, tx);
        Some(Box::new(HubStream { rx }))
    }
}

struct HubStream {
    rx: mpsc::Receiver<Vec<u8>>,
}

impl CaptureStream for HubStream {
    fn drain(&mut self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while let Ok(body) = self.rx.try_recv() {
            out.push(body);
        }
        out
    }
}

fn refuse(reason: &str) -> Response {
    tracing::warn!(target: "kian::capture", reason, "capture proxy refused the request");
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"error":{{"message":"{reason}"}}}}"#
        )))
        .expect("refusal response")
}

async fn handle(
    State(hub): State<Arc<CaptureHub>>,
    Path((session, rest)): Path<(String, String)>,
    uri: Uri,
    method: axum::http::Method,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&session) else {
        return refuse("capture: malformed session path");
    };
    let session = SessionId(uuid);
    let Some(upstream) = hub.upstream_for(&rest) else {
        return refuse("capture: unknown provider route");
    };
    if body.len() > hub.max_body_bytes {
        return refuse("capture: request body exceeds max_body_bytes");
    }
    let sink = hub
        .sinks
        .lock()
        .expect("capture sinks")
        .get(&session)
        .cloned();
    let Some(sink) = sink else {
        return refuse("capture: no open capture stream for the session");
    };
    // Record before forwarding; a full/closed queue fails closed.
    if sink.try_send(body.to_vec()).is_err() {
        return refuse("capture: capture queue unavailable");
    }

    let mut url = format!("{upstream}/{rest}");
    if let Some(query) = uri.query() {
        url.push('?');
        url.push_str(query);
    }
    let mut request = hub.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::POST),
        &url,
    );
    for (name, value) in headers.iter() {
        let lowered = name.as_str();
        if matches!(
            lowered,
            "host" | "content-length" | "connection" | "transfer-encoding"
        ) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            request = request.header(name, value);
        }
    }
    let upstream_response = match request.body(body.to_vec()).send().await {
        Ok(response) => response,
        Err(e) => {
            tracing::warn!(target: "kian::capture", %e, "capture proxy upstream failed");
            return refuse("capture: upstream unavailable");
        }
    };
    tracing::debug!(
        target: "kian::capture",
        status = %upstream_response.status(),
        "capture proxy upstream answered"
    );

    let mut response = Response::builder().status(
        StatusCode::from_u16(upstream_response.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY),
    );
    for (name, value) in upstream_response.headers().iter() {
        if matches!(
            name.as_str(),
            "content-length" | "transfer-encoding" | "connection"
        ) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            axum::http::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            axum::http::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(upstream_response.bytes_stream()))
        .unwrap_or_else(|_| refuse("capture: response build failed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type StubState = (Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>);

    async fn stub_upstream() -> (String, Arc<Mutex<Vec<Vec<u8>>>>, Arc<AtomicUsize>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let count = Arc::new(AtomicUsize::new(0));
        let state = (seen.clone(), count.clone());
        let app = Router::new()
            .route(
                "/{*rest}",
                any(
                    |State((seen, count)): State<StubState>, body: axum::body::Bytes| async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        seen.lock().unwrap().push(body.to_vec());
                        "upstream-ok"
                    },
                ),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen, count)
    }

    fn settings(upstream: String, max: usize) -> CaptureSettings {
        CaptureSettings {
            max_body_bytes: max,
            anthropic_upstream: upstream.clone(),
            openai_upstream: upstream,
        }
    }

    #[tokio::test]
    async fn a_request_is_recorded_before_it_is_forwarded_and_the_response_passes_through() {
        let (upstream, seen, _count) = stub_upstream().await;
        let hub = CaptureHub::start(settings(upstream, 4096))
            .await
            .expect("hub");
        let session = SessionId::new();
        let mut stream = hub.open(session).expect("capture stream");
        let body = br#"{"model":"claude","messages":[{"role":"user","content":"hi"}]}"#;
        let response = reqwest::Client::new()
            .post(format!(
                "{}/{}/v1/messages?beta=true",
                hub.base_url(),
                session.0
            ))
            .body(body.to_vec())
            .send()
            .await
            .expect("proxy call");
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.expect("body"), "upstream-ok");
        assert_eq!(stream.drain(), vec![body.to_vec()]);
        assert_eq!(seen.lock().unwrap().as_slice(), &[body.to_vec()]);
    }

    #[tokio::test]
    async fn an_oversized_body_is_refused_without_forwarding() {
        let (upstream, _seen, count) = stub_upstream().await;
        let hub = CaptureHub::start(settings(upstream, 64))
            .await
            .expect("hub");
        let session = SessionId::new();
        let mut stream = hub.open(session).expect("capture stream");
        let response = reqwest::Client::new()
            .post(format!("{}/{}/v1/messages", hub.base_url(), session.0))
            .body(vec![b'x'; 256])
            .send()
            .await
            .expect("proxy call");
        // The axum body limit answers 413 before the handler; either way it is fail-closed.
        assert!(
            response.status() == 502 || response.status().is_client_error(),
            "oversize must be refused: {}",
            response.status()
        );
        assert!(stream.drain().is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 0, "never forwarded");
    }

    #[tokio::test]
    async fn a_session_without_an_open_stream_is_refused() {
        let (upstream, _seen, count) = stub_upstream().await;
        let hub = CaptureHub::start(settings(upstream, 4096))
            .await
            .expect("hub");
        let session = SessionId::new();
        let response = reqwest::Client::new()
            .post(format!("{}/{}/v1/messages", hub.base_url(), session.0))
            .body(b"{}".to_vec())
            .send()
            .await
            .expect("proxy call");
        assert_eq!(response.status(), 502);
        assert_eq!(count.load(Ordering::SeqCst), 0, "never forwarded");
    }
}
