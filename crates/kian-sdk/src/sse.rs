//! The `events` SSE helper: `GET {base}/sessions/{id}/events`.
//!
//! The stream frames exactly three event names (per the frozen
//! contract): `kian` (one session event, serialized JSON), `done` (the
//! session ended — the server freezes the stream), and `error` (the
//! read failed; data is the message). Keep-alive comment frames are
//! skipped.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use serde_json::Value;

use crate::error::ClientError;
use crate::transport::KianClient;

/// One parsed frame of the SSE event stream.
#[derive(Debug, Clone, PartialEq)]
pub enum SseFrame {
    /// `event: kian` — one session event (`data` is its JSON). Servers
    /// that do not set the SSE `id` field (pre-read-model) yield this
    /// variant.
    Kian(Value),
    /// `event: kian` with `id: <seq>` (READ-MODEL §4.1): one session
    /// event and its seq — the value to send back as `Last-Event-ID`
    /// on reconnect. Additive: a server always emitting ids yields
    /// only this variant; one never emitting ids yields only
    /// [`SseFrame::Kian`].
    KianWithId { id: u64, data: Value },
    /// `event: kian` with `id: <session>:<seq>` (READ-MODEL §4.2, the
    /// P3 fan-in stream): one event of one watched session, plus the
    /// session uuid and seq the compound id carries. The fan-in is
    /// tail-only — there is no resume; the split id exists for
    /// per-session ordering checks, not reconnection.
    KianFanIn {
        session: String,
        seq: u64,
        data: Value,
    },
    /// `event: done` — the terminal event was observed; the stream is
    /// complete.
    Done,
    /// `event: error` — the server failed the read; `data` is the
    /// message.
    Error(String),
}

impl KianClient {
    /// Opens the SSE event stream for a session from the beginning and
    /// yields parsed frames until the server closes the stream (after
    /// `done` or `error`). Transport failures mid-stream surface as
    /// [`ClientError::Http`] items; a frame with an unknown event name
    /// or unparseable `kian` data is [`ClientError::Malformed`] — never
    /// a silent drop.
    pub async fn events(
        &self,
        session: &str,
    ) -> Result<impl Stream<Item = Result<SseFrame, ClientError>> + Send + 'static, ClientError>
    {
        self.events_impl(session, None).await
    }

    /// [`KianClient::events`] resuming after `last_event_id` (READ-MODEL
    /// §4.1): sends `Last-Event-ID: <seq>`; the server emits only
    /// events with `seq > last_event_id` — frames before the resume
    /// point are never re-delivered. A resume id ahead of the durable
    /// head yields one [`SseFrame::Error`] and closes.
    pub async fn events_from(
        &self,
        session: &str,
        last_event_id: u64,
    ) -> Result<impl Stream<Item = Result<SseFrame, ClientError>> + Send + 'static, ClientError>
    {
        self.events_impl(session, Some(last_event_id)).await
    }

    /// Opens the fan-in event stream (READ-MODEL §4.2 / API-CONTRACT
    /// §5.4): `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]`.
    /// `sessions` is the explicit allowlist (the caller caps it at 64);
    /// `kinds` is an optional kind allowlist (`None` = every kind).
    /// Tail-only: the stream starts at each session's current head —
    /// no `Last-Event-ID` is sent and none is honored. A malformed or
    /// over-cap request surfaces as [`ClientError::Http`] (the
    /// server's HTTP 400).
    pub async fn fan_in(
        &self,
        sessions: &[&str],
        kinds: Option<&[&str]>,
    ) -> Result<impl Stream<Item = Result<SseFrame, ClientError>> + Send + 'static, ClientError>
    {
        let mut url = format!("{}/events?sessions={}", self.base, sessions.join(","));
        if let Some(kinds) = kinds
            && !kinds.is_empty()
        {
            url.push_str("&kinds=");
            url.push_str(&kinds.join(","));
        }
        let resp = self
            .http
            .get(&url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| ClientError::Http(format!("{url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Http(format!("{url}: status {status}: {text}")));
        }
        Ok(frames(resp))
    }

    async fn events_impl(
        &self,
        session: &str,
        last_event_id: Option<u64>,
    ) -> Result<impl Stream<Item = Result<SseFrame, ClientError>> + Send + 'static, ClientError>
    {
        let url = format!("{}/sessions/{session}/events", self.base);
        let mut request = self.http.get(&url).header("accept", "text/event-stream");
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id.to_string());
        }
        let resp = request
            .send()
            .await
            .map_err(|e| ClientError::Http(format!("{url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Http(format!("{url}: status {status}: {text}")));
        }
        Ok(frames(resp))
    }
}

/// Incremental SSE parser over the response byte stream: buffer
/// chunks, emit one item per `\n\n`-delimited frame.
fn frames(resp: reqwest::Response) -> SseFrames {
    // Map to owned Vec<u8> so the chunk type is nameable without a
    // direct `bytes` dependency (reqwest 0.12 does not re-export it).
    let chunks = resp
        .bytes_stream()
        .map(|chunk| chunk.map(|bytes| bytes.to_vec()));
    SseFrames {
        chunks: Box::pin(chunks),
        buf: Vec::new(),
    }
}

/// The boxed chunk stream of a reqwest response, as owned bytes.
type ByteChunks = Pin<Box<dyn Stream<Item = reqwest::Result<Vec<u8>>> + Send>>;

/// [`futures::Stream`] of parsed SSE frames; `buf` holds bytes read but
/// not yet terminated by a frame boundary.
struct SseFrames {
    chunks: ByteChunks,
    buf: Vec<u8>,
}

impl futures::Stream for SseFrames {
    type Item = Result<SseFrame, ClientError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(end) = frame_boundary(&this.buf) {
                let frame: Vec<u8> = this.buf.drain(..end).collect();
                match parse_frame(&frame) {
                    Ok(Some(frame)) => return std::task::Poll::Ready(Some(Ok(frame))),
                    Ok(None) => continue, // keep-alive / comment frame
                    Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                }
            }
            match futures::ready!(this.chunks.as_mut().poll_next(cx)) {
                Some(Ok(bytes)) => this.buf.extend_from_slice(&bytes),
                Some(Err(e)) => {
                    return std::task::Poll::Ready(Some(Err(ClientError::Http(format!(
                        "sse body: {e}"
                    )))));
                }
                // Server closed the stream; per that follows
                // `done`/`error` — the stream simply ends.
                None => return std::task::Poll::Ready(None),
            }
        }
    }
}

/// Byte offset just past the frame terminator (`\n\n` or `\r\n\r\n`),
/// whichever comes first.
fn frame_boundary(buf: &[u8]) -> Option<usize> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2);
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Parses one raw frame. `Ok(None)` = comment/keep-alive (no fields);
/// `Err` = a named frame the contract does not know, or `kian` data
/// that is not JSON. A `kian` frame carrying `id: <seq>` (READ-MODEL
/// §4.1) parses as [`SseFrame::KianWithId`]; a non-numeric `id` on a
/// `kian` frame is [`ClientError::Malformed`] (the id is contract
/// data, not decoration — never a silent drop).
fn parse_frame(raw: &[u8]) -> Result<Option<SseFrame>, ClientError> {
    let text = String::from_utf8_lossy(raw);
    let mut event: Option<String> = None;
    let mut data = String::new();
    let mut id: Option<String> = None;
    let mut saw_field = false;
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue; // blank separator / comment
        }
        let (name, value) = match line.split_once(':') {
            Some((n, v)) => (n, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match name {
            "event" => {
                event = Some(value.to_owned());
                saw_field = true;
            }
            "data" => {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value);
                saw_field = true;
            }
            // READ-MODEL §4.1: `id` on a `kian` frame is the seq
            // (carried into `KianWithId`); on `done`/`error` it is
            // not part of the contract and ignored. `retry:` and any
            // future field stay additive-safe.
            "id" => {
                id = Some(value.to_owned());
                saw_field = true;
            }
            _ => {}
        }
    }
    if !saw_field {
        return Ok(None);
    }
    let Some(event) = event else {
        // Data-only frame (default `message` event): not part of.
        return Ok(None);
    };
    match event.as_str() {
        "kian" => {
            let data: Value = serde_json::from_str(&data)
                .map_err(|e| ClientError::Malformed(format!("kian frame data is not JSON: {e}")))?;
            match id {
                None => Ok(Some(SseFrame::Kian(data))),
                Some(raw) => match raw.parse::<u64>() {
                    // READ-MODEL §4.1: the per-session seq id.
                    Ok(seq) => Ok(Some(SseFrame::KianWithId { id: seq, data })),
                    // READ-MODEL §4.2: the fan-in compound id
                    // `<session-uuid>:<seq>`.
                    Err(_) => match raw.rsplit_once(':') {
                        Some((session, seq)) => {
                            let seq = seq.parse::<u64>().map_err(|_| {
                                ClientError::Malformed(format!(
                                    "kian frame id `{raw}` is neither a numeric seq nor \
                                     a <session>:<seq> pair"
                                ))
                            })?;
                            if session.is_empty() {
                                return Err(ClientError::Malformed(format!(
                                    "kian frame id `{raw}` names no session"
                                )));
                            }
                            Ok(Some(SseFrame::KianFanIn {
                                session: session.to_owned(),
                                seq,
                                data,
                            }))
                        }
                        None => Err(ClientError::Malformed(format!(
                            "kian frame id `{raw}` is not a numeric seq"
                        ))),
                    },
                },
            }
        }
        "done" => Ok(Some(SseFrame::Done)),
        "error" => Ok(Some(SseFrame::Error(data))),
        other => Err(ClientError::Malformed(format!(
            "unknown sse event `{other}` (frozen set: kian, done, error)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{SseFrame, frame_boundary, parse_frame};

    /// Frame boundaries handle both newline conventions.
    #[test]
    fn frame_boundary_finds_both_terminators() {
        assert_eq!(frame_boundary(b"event: kian\ndata: {}\n\nrest"), Some(22));
        assert_eq!(frame_boundary(b"event: kian\r\ndata: {}\r\n\r\n"), Some(25));
        assert_eq!(frame_boundary(b"no terminator yet"), None);
    }

    /// Keep-alive comments are skipped; the three frames parse;
    /// unknown names and bad JSON are honest errors.
    #[test]
    fn parse_frame_handles_the_c13_set() {
        assert_eq!(parse_frame(b": keep-alive\n\n").ok(), Some(None));
        assert_eq!(parse_frame(b"\n\n").ok(), Some(None));

        let kian = parse_frame(b"event: kian\ndata: {\"seq\":1}\n\n").unwrap();
        assert!(matches!(kian, Some(SseFrame::Kian(_))));

        assert_eq!(
            parse_frame(b"event: done\ndata: ended\n\n").ok(),
            Some(Some(SseFrame::Done))
        );
        assert_eq!(
            parse_frame(b"event: error\ndata: unknown session\n\n").ok(),
            Some(Some(SseFrame::Error("unknown session".into())))
        );

        // `event:kian` without the space is legal SSE and accepted.
        let compact = parse_frame(b"event:kian\ndata:{\"seq\":2}\n\n").unwrap();
        assert!(matches!(compact, Some(SseFrame::Kian(_))));

        assert!(parse_frame(b"event: surprise\ndata: x\n\n").is_err());
        assert!(parse_frame(b"event: kian\ndata: not-json\n\n").is_err());

        // Data-only frame (default event) is not — skipped.
        assert_eq!(parse_frame(b"data: hello\n\n").ok(), Some(None));
    }

    /// READ-MODEL §4.1: a `kian` frame with `id: <seq>` parses as
    /// `KianWithId` (id + data); the id-less spelling still parses as
    /// `Kian` (old servers, old fixtures); a non-numeric id on a kian
    /// frame is an honest Malformed error; `done` with an id stays
    /// `Done`.
    #[test]
    fn parse_frame_carries_kian_ids_additively() {
        assert_eq!(
            parse_frame(b"event: kian\nid: 42\ndata: {\"seq\":42}\n\n").unwrap(),
            Some(SseFrame::KianWithId {
                id: 42,
                data: serde_json::json!({"seq": 42})
            })
        );
        // `id:42` without the space is legal SSE.
        assert_eq!(
            parse_frame(b"event: kian\nid:42\ndata: {\"seq\":42}\n\n").unwrap(),
            Some(SseFrame::KianWithId {
                id: 42,
                data: serde_json::json!({"seq": 42})
            })
        );
        // Old servers / old fixtures: no id line.
        assert!(matches!(
            parse_frame(b"event: kian\ndata: {\"seq\":1}\n\n").unwrap(),
            Some(SseFrame::Kian(_))
        ));
        // A non-numeric id is contract data gone wrong — never silent.
        assert!(parse_frame(b"event: kian\nid: abc\ndata: {\"seq\":1}\n\n").is_err());
        // `done`/`error` ignore an id (not part of their contract).
        assert_eq!(
            parse_frame(b"id: 9\nevent: done\ndata: ended\n\n").ok(),
            Some(Some(SseFrame::Done))
        );
    }

    /// READ-MODEL §4.2 (P3): a `kian` frame with the compound fan-in
    /// id `<session-uuid>:<seq>` parses as `KianFanIn` (carrying the
    /// session, the seq and the data); the plain numeric id still
    /// parses as `KianWithId`; an id that is neither shape stays an
    /// honest Malformed error.
    #[test]
    fn parse_frame_splits_fan_in_ids() {
        let sid = "00000000-0000-0000-0000-000000000042";
        let raw = format!("event: kian\nid: {sid}:7\ndata: {{\"seq\":7}}\n\n");
        assert_eq!(
            parse_frame(raw.as_bytes()).unwrap(),
            Some(SseFrame::KianFanIn {
                session: sid.to_string(),
                seq: 7,
                data: serde_json::json!({"seq": 7})
            })
        );
        // No space after the colon is legal SSE.
        let raw = format!("event: kian\nid:{sid}:7\ndata: {{\"seq\":7}}\n\n");
        assert!(matches!(
            parse_frame(raw.as_bytes()).unwrap(),
            Some(SseFrame::KianFanIn { seq: 7, .. })
        ));
        // Neither shape: no colon at all.
        assert!(parse_frame(b"event: kian\nid: abc\ndata: {\"seq\":1}\n\n").is_err());
        // Compound shape with a non-numeric seq.
        assert!(
            parse_frame(
                b"event: kian\nid: 00000000-0000-0000-0000-000000000001:x\ndata: {\"seq\":1}\n\n"
            )
            .is_err()
        );
        // Compound shape naming no session.
        assert!(parse_frame(b"event: kian\nid: :7\ndata: {\"seq\":7}\n\n").is_err());
    }
}
