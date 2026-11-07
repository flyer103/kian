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
    /// `event: kian` — one session event (`data` is its JSON).
    Kian(Value),
    /// `event: done` — the terminal event was observed; the stream is
    /// complete.
    Done,
    /// `event: error` — the server failed the read; `data` is the
    /// message.
    Error(String),
}

impl KianClient {
    /// Opens the SSE event stream for a session and yields parsed
    /// frames until the server closes the stream (after `done` or
    /// `error`). Transport failures mid-stream surface as
    /// [`ClientError::Http`] items; a frame with an unknown event name
    /// or unparseable `kian` data is [`ClientError::Malformed`] — never
    /// a silent drop.
    pub async fn events(
        &self,
        session: &str,
    ) -> Result<impl Stream<Item = Result<SseFrame, ClientError>> + Send + 'static, ClientError>
    {
        let url = format!("{}/sessions/{session}/events", self.base);
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
/// that is not JSON.
fn parse_frame(raw: &[u8]) -> Result<Option<SseFrame>, ClientError> {
    let text = String::from_utf8_lossy(raw);
    let mut event: Option<String> = None;
    let mut data = String::new();
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
            // `id:` / `retry:` and any future field are not part of the
            // contract; ignoring them is additive-safe.
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
        "kian" => serde_json::from_str(&data)
            .map(SseFrame::Kian)
            .map(Some)
            .map_err(|e| ClientError::Malformed(format!("kian frame data is not JSON: {e}"))),
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
}
