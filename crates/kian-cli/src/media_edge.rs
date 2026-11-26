//! Channel-edge media processing (ADR-062, P5b).
//!
//! The assembly channel pump runs one optional edge before
//! `SubscriptionService::handle_event`. An adapter event whose payload
//! carries the additive `media` envelope has each item written into the
//! content-addressed artifact store, transcribed by the ADR-061 tools and
//! rewritten to an artifact reference plus transcript; the payload text
//! gains the transcript under a fixed marker. Base64 never re-enters the
//! payload and no message is dropped on a media failure.

use std::sync::Arc;

use kian_core::seams::{ChannelEvent, ToolCall, ToolProvider};
use kian_media::{DEFAULT_AUDIO_MEDIA_TYPE, DEFAULT_IMAGE_MEDIA_TYPE, MediaTools, base64_decode};
use kian_store::{ArtifactRef, ArtifactStore};
use serde_json::{Map, Value, json};

/// The additive payload key that marks a media event (ADR-062).
pub const MEDIA_ENVELOPE_KEY: &str = "media";
/// Marker prefixed to an image transcript in the payload text.
pub const IMAGE_TRANSCRIPT_MARKER: &str = "[image transcript]";
/// Marker prefixed to an audio transcript in the payload text.
pub const VOICE_TRANSCRIPT_MARKER: &str = "[voice message transcript]";

const KIND_IMAGE: &str = "image";
const KIND_AUDIO: &str = "audio";

/// The channel-edge media processor: adapter bytes in, artifact
/// references and transcripts out.
#[derive(Clone)]
pub struct MediaEdge {
    tools: Arc<MediaTools>,
    store: Arc<ArtifactStore>,
}

impl MediaEdge {
    pub fn new(tools: Arc<MediaTools>, store: Arc<ArtifactStore>) -> Self {
        Self { tools, store }
    }

    /// ADR-066: the outbound voice edge shares this assembly's tools and
    /// store, so a channel gains `reply_voice` only where inbound media
    /// already has an edge.
    pub fn tools(&self) -> &Arc<MediaTools> {
        &self.tools
    }

    /// ADR-066: the artifact store the voice edge reads a synthesized
    /// artifact back from.
    pub fn store(&self) -> &Arc<ArtifactStore> {
        &self.store
    }

    /// Processes one inbound event. A payload without a `media` key is
    /// returned byte-identically; every media failure is an honest
    /// per-item `error` and never blocks the event.
    pub async fn prepare(&self, mut event: ChannelEvent) -> ChannelEvent {
        let Some(payload) = event.payload.as_object().cloned() else {
            return event;
        };
        if !payload.contains_key(MEDIA_ENVELOPE_KEY) {
            return event;
        }
        let items = match payload.get(MEDIA_ENVELOPE_KEY) {
            Some(Value::Array(items)) => items.clone(),
            Some(_) => {
                let mut object = payload;
                object.insert(MEDIA_ENVELOPE_KEY.to_string(), json!([]));
                append_text(
                    &mut object,
                    "[media transcript unavailable] media must be an array",
                );
                event.payload = Value::Object(object);
                return event;
            }
            None => return event,
        };
        let mut rewritten = Vec::with_capacity(items.len());
        let mut transcript_lines = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let (rewritten_item, line) = self.process_item(&event.event_id, index, item).await;
            if let Some(line) = line {
                transcript_lines.push(line);
            }
            rewritten.push(rewritten_item);
        }
        let mut object = payload;
        object.insert(MEDIA_ENVELOPE_KEY.to_string(), Value::Array(rewritten));
        for line in &transcript_lines {
            append_text(&mut object, line);
        }
        event.payload = Value::Object(object);
        event
    }

    async fn process_item(
        &self,
        event_id: &str,
        index: usize,
        item: &Value,
    ) -> (Value, Option<String>) {
        let Some(object) = item.as_object() else {
            return (json!({"error": "media item must be an object"}), None);
        };
        let kind = object.get("kind").and_then(Value::as_str).unwrap_or("");
        let (tool, marker, default_media_type) = match kind {
            KIND_IMAGE => (
                "image_ocr",
                IMAGE_TRANSCRIPT_MARKER,
                DEFAULT_IMAGE_MEDIA_TYPE,
            ),
            KIND_AUDIO => (
                "audio_transcribe",
                VOICE_TRANSCRIPT_MARKER,
                DEFAULT_AUDIO_MEDIA_TYPE,
            ),
            _ => {
                return (
                    json!({
                        "kind": kind,
                        "error": format!("unsupported media kind: {kind}"),
                    }),
                    None,
                );
            }
        };
        let media_type = object
            .get("media_type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(encoded) = object.get("base64").and_then(Value::as_str) else {
            return (
                error_item(kind, media_type, None, "media item is missing base64 bytes"),
                None,
            );
        };
        let Some(bytes) = base64_decode(encoded) else {
            return (
                error_item(kind, media_type, None, "media item base64 is malformed"),
                None,
            );
        };
        if bytes.is_empty() {
            return (
                error_item(kind, media_type, None, "media item carries no bytes"),
                None,
            );
        }
        let reference = match self
            .store
            .put(&bytes, media_type.unwrap_or(default_media_type))
        {
            Ok(reference) => reference,
            Err(error) => {
                return (
                    error_item(
                        kind,
                        media_type,
                        None,
                        &format!("could not store the media artifact: {error}"),
                    ),
                    None,
                );
            }
        };
        let mut arguments = json!({"artifact": reference.id});
        if let Some(media_type) = media_type {
            arguments["media_type"] = json!(media_type);
        }
        if kind == KIND_AUDIO
            && let Some(language) = object
                .get("language")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        {
            arguments["language"] = json!(language);
        }
        let call = ToolCall {
            id: format!("media-edge:{event_id}:{index}"),
            name: tool.to_string(),
            arguments,
        };
        let result = match self.tools.invoke(call).await {
            Ok(result) => result,
            Err(error) => {
                return (
                    error_item(
                        kind,
                        media_type,
                        Some(&reference),
                        &format!("{tool} refused the item: {error}"),
                    ),
                    None,
                );
            }
        };
        if result.is_error {
            let message = result
                .output
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("media tool failed");
            return (
                error_item(kind, media_type, Some(&reference), message),
                None,
            );
        }
        let Some(transcript) = result.output.get("text").and_then(Value::as_str) else {
            return (
                error_item(
                    kind,
                    media_type,
                    Some(&reference),
                    &format!("{tool} returned no transcript"),
                ),
                None,
            );
        };
        (
            json!({
                "kind": kind,
                "media_type": reference.media_type,
                "artifact": reference.id,
                "size": reference.size,
                "transcript": transcript,
            }),
            Some(format!("{marker} {transcript}")),
        )
    }
}

/// The failed-item view. An item whose bytes were already stored keeps
/// its reference (ADR-065): a picture a vision target can read is worth
/// admitting even when the text tool could not describe it. A failure
/// before the store has no reference to give.
fn error_item(
    kind: &str,
    media_type: Option<&str>,
    stored: Option<&ArtifactRef>,
    message: &str,
) -> Value {
    let mut item = json!({"kind": kind, "error": message});
    match stored {
        Some(reference) => {
            item["artifact"] = json!(reference.id);
            item["media_type"] = json!(reference.media_type);
            item["size"] = json!(reference.size);
        }
        None => {
            if let Some(media_type) = media_type {
                item["media_type"] = json!(media_type);
            }
        }
    }
    item
}

fn append_text(object: &mut Map<String, Value>, line: &str) {
    match object.get_mut("text") {
        Some(Value::String(text)) => {
            if text.is_empty() {
                *text = line.to_string();
            } else {
                text.push('\n');
                text.push_str(line);
            }
        }
        Some(_) => {}
        None => {
            object.insert("text".to_string(), Value::String(line.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_media::MediaToolsConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn temp_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kian-media-edge-{}", uuid::Uuid::new_v4()))
    }

    fn tools(server: &MockServer, store: &Arc<ArtifactStore>) -> Arc<MediaTools> {
        Arc::new(
            MediaTools::new(
                MediaToolsConfig {
                    base_url: server.uri(),
                    api_key: "test-key".to_string(),
                    timeout_ms: 5_000,
                    max_input_bytes: 1_048_576,
                    default_voice: None,
                    default_format: "mp3".to_string(),
                },
                Arc::clone(store),
            )
            .expect("media tools"),
        )
    }

    fn event(payload: Value) -> ChannelEvent {
        ChannelEvent {
            channel: "mock-im".to_string(),
            event_id: "evt-1".to_string(),
            topic: None,
            sender: None,
            group: None,
            payload,
            ts_ms: 1_761_900_000_000,
        }
    }

    async fn edge(server: &MockServer) -> (MediaEdge, Arc<ArtifactStore>) {
        let store = Arc::new(ArtifactStore::open(temp_dir()).expect("store"));
        (
            MediaEdge::new(tools(server, &store), Arc::clone(&store)),
            store,
        )
    }

    #[tokio::test]
    async fn media_items_become_artifact_references_and_transcripts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tools/ocr"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"text": "Total 42"})))
            .mount(&server)
            .await;
        let (edge, store) = edge(&server).await;
        let prepared = edge
            .prepare(event(json!({
                "text": "what is on this invoice?",
                "media": [{"kind": "image", "media_type": "image/png", "base64": "aGk="}],
            })))
            .await;
        let payload = prepared.payload.as_object().expect("payload object");
        assert_eq!(
            payload["text"],
            json!("what is on this invoice?\n[image transcript] Total 42")
        );
        let item = &payload["media"][0];
        assert_eq!(item["kind"], json!("image"));
        assert_eq!(item["media_type"], json!("image/png"));
        assert_eq!(item["size"], json!(2));
        assert_eq!(item["transcript"], json!("Total 42"));
        let id = item["artifact"].as_str().expect("artifact id");
        assert_eq!(store.get(id).expect("store get").expect("blob"), b"hi");
        assert!(
            !prepared.payload.to_string().contains("aGk="),
            "base64 must not survive the edge"
        );
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body["image"]["base64"], json!("aGk="));
        assert_eq!(body["image"]["media_type"], json!("image/png"));
    }

    #[tokio::test]
    async fn an_audio_item_carries_its_language_and_gets_the_voice_marker() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tools/transcribe"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"text": "buy milk"})))
            .mount(&server)
            .await;
        let (edge, store) = edge(&server).await;
        let prepared = edge
            .prepare(event(json!({
                "media": [{
                    "kind": "audio",
                    "media_type": "audio/ogg",
                    "language": "zh",
                    "base64": "aGk="
                }],
            })))
            .await;
        let payload = prepared.payload.as_object().expect("payload object");
        assert_eq!(
            payload["text"],
            json!("[voice message transcript] buy milk")
        );
        let id = payload["media"][0]["artifact"]
            .as_str()
            .expect("artifact id");
        assert!(store.get(id).expect("store get").is_some());
        let requests = server.received_requests().await.expect("requests");
        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body["audio"]["base64"], json!("aGk="));
        assert_eq!(body["audio"]["media_type"], json!("audio/ogg"));
        assert_eq!(body["language"], json!("zh"));
    }

    #[tokio::test]
    async fn a_provider_failure_becomes_an_honest_error_item_and_the_event_proceeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tools/ocr"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        let (edge, store) = edge(&server).await;
        let prepared = edge
            .prepare(event(json!({
                "text": "look at this",
                "media": [{"kind": "image", "base64": "aGk="}],
            })))
            .await;
        let payload = prepared.payload.as_object().expect("payload object");
        assert_eq!(payload["text"], json!("look at this"));
        let item = &payload["media"][0];
        let error = item["error"].as_str().expect("error item");
        assert!(error.contains("HTTP 500"), "unexpected error: {error}");
        // ADR-065: the bytes were stored before the tool ran, so the
        // reference survives a failed transcription — a vision target can
        // still read the picture, and only the text is missing.
        let id = item["artifact"]
            .as_str()
            .expect("the stored reference survives");
        assert_eq!(item["media_type"], json!("image/png"));
        assert_eq!(item["size"], json!(2));
        assert_eq!(store.get(id).expect("store get").expect("blob"), b"hi");
        assert!(!prepared.payload.to_string().contains("aGk="));
    }

    #[tokio::test]
    async fn a_store_failure_has_no_reference_to_offer() {
        let server = MockServer::start().await;
        let dir = temp_dir();
        let store = Arc::new(ArtifactStore::open(dir.clone()).expect("store"));
        let edge = MediaEdge::new(tools(&server, &store), Arc::clone(&store));
        // A store that can no longer write: the root is now a file.
        std::fs::remove_dir_all(&dir).expect("remove the root");
        std::fs::write(&dir, b"in the way").expect("file in the way");
        let prepared = edge
            .prepare(event(json!({
                "media": [{"kind": "image", "base64": "aGk="}],
            })))
            .await;
        let payload = prepared.payload.as_object().expect("payload object");
        let item = &payload["media"][0];
        let error = item["error"].as_str().expect("error item");
        assert!(error.contains("could not store"), "{error}");
        assert!(item.get("artifact").is_none(), "{item}");
        assert!(item.get("size").is_none(), "{item}");
        std::fs::remove_file(&dir).ok();
    }

    #[tokio::test]
    async fn an_undecodable_item_is_reported_not_dropped() {
        let server = MockServer::start().await;
        let (edge, _store) = edge(&server).await;
        let prepared = edge
            .prepare(event(json!({
                "text": "voice note follows",
                "media": [{"kind": "audio", "media_type": "audio/ogg", "base64": "!!!!"}],
            })))
            .await;
        let payload = prepared.payload.as_object().expect("payload object");
        assert_eq!(payload["text"], json!("voice note follows"));
        let item = &payload["media"][0];
        assert_eq!(item["kind"], json!("audio"));
        assert_eq!(item["media_type"], json!("audio/ogg"));
        assert!(
            item["error"]
                .as_str()
                .expect("error item")
                .contains("malformed")
        );
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_unsupported_kind_is_reported_and_its_bytes_dropped() {
        let server = MockServer::start().await;
        let (edge, _store) = edge(&server).await;
        let prepared = edge
            .prepare(event(json!({
                "media": [{"kind": "video", "media_type": "video/mp4", "base64": "aGk="}],
            })))
            .await;
        let item = &prepared.payload["media"][0];
        assert!(
            item["error"]
                .as_str()
                .expect("error item")
                .contains("unsupported media kind")
        );
        assert!(!prepared.payload.to_string().contains("aGk="));
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_payload_without_media_is_byte_identical() {
        let server = MockServer::start().await;
        let (edge, _store) = edge(&server).await;
        let original = event(json!({"text": "plain message", "create_time_ms": 12}));
        let prepared = edge.prepare(original.clone()).await;
        assert_eq!(prepared, original);
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
        let opaque = event(json!(["not", "an", "object"]));
        assert_eq!(edge.prepare(opaque.clone()).await, opaque);
    }

    #[tokio::test]
    async fn a_malformed_envelope_is_reported_not_forwarded() {
        let server = MockServer::start().await;
        let (edge, _store) = edge(&server).await;
        let prepared = edge.prepare(event(json!({"media": "nope"}))).await;
        assert_eq!(prepared.payload["media"], json!([]));
        let text = prepared.payload["text"].as_str().expect("marker text");
        assert!(text.contains("media must be an array"));
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }
}
