//! The outbound voice edge (ADR-066).
//!
//! A channel whose config says `voice: true` is registered behind this
//! decorator. The Router's `reply_voice` emits an *intent* — a
//! `ChannelDelivery` whose content is `{"voice": {"text": …}}` — because
//! the bundle owns no provider and no artifact store. The edge turns that
//! intent into bytes exactly once per send: it synthesizes speech through
//! the ADR-061 tool, reads the artifact back, and rewrites the content to
//! `{"voice": {"text", "base64", "media_type", "audio"}}` before handing
//! the delivery to the adapter. Every other delivery passes through
//! byte-identically, so a channel gains voice without changing what it
//! already sends.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{
    ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt, ToolCall,
    ToolProvider,
};
use kian_media::{MediaTools, base64_encode};
use kian_store::ArtifactStore;
use serde_json::{Value, json};

/// The bridge's outbound voice cap (CHANNEL-IMESSAGE §3.2). Enforced here
/// too, so an over-size payload never crosses the channel boundary: the
/// adapter would otherwise learn about it as a 413 after the round trip.
pub const MAX_VOICE_BYTES: usize = 1024 * 1024;

/// The speech-synthesis tool name (ADR-061).
const SPEECH_TOOL: &str = "speech_synthesize";

/// The channel-edge voice decorator: text intent in, audio bytes out.
#[derive(Clone)]
pub struct VoiceEdge {
    inner: Arc<dyn ChannelAdapter>,
    tools: Arc<MediaTools>,
    store: Arc<ArtifactStore>,
}

impl VoiceEdge {
    /// Wraps `inner` so a voice intent on it arrives as bytes. The caller
    /// has already proven the assembly has both a `media:` edge and an
    /// artifact store.
    pub fn wrap(
        inner: Arc<dyn ChannelAdapter>,
        tools: Arc<MediaTools>,
        store: Arc<ArtifactStore>,
    ) -> Arc<dyn ChannelAdapter> {
        Arc::new(Self {
            inner,
            tools,
            store,
        })
    }

    /// Rewrites one delivery: the intent's text becomes the synthesized
    /// artifact's bytes. A delivery without the `voice` key is returned
    /// untouched.
    async fn prepare(
        &self,
        mut delivery: ChannelDelivery,
    ) -> Result<ChannelDelivery, ChannelError> {
        let Some(voice) = delivery.content.get("voice") else {
            return Ok(delivery);
        };
        let object = voice.as_object().ok_or_else(|| {
            ChannelError::DeliveryFailed(
                "voice delivery content must be {\"voice\": {\"text\": …}}".into(),
            )
        })?;
        let text = object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if text.trim().is_empty() {
            return Err(ChannelError::DeliveryFailed(
                "voice delivery carries no text to speak".into(),
            ));
        }
        let audio = object.get("audio").and_then(Value::as_bool).unwrap_or(true);
        let artifact = self.synthesize(&text).await?;
        let bytes = self
            .store
            .get(&artifact.id)
            .map_err(|e| {
                ChannelError::DeliveryFailed(format!(
                    "voice artifact `{}` is unreadable: {e}",
                    artifact.id
                ))
            })?
            .ok_or_else(|| {
                ChannelError::DeliveryFailed(format!(
                    "voice artifact `{}` is no longer available (the store aged it out)",
                    artifact.id
                ))
            })?;
        if bytes.len() > MAX_VOICE_BYTES {
            return Err(ChannelError::DeliveryFailed(format!(
                "voice artifact `{}` is {} bytes, over the {} byte channel cap",
                artifact.id,
                bytes.len(),
                MAX_VOICE_BYTES
            )));
        }
        delivery.content = json!({
            "voice": {
                "text": text,
                "base64": base64_encode(&bytes),
                "media_type": artifact.media_type,
                "audio": audio,
            }
        });
        Ok(delivery)
    }

    /// One synthesis through the ADR-061 tool. A tool-level failure is a
    /// delivery failure carrying the tool's own honest output text.
    async fn synthesize(&self, text: &str) -> Result<Artifact, ChannelError> {
        let call = ToolCall {
            id: format!("voice-edge:{}", uuid::Uuid::new_v4()),
            name: SPEECH_TOOL.to_string(),
            arguments: json!({"text": text}),
        };
        let result = self.tools.invoke(call).await.map_err(|e| {
            ChannelError::DeliveryFailed(format!("{SPEECH_TOOL} refused the voice reply: {e}"))
        })?;
        if result.is_error {
            return Err(ChannelError::DeliveryFailed(format!(
                "voice synthesis failed: {}",
                result.output
            )));
        }
        let artifact = result.output.get("artifact").ok_or_else(|| {
            ChannelError::DeliveryFailed("voice synthesis returned no artifact reference".into())
        })?;
        let id = artifact
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ChannelError::DeliveryFailed("voice artifact reference has no id".into())
            })?
            .to_string();
        let media_type = artifact
            .get("media_type")
            .and_then(Value::as_str)
            .unwrap_or(kian_media::DEFAULT_AUDIO_MEDIA_TYPE)
            .to_string();
        Ok(Artifact { id, media_type })
    }
}

/// The part of a synthesized artifact this edge needs.
struct Artifact {
    id: String,
    media_type: String,
}

impl ChannelAdapter for VoiceEdge {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        self.inner.subscribe()
    }

    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let this = self.clone();
        Box::pin(async move {
            let delivery = this.prepare(out).await?;
            this.inner.send(delivery).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures::stream;
    use kian_media::MediaToolsConfig;
    use std::sync::Mutex;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Records what the adapter was handed and answers every send.
    struct Recorder {
        sent: Mutex<Vec<ChannelDelivery>>,
    }

    impl ChannelAdapter for Recorder {
        fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
            stream::empty().boxed()
        }

        fn send(
            &self,
            out: ChannelDelivery,
        ) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
            self.sent.lock().expect("sent").push(out);
            Box::pin(async move {
                Ok(SendReceipt {
                    message_id: "mid-1".to_string(),
                    ts_ms: 1_761_900_000_000,
                })
            })
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kian-voice-edge-{}", uuid::Uuid::new_v4()))
    }

    fn edge(server: &MockServer) -> (Arc<dyn ChannelAdapter>, Arc<Recorder>, Arc<ArtifactStore>) {
        let store = Arc::new(ArtifactStore::open(temp_dir()).expect("store"));
        let tools = Arc::new(
            MediaTools::new(
                MediaToolsConfig {
                    base_url: server.uri(),
                    api_key: "test-key".to_string(),
                    timeout_ms: 5_000,
                    max_input_bytes: 1_048_576,
                    default_voice: None,
                    default_format: "mp3".to_string(),
                },
                Arc::clone(&store),
            )
            .expect("media tools"),
        );
        let recorder = Arc::new(Recorder {
            sent: Mutex::new(Vec::new()),
        });
        (
            VoiceEdge::wrap(
                Arc::clone(&recorder) as Arc<dyn ChannelAdapter>,
                tools,
                Arc::clone(&store),
            ),
            recorder,
            store,
        )
    }

    fn delivery(content: Value) -> ChannelDelivery {
        ChannelDelivery {
            channel: "imessage".to_string(),
            target: json!({"handle": "+15550000001"}),
            content,
        }
    }

    fn synthesis_ok(audio: &[u8]) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "audio": {"base64": base64_encode(audio), "media_type": "audio/mpeg"}
        }))
    }

    #[tokio::test]
    async fn a_voice_intent_arrives_as_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tools/synthesize"))
            .respond_with(synthesis_ok(b"ID3-audio"))
            .expect(1)
            .mount(&server)
            .await;
        let (edge, recorder, _store) = edge(&server);
        let receipt = edge
            .send(delivery(json!({"voice": {"text": "hello"}})))
            .await
            .expect("send");
        assert_eq!(receipt.message_id, "mid-1");
        let sent = recorder.sent.lock().expect("sent");
        assert_eq!(sent.len(), 1);
        let voice = &sent[0].content["voice"];
        assert_eq!(voice["text"], json!("hello"));
        assert_eq!(voice["media_type"], json!("audio/mpeg"));
        assert_eq!(voice["audio"], json!(true));
        assert_eq!(voice["base64"], json!(base64_encode(b"ID3-audio")));
        assert_eq!(sent[0].target, json!({"handle": "+15550000001"}));
    }

    #[tokio::test]
    async fn a_text_delivery_passes_through_byte_identically() {
        let server = MockServer::start().await;
        let (edge, recorder, _store) = edge(&server);
        let content = json!({"text": "plain"});
        edge.send(delivery(content.clone())).await.expect("send");
        assert_eq!(recorder.sent.lock().expect("sent")[0].content, content);
        let requests = server.received_requests().await.expect("requests");
        assert!(requests.is_empty(), "no voice intent means no synthesis");
    }

    #[tokio::test]
    async fn a_synthesis_failure_is_an_honest_delivery_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tools/synthesize"))
            .respond_with(ResponseTemplate::new(500).set_body_string("no tts"))
            .mount(&server)
            .await;
        let (edge, recorder, _store) = edge(&server);
        let error = edge
            .send(delivery(json!({"voice": {"text": "hello"}})))
            .await
            .expect_err("synthesis must fail");
        match error {
            ChannelError::DeliveryFailed(message) => {
                assert!(message.contains("voice synthesis failed"), "{message}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(recorder.sent.lock().expect("sent").is_empty());
    }

    #[tokio::test]
    async fn an_empty_text_is_refused_before_any_synthesis() {
        let server = MockServer::start().await;
        let (edge, recorder, _store) = edge(&server);
        let error = edge
            .send(delivery(json!({"voice": {"text": "   "}})))
            .await
            .expect_err("empty text must fail");
        match error {
            ChannelError::DeliveryFailed(message) => {
                assert!(message.contains("no text to speak"), "{message}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(recorder.sent.lock().expect("sent").is_empty());
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }
}
