//! Tool-result artifacts at the assembly edge (ADR-055, phase 2).
//!
//! The engine spills oversized successful `tool/result` outputs into a
//! content-addressed [`ArtifactStore`] before the event is written; the
//! model then reads the bounded preview from context and pages the rest
//! back through the `artifact_read` tool implemented here. The tool is a
//! bundle-edge [`ToolProvider`] — the core knows nothing about blobs —
//! composed with the local fs/exec tools by `local-tools` whenever the
//! `artifacts:` section is present.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::seams::{ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema};
use kian_store::ArtifactStore;
use serde_json::{Value, json};

use crate::config::ServeConfig;

/// Default window size when `limit` is omitted, bytes.
pub const DEFAULT_ARTIFACT_WINDOW: usize = 8 * 1024;
/// Hard cap on one window, bytes (the model can page, not slurp).
pub const MAX_ARTIFACT_WINDOW: usize = 64 * 1024;

/// The `artifact_read` tool: a bounded, offset-addressable read of one
/// spilled tool result.
pub struct ArtifactReadTool {
    store: Arc<ArtifactStore>,
}

impl ArtifactReadTool {
    #[must_use]
    pub fn new(store: Arc<ArtifactStore>) -> Self {
        Self { store }
    }

    /// The advertised schema (frozen bytes once an agent mounts it).
    #[must_use]
    pub fn schema() -> ToolSchema {
        ToolSchema {
            name: "artifact_read".into(),
            description:
                "Read a window of a spilled tool-result artifact by its content-addressed id."
                    .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "artifact id from a truncated tool result"},
                    "offset": {"type": "integer", "description": "byte offset into the artifact (default 0)"},
                    "limit": {"type": "integer", "description": "window size in bytes (default 8192, max 65536)"}
                },
                "required": ["id"]
            }),
        }
    }
}

struct ReadArgs {
    id: String,
    offset: u64,
    limit: usize,
}

fn parse_args(arguments: &Value) -> Result<ReadArgs, ToolError> {
    let object = arguments.as_object().ok_or_else(|| {
        ToolError::InvalidCall("artifact_read arguments must be an object".into())
    })?;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidCall("artifact_read requires a string `id`".into()))?
        .to_string();
    let offset = match object.get("offset") {
        None | Some(Value::Null) => 0,
        Some(value) => value.as_u64().ok_or_else(|| {
            ToolError::InvalidCall("artifact_read `offset` must be a non-negative integer".into())
        })?,
    };
    let limit = match object.get("limit") {
        None | Some(Value::Null) => DEFAULT_ARTIFACT_WINDOW,
        Some(value) => {
            let limit = value.as_u64().ok_or_else(|| {
                ToolError::InvalidCall(
                    "artifact_read `limit` must be a non-negative integer".into(),
                )
            })?;
            if limit == 0 {
                DEFAULT_ARTIFACT_WINDOW
            } else {
                usize::try_from(limit)
                    .unwrap_or(MAX_ARTIFACT_WINDOW)
                    .min(MAX_ARTIFACT_WINDOW)
            }
        }
    };
    Ok(ReadArgs { id, offset, limit })
}

impl ToolProvider for ArtifactReadTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![Self::schema()]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        if call.name != "artifact_read" {
            return Box::pin(async move {
                Err(ToolError::InvalidCall(format!(
                    "artifact_read provider cannot serve `{}`",
                    call.name
                )))
            });
        }
        let call_id = call.id;
        let parsed = match parse_args(&call.arguments) {
            Ok(parsed) => parsed,
            Err(e) => {
                return Box::pin(async move {
                    let message = e.to_string();
                    Ok(ToolResult {
                        call_id,
                        output: json!({ "error": message }),
                        is_error: true,
                        redactions: Vec::new(),
                    })
                });
            }
        };
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let ReadArgs { id, offset, limit } = parsed;
            let read_id = id.clone();
            let window =
                tokio::task::spawn_blocking(move || store.read_window(&read_id, offset, limit))
                    .await
                    .map_err(|e| ToolError::Execution(format!("artifact read task: {e}")))?;
            let output = match window {
                Ok(Some(window)) => {
                    let content = String::from_utf8_lossy(&window.content).into_owned();
                    let next_offset = window.offset.saturating_add(window.content.len() as u64);
                    let mut out = json!({
                        "id": id,
                        "size": window.size,
                        "offset": window.offset,
                        "content": content,
                    });
                    if next_offset < window.size {
                        out["next_offset"] = json!(next_offset);
                    }
                    (out, false)
                }
                Ok(None) => (
                    json!({ "error": "artifact expired or unavailable", "artifact_id": id }),
                    true,
                ),
                Err(e) => (
                    json!({ "error": format!("artifact read failed: {e}") }),
                    true,
                ),
            };
            Ok(ToolResult {
                call_id,
                output: output.0,
                is_error: output.1,
                redactions: Vec::new(),
            })
        })
    }
}

/// The artifact ids a JSONL export references, sorted and de-duplicated
/// (P3b, ADR-058). A spill-shaped `tool/result` carries its reference at
/// `content.output.artifact.id`; inline results and errors carry none,
/// so an export from a spill-off assembly pins nothing.
pub fn referenced_artifact_ids<'a>(
    events: impl IntoIterator<Item = &'a Event>,
) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for event in events {
        if !matches!(event.kind, EventKind::ToolResult) {
            continue;
        }
        if let Some(id) = event.bytes["content"]["output"]["artifact"]["id"].as_str()
            && !id.is_empty()
        {
            ids.insert(id.to_string());
        }
    }
    ids
}

/// What an export pinned: blobs copied beside the JSONL, plus ids whose
/// blob was already gone (reclaimed by retention GC). A missing blob
/// never fails the export — the event stream is the truth and the
/// preview still renders; the report states the degradation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPinReport {
    pub pinned: usize,
    pub missing: Vec<String>,
}

impl ArtifactPinReport {
    /// The one-line honest summary appended to the export message.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut out = format!("pinned {} artifact(s) beside the export", self.pinned);
        if !self.missing.is_empty() {
            out.push_str(&format!(
                "; {} missing (already reclaimed): {}",
                self.missing.len(),
                self.missing.join(", ")
            ));
        }
        out
    }
}

/// Copies every referenced blob into `<out>.artifacts/<id>`, a sibling
/// directory of the exported JSONL (P3b, ADR-058). Content-addressed by
/// construction: the id is the file name, so a re-export is idempotent
/// and the pinned tree is verifiable byte-for-byte.
pub fn pin_artifacts(
    store: &ArtifactStore,
    ids: &BTreeSet<String>,
    out: &std::path::Path,
) -> Result<ArtifactPinReport, String> {
    let mut os = out.as_os_str().to_owned();
    os.push(".artifacts");
    let dir = std::path::PathBuf::from(os);
    let mut report = ArtifactPinReport {
        pinned: 0,
        missing: Vec::new(),
    };
    if ids.is_empty() {
        return Ok(report);
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    for id in ids {
        match store
            .get(id)
            .map_err(|e| format!("artifacts: read {id}: {e}"))?
        {
            Some(bytes) => {
                let path = dir.join(id);
                std::fs::write(&path, bytes)
                    .map_err(|e| format!("write {}: {e}", path.display()))?;
                report.pinned += 1;
            }
            None => report.missing.push(id.clone()),
        }
    }
    Ok(report)
}

/// `kian artifacts gc` — one retention pass over a content-addressed blob tree,
/// oldest mtime first, until both configured bounds hold (ADR-055).
///
/// Bounds come from the `artifacts:` section; an explicit `dir` lets an
/// operator collect a standalone tree without a loadable config file.
pub fn run_artifacts_gc(
    config_path: &std::path::Path,
    dir: Option<std::path::PathBuf>,
    max_bytes: Option<u64>,
    retention_days: Option<u64>,
    json: bool,
) -> Result<String, String> {
    let config = match dir {
        Some(_) => ServeConfig::load(config_path).ok(),
        None => Some(ServeConfig::load(config_path).map_err(|e| format!("config: {e}"))?),
    };
    let gc = config
        .as_ref()
        .and_then(|config| config.artifacts.as_ref())
        .map(|artifacts| artifacts.gc.clone())
        .unwrap_or_default();
    let dir = match dir {
        Some(dir) => dir,
        None => {
            let config = config.expect("config loads when dir is absent");
            let artifacts = config.artifacts.as_ref().ok_or_else(|| {
                "artifacts: the config has no `artifacts:` section — add one or pass --dir"
                    .to_string()
            })?;
            artifacts.resolve_dir(&config.storage)?
        }
    };
    let max_bytes = max_bytes.unwrap_or(gc.max_bytes);
    let retention_days = retention_days.unwrap_or(gc.retention_days);
    if max_bytes == 0 && retention_days == 0 {
        return Err(
            "artifacts: both max_bytes and retention_days are 0 — nothing would be reclaimed"
                .to_string(),
        );
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0);
    let retention_ms = retention_days.saturating_mul(24 * 60 * 60 * 1000);
    let store =
        ArtifactStore::open(&dir).map_err(|e| format!("artifacts: open {}: {e}", dir.display()))?;
    let report = store
        .gc(now_ms, max_bytes, retention_ms)
        .map_err(|e| format!("artifacts: gc {}: {e}", dir.display()))?;
    if json {
        return serde_json::to_string_pretty(&json!({
            "dir": dir.display().to_string(),
            "removed": report.removed,
            "bytes_freed": report.bytes_freed,
            "kept": report.kept,
            "bytes_kept": report.bytes_kept,
            "max_bytes": max_bytes,
            "retention_days": retention_days,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "kian artifacts gc: {}\n  removed {} blobs ({} bytes)\n  kept {} blobs ({} bytes)\n  max_bytes={} retention_days={}\n",
        dir.display(),
        report.removed,
        report.bytes_freed,
        report.kept,
        report.bytes_kept,
        max_bytes,
        retention_days,
    ))
}

/// `kian artifacts put` — ingest a local file into the content-addressed
/// blob tree and print its reference (ADR-060).
///
/// Bounds and the default directory come from the artifacts: section; an
/// explicit dir lets an operator ingest into a standalone tree.
pub fn run_artifacts_put(
    config_path: &std::path::Path,
    dir: Option<std::path::PathBuf>,
    path: &std::path::Path,
    media_type: Option<String>,
    json: bool,
) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let config = match dir {
        Some(_) => ServeConfig::load(config_path).ok(),
        None => Some(ServeConfig::load(config_path).map_err(|e| format!("config: {e}"))?),
    };
    let dir = match dir {
        Some(dir) => dir,
        None => {
            let config = config.expect("config loads when dir is absent");
            let artifacts = config.artifacts.as_ref().ok_or_else(|| {
                "artifacts: the config has no artifacts: section, add one or pass --dir".to_string()
            })?;
            artifacts.resolve_dir(&config.storage)?
        }
    };
    let media_type = media_type.unwrap_or_else(|| infer_media_type(path));
    let store =
        ArtifactStore::open(&dir).map_err(|e| format!("artifacts: open {}: {e}", dir.display()))?;
    let reference = store
        .put(&bytes, media_type.clone())
        .map_err(|e| format!("artifacts: put {}: {e}", dir.display()))?;
    if json {
        return serde_json::to_string_pretty(&json!({
            "dir": dir.display().to_string(),
            "path": path.display().to_string(),
            "id": reference.id,
            "size": reference.size,
            "media_type": reference.media_type,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "kian artifacts put: {} -> {} ({}, {} bytes)\n",
        path.display(),
        reference.id,
        reference.media_type,
        reference.size,
    ))
}

/// The media type guessed from a file extension (the operator override wins).
fn infer_media_type(path: &std::path::Path) -> String {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "wav" => "audio/wav",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "m4a" => "audio/mp4",
        "pdf" => "application/pdf",
        "txt" => "text/plain",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kian-artifact-tool-{}", uuid::Uuid::new_v4()))
    }

    async fn invoke(tool: &ArtifactReadTool, arguments: Value) -> ToolResult {
        tool.invoke(ToolCall {
            id: "call-1".into(),
            name: "artifact_read".into(),
            arguments,
        })
        .await
        .expect("artifact_read never fails the call itself")
    }

    #[tokio::test]
    async fn reads_windows_and_pages_with_next_offset() {
        let dir = temp_dir();
        let store = Arc::new(ArtifactStore::open(&dir).expect("store"));
        let blob: Vec<u8> = (0..100u8).collect();
        let reference = store.put(&blob, "application/json").expect("put");
        let tool = ArtifactReadTool::new(Arc::clone(&store));

        let first = invoke(&tool, json!({ "id": reference.id, "limit": 40 })).await;
        assert!(!first.is_error);
        assert_eq!(first.output["offset"], json!(0));
        assert_eq!(first.output["size"], json!(100));
        assert_eq!(first.output["next_offset"], json!(40));
        assert_eq!(first.output["content"].as_str().unwrap().len(), 40);

        let second = invoke(
            &tool,
            json!({ "id": reference.id, "offset": 40, "limit": 40 }),
        )
        .await;
        assert_eq!(second.output["offset"], json!(40));
        assert_eq!(second.output["next_offset"], json!(80));

        let tail = invoke(&tool, json!({ "id": reference.id, "offset": 80 })).await;
        assert_eq!(tail.output["offset"], json!(80));
        assert!(
            tail.output.get("next_offset").is_none(),
            "the last window carries no next offset"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_missing_blob_is_an_honest_error_and_bad_arguments_are_rejected() {
        let dir = temp_dir();
        let store = Arc::new(ArtifactStore::open(&dir).expect("store"));
        let tool = ArtifactReadTool::new(store);
        let missing = invoke(&tool, json!({ "id": "a".repeat(64) })).await;
        assert!(
            missing.is_error,
            "a GC'd blob reports expired, never empty content"
        );
        assert_eq!(
            missing.output["error"],
            json!("artifact expired or unavailable")
        );

        let bad = invoke(&tool, json!({})).await;
        assert!(bad.is_error);
        assert!(
            bad.output["error"]
                .as_str()
                .unwrap()
                .contains("requires a string")
        );

        let invalid_id = invoke(&tool, json!({ "id": "../../etc/passwd" })).await;
        assert!(
            invalid_id.is_error,
            "ids are validated before any path join"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn the_window_is_capped_and_zero_limit_falls_back_to_the_default() {
        let dir = temp_dir();
        let store = Arc::new(ArtifactStore::open(&dir).expect("store"));
        let reference = store
            .put(&vec![b'z'; 200_000], "application/json")
            .expect("put");
        let tool = ArtifactReadTool::new(store);
        let window = invoke(&tool, json!({ "id": reference.id, "limit": 10_000_000 })).await;
        assert_eq!(
            window.output["content"].as_str().unwrap().len(),
            MAX_ARTIFACT_WINDOW
        );
        let default = invoke(&tool, json!({ "id": reference.id, "limit": 0 })).await;
        assert_eq!(
            default.output["content"].as_str().unwrap().len(),
            DEFAULT_ARTIFACT_WINDOW
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_gc_command_reclaims_aged_blobs_without_a_loadable_config() {
        let dir = temp_dir();
        let store = ArtifactStore::open(&dir).expect("store");
        let aged = store.put(b"old payload", "application/json").expect("put");
        let aged_path = dir.join(&aged.id[..2]).join(&aged.id);
        let hundred_days_ago =
            std::time::SystemTime::now() - std::time::Duration::from_secs(100 * 24 * 60 * 60);
        std::fs::File::options()
            .write(true)
            .open(&aged_path)
            .expect("open blob")
            .set_modified(hundred_days_ago)
            .expect("mtime");
        let fresh = store
            .put(b"fresh payload", "application/json")
            .expect("put");
        let out = run_artifacts_gc(
            std::path::Path::new("/nonexistent/kian.yaml"),
            Some(dir.clone()),
            None,
            Some(30),
            true,
        )
        .expect("gc runs");
        let report: Value = serde_json::from_str(&out).expect("json report");
        assert_eq!(report["removed"], 1);
        assert_eq!(report["kept"], 1);
        assert!(!aged_path.exists(), "aged blob removed");
        assert!(
            dir.join(&fresh.id[..2]).join(&fresh.id).exists(),
            "fresh blob kept"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn tool_result_event(seq: u64, artifact: Option<&str>) -> Event {
        let output = match artifact {
            Some(id) => json!({
                "artifact": {"id": id, "size": 5, "media_type": "application/json"},
                "preview": "preview",
                "truncated": true
            }),
            None => json!({"ok": true}),
        };
        Event {
            seq,
            session: kian_core::model::SessionId(uuid::Uuid::nil()),
            epoch: kian_core::model::Epoch(1),
            ts: 1_761_900_000_000,
            kind: EventKind::ToolResult,
            bytes: json!({"role": "tool", "content": {"call_id": "c1", "output": output, "is_error": false}}),
            version: kian_core::events::Event::current_version(),
        }
    }

    #[test]
    fn referenced_ids_come_only_from_spill_shaped_results() {
        let event = tool_result_event(2, Some(&"a".repeat(64)));
        let inline = tool_result_event(3, None);
        let ids = referenced_artifact_ids([&event, &inline]);
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"a".repeat(64)));
    }

    #[test]
    fn export_pinning_copies_blobs_and_reports_missing_ones() {
        let dir = temp_dir();
        let store = ArtifactStore::open(&dir).expect("store");
        let blob = store
            .put(b"{\"payload\":\"spilled\"}", "application/json")
            .expect("put");
        let missing = "b".repeat(64);
        let mut ids = BTreeSet::new();
        ids.insert(blob.id.clone());
        ids.insert(missing.clone());
        let out = temp_dir().join("session-export.jsonl");
        let report = pin_artifacts(&store, &ids, &out).expect("pin");
        assert_eq!(report.pinned, 1);
        assert_eq!(report.missing, vec![missing.clone()]);
        let pinned = std::path::PathBuf::from(format!("{}.artifacts", out.display()));
        assert_eq!(
            std::fs::read(pinned.join(&blob.id)).expect("pinned blob"),
            b"{\"payload\":\"spilled\"}"
        );
        assert!(
            report.summary().contains("1 missing (already reclaimed)"),
            "the report states the degradation honestly"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&pinned);
    }

    #[test]
    fn the_put_command_ingests_a_file_with_an_inferred_media_type() {
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).expect("dir");
        let source = dir.join("frame.png");
        std::fs::write(&source, b"\x89PNG\r\n\x1a\nbytes").expect("write");
        let out = run_artifacts_put(
            std::path::Path::new("/nonexistent/kian.yaml"),
            Some(dir.clone()),
            &source,
            None,
            true,
        )
        .expect("put runs");
        let report: Value = serde_json::from_str(&out).expect("json report");
        assert_eq!(report["media_type"], "image/png");
        assert_eq!(report["size"], 13);
        let id = report["id"].as_str().expect("id").to_string();
        let store = ArtifactStore::open(&dir).expect("store");
        assert_eq!(
            store.get(&id).expect("get").expect("blob"),
            b"\x89PNG\r\n\x1a\nbytes".to_vec()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_put_command_honours_an_explicit_media_type_and_reports_text() {
        let dir = temp_dir();
        std::fs::create_dir_all(&dir).expect("dir");
        let source = dir.join("clip.bin");
        std::fs::write(&source, b"raw").expect("write");
        let out = run_artifacts_put(
            std::path::Path::new("/nonexistent/kian.yaml"),
            Some(dir.clone()),
            &source,
            Some("audio/wav".to_string()),
            false,
        )
        .expect("put runs");
        assert!(out.contains("kian artifacts put:"), "{out}");
        assert!(out.contains("audio/wav"), "{out}");
        assert_eq!(
            infer_media_type(std::path::Path::new("clip.MP3")),
            "audio/mpeg"
        );
        assert_eq!(
            infer_media_type(std::path::Path::new("clip.webp")),
            "image/webp"
        );
        assert_eq!(
            infer_media_type(std::path::Path::new("clip")),
            "application/octet-stream"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
