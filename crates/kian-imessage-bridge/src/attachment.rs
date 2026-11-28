use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use thiserror::Error;

pub const SIPS: &str = "/usr/bin/sips";

/// Extra `locate` attempts made when the attachment row is not committed yet.
///
/// kian fetches the bytes of an attachment as soon as `/v1/updates` mentions it,
/// but Messages commits the `attachment` and `message_attachment_join` rows a
/// moment after the `message` row itself. Inside that window `locate` returns
/// `Ok(None)` and used to answer 404, while the very same request is a 200 a few
/// hundred milliseconds later -- and kian does not retry. Thirty-two attempts
/// spaced `LOCATE_RETRY_DELAY` apart ride out that window (~8s) while keeping a
/// truly missing attachment a 404 once the budget is spent.
pub const LOCATE_RETRIES: usize = 32;

/// Spacing between two `locate` attempts while waiting for that commit.
pub const LOCATE_RETRY_DELAY: Duration = Duration::from_millis(250);

/// The media type the row records, else the one its filename implies.
///
/// A voice note lands in `chat.db` with an empty `mime_type` and an
/// `Audio Message.caf` filename; without the filename fallback the row is
/// neither an image nor an audio item and its bytes can never be read
/// (ADR-078). The table is deliberately small: a format this channel has no
/// tool for (video, PDF, a contact card) keeps no type and is refused by
/// `read` instead of being guessed at.
#[must_use]
pub fn media_type_of(filename: &Path, recorded: Option<&str>) -> Option<String> {
    if let Some(media_type) = recorded.map(str::trim).filter(|mime| !mime.is_empty()) {
        return Some(media_type.to_string());
    }
    let extension = filename.extension()?.to_str()?.to_ascii_lowercase();
    let inferred = match extension.as_str() {
        "caf" => "audio/x-caf",
        "m4a" => "audio/mp4",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "aac" => "audio/aac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "amr" => "audio/amr",
        "aif" | "aiff" => "audio/aiff",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "tif" | "tiff" => "image/tiff",
        "bmp" => "image/bmp",
        _ => return None,
    };
    Some(inferred.to_string())
}

#[derive(Debug, Clone)]
pub struct AttachmentSource {
    db: PathBuf,
    cache_dir: PathBuf,
    sips: PathBuf,
    jpeg_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct LocatedAttachment {
    pub filename: PathBuf,
    pub mime_type: Option<String>,
    pub total_bytes: Option<u64>,
    pub is_sticker: bool,
    pub sender: Option<String>,
    /// `is_from_me` for the owning row: a self-sent photo has no `handle`.
    pub is_from_me: bool,
    /// `"~"`, `"relative"` or `"absolute"`: the shape of `attachment.filename`.
    pub path_shape: &'static str,
    /// First 30 characters of the resolved path, `$HOME` collapsed to `~`.
    pub redacted_path: String,
}

#[derive(Debug)]
pub struct AttachmentBytes {
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub total_bytes: u64,
    pub variant: &'static str,
}

#[derive(Debug, Error)]
pub enum AttachmentError {
    #[error("attachment file is no longer available")]
    Gone {
        basename: String,
        shape: &'static str,
        redacted: String,
    },
    #[error("attachment exceeds the configured limit")]
    TooLarge,
    #[error("attachment type `{0}` is not an image or an audio message")]
    Unsupported(String),
    #[error("attachment bytes are unavailable")]
    Unavailable,
}

impl AttachmentSource {
    #[must_use]
    pub fn new(db: impl Into<PathBuf>, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            db: db.into(),
            cache_dir: cache_dir.into(),
            sips: PathBuf::from(SIPS),
            jpeg_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[must_use]
    pub fn with_sips(mut self, sips: impl Into<PathBuf>) -> Self {
        self.sips = sips.into();
        self
    }

    pub async fn locate(
        &self,
        rowid: u64,
        index: usize,
    ) -> Result<Option<LocatedAttachment>, AttachmentError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || locate_in_db(&db, rowid, index))
            .await
            .map_err(|_| AttachmentError::Unavailable)?
    }

    /// [`Self::locate`], retried while the row is not committed yet.
    ///
    /// See [`LOCATE_RETRIES`] for why the first miss is not final.
    pub async fn locate_settled(
        &self,
        rowid: u64,
        index: usize,
    ) -> Result<Option<LocatedAttachment>, AttachmentError> {
        let started = Instant::now();
        let mut attempts = 0usize;
        let located = locate_when_committed(LOCATE_RETRIES, LOCATE_RETRY_DELAY, || {
            attempts += 1;
            self.locate(rowid, index)
        })
        .await?;
        if attempts > 1 && located.is_some() {
            tracing::warn!(
                rowid,
                index,
                attempts,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "attachment row was not committed when kian asked; served after retrying"
            );
        }
        Ok(located)
    }

    pub async fn read(
        &self,
        rowid: u64,
        index: usize,
        located: LocatedAttachment,
        max_bytes: u64,
        as_jpeg: bool,
    ) -> Result<AttachmentBytes, AttachmentError> {
        if located.is_sticker {
            return Err(AttachmentError::Unsupported("sticker".into()));
        }
        let media_type = media_type_of(&located.filename, located.mime_type.as_deref())
            .ok_or_else(|| AttachmentError::Unsupported("application/octet-stream".into()))?;
        let lowered = media_type.to_ascii_lowercase();
        if !lowered.starts_with("image/") && !lowered.starts_with("audio/") {
            return Err(AttachmentError::Unsupported(media_type));
        }

        let metadata = tokio::fs::metadata(&located.filename)
            .await
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    gone(&located)
                } else {
                    AttachmentError::Unavailable
                }
            })?;
        if !metadata.is_file() {
            return Err(gone(&located));
        }
        if metadata.len() > max_bytes {
            return Err(AttachmentError::TooLarge);
        }

        let variant = if as_jpeg && media_type.eq_ignore_ascii_case("image/heic") {
            let _guard = self.jpeg_lock.lock().await;
            self.jpeg_variant(rowid, index, &located.filename, &metadata, max_bytes)
                .await?
        } else {
            None
        };
        let (bytes, media_type, variant, returned_len) = match variant {
            Some((bytes, len)) => (bytes, "image/jpeg".to_string(), "jpeg", len),
            None => {
                let bytes = tokio::fs::read(&located.filename).await.map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        gone(&located)
                    } else {
                        AttachmentError::Unavailable
                    }
                })?;
                let len = bytes.len() as u64;
                (bytes, media_type, "original", len)
            }
        };
        Ok(AttachmentBytes {
            bytes,
            media_type,
            total_bytes: located.total_bytes.unwrap_or(returned_len),
            variant,
        })
    }

    async fn jpeg_variant(
        &self,
        rowid: u64,
        index: usize,
        source: &Path,
        source_metadata: &std::fs::Metadata,
        max_bytes: u64,
    ) -> Result<Option<(Vec<u8>, u64)>, AttachmentError> {
        let cache_path = self.cache_path(rowid, index, source_metadata);
        if let Ok(metadata) = tokio::fs::metadata(&cache_path).await
            && metadata.is_file()
            && metadata.len() > 0
            && metadata.len() <= max_bytes
        {
            let bytes = tokio::fs::read(&cache_path)
                .await
                .map_err(|_| AttachmentError::Unavailable)?;
            return Ok(Some((bytes, metadata.len())));
        }

        let Some(parent) = cache_path.parent() else {
            return Err(AttachmentError::Unavailable);
        };
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| AttachmentError::Unavailable)?;
        let temporary = parent.join(format!(".{}.{}.tmp", index, uuid::Uuid::new_v4()));
        let status = tokio::process::Command::new(&self.sips)
            .args(["-s", "format", "jpeg", "-Z", "2048"])
            .arg(source)
            .arg("--out")
            .arg(&temporary)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|_| AttachmentError::Unavailable)?;
        if !status.success() {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AttachmentError::Unavailable);
        }
        let Ok(source_after) = tokio::fs::metadata(source).await else {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AttachmentError::Unavailable);
        };
        if !source_after.is_file()
            || source_after.len() != source_metadata.len()
            || source_after.modified().ok() != source_metadata.modified().ok()
        {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AttachmentError::Unavailable);
        }
        let generated = match tokio::fs::metadata(&temporary).await {
            Ok(metadata) if metadata.is_file() && metadata.len() > 0 => metadata,
            _ => {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(AttachmentError::Unavailable);
            }
        };
        if generated.len() > max_bytes {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AttachmentError::TooLarge);
        }
        if tokio::fs::rename(&temporary, &cache_path).await.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(AttachmentError::Unavailable);
        }
        let bytes = tokio::fs::read(&cache_path)
            .await
            .map_err(|_| AttachmentError::Unavailable)?;
        Ok(Some((bytes, generated.len())))
    }

    fn cache_path(&self, rowid: u64, index: usize, metadata: &std::fs::Metadata) -> PathBuf {
        let (seconds, nanos) = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map_or((0, 0), |duration| {
                (duration.as_secs(), duration.subsec_nanos())
            });
        self.cache_dir
            .join(rowid.to_string())
            .join(format!("{index}-{}-{seconds}-{nanos}.jpg", metadata.len()))
    }
}

/// Retries `locate` while it reports `Ok(None)`.
///
/// Only a "row not committed yet" miss is retried. A database failure or any
/// other error surfaces on the first attempt, so the 410/413/415/503 semantics
/// of the caller are unchanged.
pub async fn locate_when_committed<F, Fut>(
    retries: usize,
    delay: Duration,
    mut locate: F,
) -> Result<Option<LocatedAttachment>, AttachmentError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<LocatedAttachment>, AttachmentError>>,
{
    let mut attempts = 0;
    loop {
        if let Some(located) = locate().await? {
            return Ok(Some(located));
        }
        if attempts >= retries {
            return Ok(None);
        }
        attempts += 1;
        tokio::time::sleep(delay).await;
    }
}

fn locate_in_db(
    db: &Path,
    rowid: u64,
    index: usize,
) -> Result<Option<LocatedAttachment>, AttachmentError> {
    let rowid = i64::try_from(rowid).map_err(|_| AttachmentError::Unavailable)?;
    let index = i64::try_from(index).map_err(|_| AttachmentError::Unavailable)?;
    let connection = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| AttachmentError::Unavailable)?;
    connection
        .busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| AttachmentError::Unavailable)?;
    let row = connection
        .query_row(
            "SELECT a.filename, a.mime_type, a.total_bytes, a.is_sticker, h.id, m.is_from_me
             FROM message_attachment_join AS j
             JOIN attachment AS a ON a.ROWID = j.attachment_id
             JOIN message AS m ON m.ROWID = j.message_id
             LEFT JOIN handle AS h ON h.ROWID = m.handle_id
             WHERE m.ROWID = ?1
             ORDER BY a.ROWID ASC
             LIMIT 1 OFFSET ?2",
            params![rowid, index],
            |row| {
                let filename: Option<String> = row.get(0)?;
                let mime_type: Option<String> = row.get(1)?;
                let total_bytes: Option<i64> = row.get(2)?;
                let is_sticker: Option<bool> = row.get(3)?;
                let sender: Option<String> = row.get(4)?;
                let is_from_me: Option<bool> = row.get(5)?;
                Ok((
                    filename,
                    mime_type,
                    total_bytes,
                    is_sticker,
                    sender,
                    is_from_me,
                ))
            },
        )
        .optional()
        .map_err(|_| AttachmentError::Unavailable)?;
    let Some((filename, mime_type, total_bytes, is_sticker, sender, is_from_me)) = row else {
        return Ok(None);
    };
    let Some(raw) = filename.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let raw = raw.trim();
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|home| !home.as_os_str().is_empty())
        .ok_or(AttachmentError::Unavailable)?;
    let filename = resolve_attachment_path(raw, &home);
    Ok(Some(LocatedAttachment {
        redacted_path: redacted_prefix(&filename, &home),
        filename,
        mime_type,
        total_bytes: total_bytes.and_then(|value| u64::try_from(value).ok()),
        is_sticker: is_sticker.unwrap_or(false),
        sender,
        is_from_me: is_from_me.unwrap_or(false),
        path_shape: path_shape(raw),
    }))
}

/// Builds the 410 error for a located attachment, keeping only the
/// basename, the stored path shape and a `$HOME`-redacted prefix.
fn gone(located: &LocatedAttachment) -> AttachmentError {
    AttachmentError::Gone {
        basename: located.filename.file_name().map_or_else(
            || "attachment".to_string(),
            |name| name.to_string_lossy().to_string(),
        ),
        shape: located.path_shape,
        redacted: located.redacted_path.clone(),
    }
}

/// Expands a Messages `attachment.filename` into a filesystem path.
///
/// `chat.db` stores three shapes: a `~/`-prefixed path, a bare relative
/// path such as `Attachments/ab/cd/IMG_0035.heic`, and an absolute path.
/// Treating the relative form as absolute is why every attachment used to
/// resolve to a missing file.
#[must_use]
pub fn resolve_attachment_path(raw: &str, home: &Path) -> PathBuf {
    if let Some(rest) = raw.strip_prefix("~/") {
        home.join(rest)
    } else if raw.starts_with('/') {
        PathBuf::from(raw)
    } else {
        home.join("Library/Messages").join(raw)
    }
}

/// Classifies a stored `attachment.filename` without revealing the path.
#[must_use]
pub fn path_shape(raw: &str) -> &'static str {
    if raw.starts_with("~/") {
        "~"
    } else if raw.starts_with('/') {
        "absolute"
    } else {
        "relative"
    }
}

/// First 30 characters of `path` with `home` collapsed to `~`.
fn redacted_prefix(path: &Path, home: &Path) -> String {
    let text = path.to_string_lossy();
    let home = home.to_string_lossy();
    let redacted = match text.strip_prefix(home.as_ref()) {
        Some(rest) => format!("~{rest}"),
        None => text.to_string(),
    };
    redacted.chars().take(30).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        AttachmentError, AttachmentSource, LocatedAttachment, locate_when_committed, media_type_of,
        path_shape, redacted_prefix, resolve_attachment_path,
    };
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    const HOME: &str = "/Users/tester";

    #[test]
    fn attachment_filenames_are_normalized_from_the_three_stored_shapes() {
        let home = Path::new(HOME);
        assert_eq!(
            resolve_attachment_path("~/Library/Messages/Attachments/a/b/IMG_0035.heic", home),
            PathBuf::from("/Users/tester/Library/Messages/Attachments/a/b/IMG_0035.heic")
        );
        assert_eq!(
            resolve_attachment_path("Attachments/c/d/IMG_0036.heic", home),
            PathBuf::from("/Users/tester/Library/Messages/Attachments/c/d/IMG_0036.heic")
        );
        assert_eq!(
            resolve_attachment_path("/var/folders/xy/T/IMG_0037.jpg", home),
            PathBuf::from("/var/folders/xy/T/IMG_0037.jpg")
        );
    }

    #[test]
    fn path_shapes_are_reported_without_the_path_itself() {
        assert_eq!(path_shape("~/Library/Messages/Attachments/a/IMG.heic"), "~");
        assert_eq!(path_shape("Attachments/a/IMG.heic"), "relative");
        assert_eq!(
            path_shape("/Users/tester/Library/Messages/Attachments/a/IMG.heic"),
            "absolute"
        );
    }

    #[test]
    fn the_logged_prefix_is_redacted_to_home_and_truncated() {
        let path =
            Path::new("/Users/tester/Library/Messages/Attachments/aaaa/bbbb/very-long-name.heic");
        let prefix = redacted_prefix(path, Path::new(HOME));
        assert!(prefix.starts_with("~/Library/Messages"));
        assert_eq!(prefix.chars().count(), 30);
        assert!(!prefix.contains(HOME));
    }

    fn temp_file(extension: &str, bytes: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "kian-attachment-test-{}.{extension}",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, bytes).expect("write the fixture");
        path
    }

    fn located_audio(path: &Path, mime: Option<&str>) -> LocatedAttachment {
        LocatedAttachment {
            filename: path.to_path_buf(),
            mime_type: mime.map(str::to_string),
            total_bytes: None,
            is_sticker: false,
            sender: Some("+15551234567".into()),
            is_from_me: false,
            path_shape: "absolute",
            redacted_path: "~/Library/Messages/Attachm".into(),
        }
    }

    fn source_for(path: &Path) -> AttachmentSource {
        AttachmentSource::new(
            PathBuf::from("/nonexistent/chat.db"),
            path.parent()
                .unwrap_or(Path::new("/tmp"))
                .join("kian-cache"),
        )
    }

    #[test]
    fn a_recorded_media_type_wins_and_a_filename_supplies_a_missing_one() {
        let voice = Path::new("/Users/tester/Library/Messages/Attachments/a/Audio Message.caf");
        assert_eq!(
            media_type_of(voice, Some("audio/mp4")).as_deref(),
            Some("audio/mp4")
        );
        assert_eq!(
            media_type_of(voice, Some("  ")).as_deref(),
            Some("audio/x-caf"),
            "an empty recorded type is the voice-note case (ADR-078)"
        );
        assert_eq!(media_type_of(voice, None).as_deref(), Some("audio/x-caf"));
        assert_eq!(
            media_type_of(Path::new("IMG_0001.HEIC"), None).as_deref(),
            Some("image/heic")
        );
        assert_eq!(media_type_of(Path::new("notes.pdf"), None), None);
        assert_eq!(
            media_type_of(Path::new("Audio Message"), None),
            None,
            "a filename without an extension proves nothing"
        );
    }

    #[tokio::test]
    async fn an_audio_item_with_no_recorded_type_is_served_as_its_filename_implies() {
        let path = temp_file("caf", b"caf-bytes");
        let served = source_for(&path)
            .read(7, 0, located_audio(&path, Some("")), 1 << 20, false)
            .await
            .expect("a voice note is readable");
        assert_eq!(served.media_type, "audio/x-caf");
        assert_eq!(served.variant, "original");
        assert_eq!(served.bytes, b"caf-bytes");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn audio_ignores_the_jpeg_variant_selector() {
        let path = temp_file("caf", b"caf-bytes");
        let served = source_for(&path)
            .read(7, 0, located_audio(&path, None), 1 << 20, true)
            .await
            .expect("a voice note is readable");
        assert_eq!(
            served.variant, "original",
            "only image/heic has a jpeg variant"
        );
        assert_eq!(served.media_type, "audio/x-caf");
        assert_eq!(served.bytes, b"caf-bytes");
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn a_type_the_edge_has_no_tool_for_is_refused() {
        let path = temp_file("pdf", b"%PDF-1.7");
        let error = source_for(&path)
            .read(
                7,
                0,
                located_audio(&path, Some("application/pdf")),
                1 << 20,
                false,
            )
            .await
            .expect_err("a pdf has no media tool");
        assert!(matches!(error, AttachmentError::Unsupported(kind) if kind == "application/pdf"));
        std::fs::remove_file(&path).ok();
    }

    fn sample_attachment() -> LocatedAttachment {
        LocatedAttachment {
            filename: PathBuf::from("/Users/tester/Library/Messages/Attachments/a/IMG_0035.heic"),
            mime_type: Some("image/heic".into()),
            total_bytes: Some(42),
            is_sticker: false,
            sender: Some("+15551234567".into()),
            is_from_me: false,
            path_shape: "~",
            redacted_path: "~/Library/Messages/Attachm".into(),
        }
    }

    /// The commit race: two misses (kian arrived before `chat.db` had the
    /// join/attachment rows) followed by a hit must be served, not 404'd.
    #[tokio::test]
    async fn a_row_committed_after_two_misses_is_found() {
        let calls = Cell::new(0_usize);
        let located = locate_when_committed(6, Duration::ZERO, || {
            let attempt = calls.get() + 1;
            calls.set(attempt);
            async move {
                if attempt < 3 {
                    Ok(None)
                } else {
                    Ok(Some(sample_attachment()))
                }
            }
        })
        .await
        .expect("a committed row must not be an error");
        assert_eq!(calls.get(), 3);
        let located = located.expect("the third attempt finds the row");
        assert_eq!(located.mime_type.as_deref(), Some("image/heic"));
    }

    #[tokio::test]
    async fn a_missing_row_stays_not_found_after_the_whole_budget() {
        let calls = Cell::new(0_usize);
        let located = locate_when_committed(6, Duration::ZERO, || {
            calls.set(calls.get() + 1);
            async { Ok(None) }
        })
        .await
        .expect("an absent row is not an error");
        assert_eq!(calls.get(), 7, "one try plus six retries");
        assert!(located.is_none());
    }

    #[tokio::test]
    async fn database_failures_are_never_retried() {
        let calls = Cell::new(0_usize);
        let error = locate_when_committed(6, Duration::ZERO, || {
            calls.set(calls.get() + 1);
            async { Err::<Option<LocatedAttachment>, _>(AttachmentError::Unavailable) }
        })
        .await
        .expect_err("a failing database must surface immediately");
        assert!(matches!(error, AttachmentError::Unavailable));
        assert_eq!(calls.get(), 1, "503 must not be delayed or swallowed");
    }
}
