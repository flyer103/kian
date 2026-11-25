use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use thiserror::Error;

pub const SIPS: &str = "/usr/bin/sips";

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
    Gone,
    #[error("attachment exceeds the configured limit")]
    TooLarge,
    #[error("attachment type `{0}` is not an image")]
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
        let media_type = located
            .mime_type
            .as_deref()
            .map(str::trim)
            .filter(|mime| !mime.is_empty())
            .ok_or_else(|| AttachmentError::Unsupported("application/octet-stream".into()))?
            .to_string();
        if !media_type.to_ascii_lowercase().starts_with("image/") {
            return Err(AttachmentError::Unsupported(media_type));
        }

        let metadata = tokio::fs::metadata(&located.filename)
            .await
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    AttachmentError::Gone
                } else {
                    AttachmentError::Unavailable
                }
            })?;
        if !metadata.is_file() {
            return Err(AttachmentError::Gone);
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
                        AttachmentError::Gone
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
            "SELECT a.filename, a.mime_type, a.total_bytes, a.is_sticker, h.id
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
                Ok((filename, mime_type, total_bytes, is_sticker, sender))
            },
        )
        .optional()
        .map_err(|_| AttachmentError::Unavailable)?;
    Ok(
        row.and_then(|(filename, mime_type, total_bytes, is_sticker, sender)| {
            let filename = filename.filter(|value| !value.trim().is_empty())?;
            Some(LocatedAttachment {
                filename: PathBuf::from(filename),
                mime_type,
                total_bytes: total_bytes.and_then(|value| u64::try_from(value).ok()),
                is_sticker: is_sticker.unwrap_or(false),
                sender,
            })
        }),
    )
}
