//! Content-addressed artifact blobs (ADR-055).
//!
//! Large tool results do not belong in the event log: the event carries a
//! reference plus a bounded preview, and this store keeps the full bytes.
//! It is deliberately **not** truth — see ADR-055. A blob may age out at any
//! time and replay stays correct (the preview is in the event), so the store
//! is an operator cache with retention GC, never reference-counted.
//!
//! Layout: root/<first two hex chars>/<64-hex id>; ids are the sha256 hex of
//! the bytes, validated before every path join so a crafted id cannot traverse
//! out of the root. Writes are temp-file + rename (a crash leaves no half
//! blob; an existing blob is reused, so identical payloads dedupe).

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A reference to one stored blob. media_type is advisory (the caller spelled
/// it at spill time); size is the full byte length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: String,
    pub size: u64,
    pub media_type: String,
}

/// One bounded read window over a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWindow {
    /// Full blob size.
    pub size: u64,
    /// Where this window starts (the requested offset, clamped to size).
    pub offset: u64,
    pub content: Vec<u8>,
}

/// Outcome of one GC pass. kept/bytes_kept describe what remains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub removed: u64,
    pub bytes_freed: u64,
    pub kept: u64,
    pub bytes_kept: u64,
}

/// Blob tree occupancy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactStats {
    pub blobs: u64,
    pub bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact id {0:?} is not a 64-character lowercase hex sha256")]
    InvalidId(String),
    #[error("artifact store io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    /// Open (creating if needed) the blob tree rooted at root.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Store bytes (content-addressed; an identical payload is reused) and
    /// return its reference.
    pub fn put(
        &self,
        bytes: &[u8],
        media_type: impl Into<String>,
    ) -> Result<ArtifactRef, ArtifactError> {
        let id = sha256_hex(bytes);
        let path = self.path_for(&id)?;
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let tmp = self.root.join("tmp").join(format!(
                "{id}.{}.{}",
                std::process::id(),
                nanos_since_epoch()
            ));
            fs::write(&tmp, bytes)?;
            match fs::rename(&tmp, &path) {
                Ok(()) => {}
                Err(_) if path.exists() => {
                    let _ = fs::remove_file(&tmp);
                }
                Err(e) => {
                    let _ = fs::remove_file(&tmp);
                    return Err(ArtifactError::Io(e));
                }
            }
        }
        Ok(ArtifactRef {
            id,
            size: bytes.len() as u64,
            media_type: media_type.into(),
        })
    }

    /// Read one blob. Ok(None) means it is gone (aged out or never stored) —
    /// a normal outcome, not an error (ADR-055).
    pub fn get(&self, id: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        let path = self.path_for(id)?;
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ArtifactError::Io(e)),
        }
    }

    /// Bounded window read: offset clamps to the end, limit bounds the bytes
    /// returned. Ok(None) means the blob is gone.
    pub fn read_window(
        &self,
        id: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Option<ArtifactWindow>, ArtifactError> {
        let path = self.path_for(id)?;
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(ArtifactError::Io(e)),
        };
        let size = file.metadata()?.len();
        let offset = offset.min(size);
        file.seek(SeekFrom::Start(offset))?;
        let mut content = Vec::new();
        file.take(limit as u64).read_to_end(&mut content)?;
        Ok(Some(ArtifactWindow {
            size,
            offset,
            content,
        }))
    }

    /// Blob occupancy (one filesystem walk; operator/CLI surface only).
    pub fn stats(&self) -> Result<ArtifactStats, ArtifactError> {
        let mut stats = ArtifactStats::default();
        for (_, size, _) in self.walk()? {
            stats.blobs += 1;
            stats.bytes += size;
        }
        Ok(stats)
    }

    /// Retention-based GC: delete blobs older than retention_ms (when
    /// non-zero), then oldest-first until total bytes <= max_bytes (when
    /// non-zero). Never reference-counted; see ADR-055.
    pub fn gc(
        &self,
        now_ms: u64,
        max_bytes: u64,
        retention_ms: u64,
    ) -> Result<GcReport, ArtifactError> {
        let mut files = self.walk()?;
        files.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
        let mut total: u64 = files.iter().map(|(_, size, _)| size).sum();
        let mut report = GcReport::default();
        let mut survivors = Vec::new();
        let cutoff = if retention_ms == 0 {
            None
        } else {
            Some(now_ms.saturating_sub(retention_ms))
        };
        for (path, size, mtime) in files {
            if cutoff.is_some_and(|cutoff| mtime < cutoff) {
                fs::remove_file(&path)?;
                report.removed += 1;
                report.bytes_freed += size;
                total = total.saturating_sub(size);
            } else {
                survivors.push((path, size));
            }
        }
        if max_bytes != 0 {
            for (path, size) in &survivors {
                if total <= max_bytes {
                    break;
                }
                fs::remove_file(path)?;
                report.removed += 1;
                report.bytes_freed += *size;
                total = total.saturating_sub(*size);
            }
        }
        for (path, size) in &survivors {
            if path.exists() {
                report.kept += 1;
                report.bytes_kept += *size;
            }
        }
        if let Some(cutoff) = cutoff {
            let tmp = self.root.join("tmp");
            if let Ok(entries) = fs::read_dir(&tmp) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata()
                        && mtime_ms(&meta) < cutoff
                    {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(report)
    }

    fn walk(&self) -> Result<Vec<(PathBuf, u64, u64)>, ArtifactError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "tmp" || name.len() != 2 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            for file in fs::read_dir(&dir)? {
                let file = file?;
                let file_name = file.file_name();
                let file_name = file_name.to_string_lossy();
                if !is_valid_id(&file_name) {
                    continue;
                }
                let meta = file.metadata()?;
                if !meta.is_file() {
                    continue;
                }
                out.push((file.path(), meta.len(), mtime_ms(&meta)));
            }
        }
        Ok(out)
    }

    fn path_for(&self, id: &str) -> Result<PathBuf, ArtifactError> {
        if !is_valid_id(id) {
            return Err(ArtifactError::InvalidId(id.to_string()));
        }
        Ok(self.root.join(&id[..2]).join(id))
    }
}

fn is_valid_id(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn mtime_ms(meta: &fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn nanos_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn store() -> ArtifactStore {
        let root = std::env::temp_dir().join(format!("kian-artifacts-{}", Uuid::new_v4()));
        ArtifactStore::open(root).expect("open")
    }

    #[test]
    fn put_get_round_trips_and_dedupes_by_content() {
        let store = store();
        let a = store.put(b"hello artifact", "text/plain").expect("put");
        assert_eq!(a.size, 14);
        assert_eq!(a.id.len(), 64);
        assert!(is_valid_id(&a.id));
        assert_eq!(
            store.get(&a.id).expect("get"),
            Some(b"hello artifact".to_vec())
        );
        let b = store.put(b"hello artifact", "text/plain").expect("put2");
        assert_eq!(a.id, b.id);
        assert_eq!(store.stats().expect("stats").blobs, 1);
        assert_eq!(store.get(&"0".repeat(64)).expect("missing"), None);
    }

    #[test]
    fn ids_are_validated_before_any_path_join() {
        let store = store();
        for id in ["../etc/passwd", "a/b", "", &"Z".repeat(64)] {
            assert!(matches!(store.get(id), Err(ArtifactError::InvalidId(_))));
        }
    }

    #[test]
    fn window_reads_clamp_offset_and_bound_length() {
        let store = store();
        let blob = store
            .put(b"0123456789", "application/octet-stream")
            .expect("put");
        let window = store
            .read_window(&blob.id, 3, 4)
            .expect("read")
            .expect("present");
        assert_eq!(window.offset, 3);
        assert_eq!(window.size, 10);
        assert_eq!(window.content, b"3456".to_vec());
        let past_end = store
            .read_window(&blob.id, 100, 4)
            .expect("read")
            .expect("present");
        assert_eq!(past_end.offset, 10);
        assert!(past_end.content.is_empty());
    }

    #[test]
    fn gc_honors_retention_then_the_size_cap() {
        let store = store();
        let old = store.put(b"old payload", "text/plain").expect("put old");
        let fresh = store
            .put(b"fresh payload", "text/plain")
            .expect("put fresh");
        let path = store.path_for(&old.id).expect("path");
        let old_mtime = SystemTime::now() - std::time::Duration::from_secs(90 * 24 * 3600);
        let file = fs::File::options()
            .write(true)
            .open(&path)
            .expect("open blob");
        file.set_modified(old_mtime).expect("set mtime");
        drop(file);

        let report = store
            .gc(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock")
                    .as_millis() as u64,
                0,
                30 * 24 * 3600 * 1000,
            )
            .expect("gc");
        assert_eq!(report.removed, 1);
        assert_eq!(report.kept, 1);
        assert!(store.get(&old.id).expect("get").is_none());
        assert!(store.get(&fresh.id).expect("get").is_some());

        let report = store.gc(0, 1, 0).expect("gc cap");
        assert_eq!(report.removed, 1);
        assert_eq!(store.stats().expect("stats").blobs, 0);
    }
}
