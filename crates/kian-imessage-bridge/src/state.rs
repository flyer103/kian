//! The bridge's disposable read position (the `rowid` cursor).
//!
//! The cursor is **bridge-local transport state**, not kian state
//! (CHANNEL-IMESSAGE §9): kian never reads or writes it, and losing it
//! costs at most a bounded replay (dedupe absorbs duplicates). It lives
//! in `state_dir/cursor.json`, written atomically and debounced — a
//! crash loses at most a few seconds of position, never a message.
//!
//! `generation` identifies the `chat.db` the cursor belongs to. ROWIDs
//! are per database instance; a replaced/restored database resets the
//! generation and the cursor (the ABI reports the change, the adapter
//! resets its own in-memory cursor).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// How long a cursor write may be deferred (a crash replays a few
/// seconds; duplicates are absorbed by `guid` dedupe).
pub const PERSIST_DEBOUNCE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CursorFile {
    version: u32,
    rowid: u64,
    generation: String,
    /// `chat.db`'s inode: a changed inode is a different database.
    #[serde(default)]
    inode: u64,
}

/// The in-memory cursor + its persistence.
pub struct CursorStore {
    path: PathBuf,
    inner: Mutex<CursorState>,
    dirty: AtomicBool,
    last_write: Mutex<Instant>,
}

struct CursorState {
    rowid: u64,
    generation: String,
    inode: u64,
    /// True until the first `observe` (the startup tail bootstrap).
    fresh: bool,
}

impl CursorStore {
    /// Loads (or creates) the cursor for `db`. A different `chat.db`
    /// inode rotates the generation and resets the position.
    #[must_use]
    pub fn load(state_dir: &Path, db: &Path) -> Self {
        let path = state_dir.join("cursor.json");
        let inode = inode_of(db);
        let persisted = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str::<CursorFile>(&text).ok());
        let state = match persisted {
            Some(file) if file.inode == inode && !file.generation.is_empty() => CursorState {
                rowid: file.rowid,
                generation: file.generation,
                inode,
                fresh: false,
            },
            _ => CursorState {
                rowid: 0,
                generation: uuid::Uuid::new_v4().to_string(),
                inode,
                fresh: true,
            },
        };
        Self {
            path,
            inner: Mutex::new(state),
            dirty: AtomicBool::new(false),
            last_write: Mutex::new(Instant::now() - PERSIST_DEBOUNCE),
        }
    }

    /// True while the store has no usable position yet (first boot or a
    /// replaced database): the caller bootstraps the tail.
    #[must_use]
    pub fn fresh(&self) -> bool {
        self.inner.lock().expect("cursor lock").fresh
    }

    #[must_use]
    pub fn generation(&self) -> String {
        self.inner.lock().expect("cursor lock").generation.clone()
    }

    #[must_use]
    pub fn rowid(&self) -> u64 {
        self.inner.lock().expect("cursor lock").rowid
    }

    /// Advances the position (monotonic: a lower rowid never moves the
    /// cursor back) and schedules a debounced persist.
    pub fn observe(&self, rowid: u64) {
        {
            let mut inner = self.inner.lock().expect("cursor lock");
            inner.rowid = inner.rowid.max(rowid);
            inner.fresh = false;
        }
        self.dirty.store(true, Ordering::Relaxed);
        self.maybe_persist();
    }

    /// Rotates the generation (the database was replaced mid-flight) and
    /// resets the position; the next call bootstraps the new tail.
    pub fn rotate_generation(&self) {
        {
            let mut inner = self.inner.lock().expect("cursor lock");
            inner.generation = uuid::Uuid::new_v4().to_string();
            inner.rowid = 0;
            inner.fresh = true;
        }
        self.dirty.store(true, Ordering::Relaxed);
        self.persist_now();
    }

    fn maybe_persist(&self) {
        if !self.dirty.load(Ordering::Relaxed) {
            return;
        }
        let mut last = self.last_write.lock().expect("cursor lock");
        if last.elapsed() < PERSIST_DEBOUNCE {
            return;
        }
        *last = Instant::now();
        drop(last);
        self.persist_now();
    }

    /// Writes the cursor now (atomic temp+rename; a write failure is a
    /// warning, never a reason to stop serving).
    pub fn persist_now(&self) {
        let snapshot = {
            let inner = self.inner.lock().expect("cursor lock");
            CursorFile {
                version: 1,
                rowid: inner.rowid,
                generation: inner.generation.clone(),
                inode: inner.inode,
            }
        };
        self.dirty.store(false, Ordering::Relaxed);
        let Some(parent) = self.path.parent() else {
            return;
        };
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
        let temp = self.path.with_extension("json.tmp");
        let Ok(text) = serde_json::to_string(&snapshot) else {
            return;
        };
        if std::fs::write(&temp, text).is_err() {
            return;
        }
        if std::fs::rename(&temp, &self.path).is_err() {
            let _ = std::fs::remove_file(&temp);
        }
    }
}

/// `chat.db`'s inode (0 when the file does not exist yet).
#[must_use]
pub fn inode_of(path: &Path) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).map(|meta| meta.ino()).unwrap_or(0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kian-imessage-cursor-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        dir
    }

    #[test]
    fn a_fresh_store_bootstraps_and_persists() {
        let dir = dir();
        let db = dir.join("chat.db");
        std::fs::write(&db, b"x").expect("db");
        let store = CursorStore::load(&dir, &db);
        assert!(store.fresh());
        assert_eq!(store.rowid(), 0);
        store.observe(42);
        assert!(!store.fresh());
        assert_eq!(store.rowid(), 42);
        store.persist_now();
        let reloaded = CursorStore::load(&dir, &db);
        assert!(!reloaded.fresh());
        assert_eq!(reloaded.rowid(), 42);
        assert_eq!(reloaded.generation(), store.generation());
    }

    #[test]
    fn a_monotonic_cursor_never_moves_back() {
        let dir = dir();
        let db = dir.join("chat.db");
        std::fs::write(&db, b"x").expect("db");
        let store = CursorStore::load(&dir, &db);
        store.observe(10);
        store.observe(5);
        assert_eq!(store.rowid(), 10);
    }

    #[test]
    fn a_replaced_database_rotates_the_generation() {
        let dir = dir();
        let db = dir.join("chat.db");
        std::fs::write(&db, b"x").expect("db");
        let store = CursorStore::load(&dir, &db);
        store.observe(100);
        store.persist_now();
        let generation = store.generation();
        // A different inode = a different database. Build the replacement while
        // the original still exists, then rename over it: unlink-then-create can
        // reuse the freed inode number on ext4/overlay filesystems (the GitHub
        // runners), which would keep the old generation and fail this assertion
        // for an environment reason rather than a bridge bug.
        let replacement = dir.join("chat.db.replacement");
        std::fs::write(&replacement, b"z").expect("replacement db");
        std::fs::rename(&replacement, &db).expect("replace db");
        let reloaded = CursorStore::load(&dir, &db);
        assert_ne!(reloaded.generation(), generation);
        assert!(reloaded.fresh());
        assert_eq!(reloaded.rowid(), 0);
    }

    #[test]
    fn rotate_generation_resets_the_position() {
        let dir = dir();
        let db = dir.join("chat.db");
        std::fs::write(&db, b"x").expect("db");
        let store = CursorStore::load(&dir, &db);
        store.observe(7);
        store.rotate_generation();
        assert!(store.fresh());
        assert_eq!(store.rowid(), 0);
    }
}
