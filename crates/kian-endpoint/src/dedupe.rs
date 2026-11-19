//! Bounded, durable result retention on the endpoint (ADR-037).
//!
//! The server's durable queue is at-least-once (ADR-007): a crash or a
//! transport loss may re-dispatch a command whose result was never
//! acknowledged. For most commands a replay costs one extra execution;
//! for a command whose side effect **is** the ack path — `systemctl
//! restart kian`, `pkill kiand` — the replay repeats the damage, and
//! ADR-033's bound only stops it after the damage happened.
//!
//! This module closes that window on the endpoint side: each result frame
//! is recorded by command id **before** it is sent, and a re-dispatched
//! id is answered from the record instead of being executed again. A
//! duplicate that arrives while the first execution is still running
//! subscribes to it (one execution, one result). A cache miss, an
//! oversized result, a corrupt entry or a disabled cache all mean
//! *execute again* — at-least-once is unchanged and this module never
//! fabricates a result.
//!
//! The record is bounded by TTL, entry count, entry size and file budget;
//! it carries no identity beyond the command id (brain–hand separation:
//! it can only answer a command this endpoint already ran, never choose
//! the next command).

use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::proto::{CommandId, EndpointFrame};

/// How long a recorded result stays answerable.
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// How many results are kept (oldest evicted first).
pub const DEFAULT_MAX_ENTRIES: usize = 512;
/// Results larger than this are not recorded (a miss re-executes).
pub const DEFAULT_MAX_ENTRY_BYTES: usize = 256 * 1024;
/// On-disk budget; reaching it triggers a compacted rewrite.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Tuning for [`ResultCache`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheOptions {
    pub ttl: Duration,
    pub max_entries: usize,
    pub max_entry_bytes: usize,
    pub max_file_bytes: u64,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            max_entries: DEFAULT_MAX_ENTRIES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }
}

/// What a dispatch of `command_id` should do.
pub enum Claim {
    /// The endpoint already produced this result: answer with it, do not
    /// execute.
    Cached(EndpointFrame),
    /// The same command is executing right now: receive its frame when it
    /// lands. A closed channel (the owner vanished without a result) means
    /// the duplicate is dropped — the row stays re-drivable server-side.
    Follower(broadcast::Receiver<EndpointFrame>),
    /// This dispatch owns the execution. Recording the frame settles the
    /// claim; dropping the guard without recording releases any followers.
    Owner(OwnerGuard),
}

/// Proof that this dispatch owns one execution. Dropping it without
/// [`OwnerGuard::disarm`] (i.e. without [`ResultCache::record`]) releases
/// followers so they are not left waiting behind a vanished owner.
pub struct OwnerGuard {
    cache: Arc<ResultCache>,
    id: CommandId,
    armed: bool,
}

impl OwnerGuard {
    /// Marks the execution as settled by a recorded result.
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OwnerGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cache.abandon(self.id);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    v: u8,
    id: Uuid,
    at: u64,
    frame: EndpointFrame,
}

struct Entry {
    at_ms: u64,
    frame: EndpointFrame,
}

struct State {
    entries: HashMap<CommandId, Entry>,
    order: VecDeque<CommandId>,
    running: HashMap<CommandId, broadcast::Sender<EndpointFrame>>,
    file_bytes: u64,
}

impl State {
    fn evict(&mut self, max_entries: usize) {
        while self.entries.len() > max_entries {
            match self.order.pop_front() {
                Some(id) => {
                    self.entries.remove(&id);
                }
                None => break,
            }
        }
    }
}

/// The endpoint's recorded results (ADR-037). Cheap to share; one per
/// process so every connection sees the same records.
pub struct ResultCache {
    path: PathBuf,
    options: CacheOptions,
    state: Mutex<State>,
}

impl ResultCache {
    /// Load (or create) the cache at `path`, dropping expired, oversized
    /// and corrupt records and rewriting the file compacted. An unreadable
    /// file is not an error: it is treated as an empty cache (a miss
    /// re-executes) and the load still succeeds.
    pub fn load(path: impl Into<PathBuf>, options: CacheOptions) -> Arc<Self> {
        let path = path.into();
        let now = now_ms();
        let mut state = State {
            entries: HashMap::new(),
            order: VecDeque::new(),
            running: HashMap::new(),
            file_bytes: 0,
        };
        if let Ok(text) = std::fs::read_to_string(&path) {
            for line in text.lines() {
                if line.trim().is_empty() || line.len() > options.max_entry_bytes {
                    continue;
                }
                let record: Record = match serde_json::from_str(line) {
                    Ok(record) => record,
                    Err(_) => continue, // corrupt line: skip, never fail
                };
                if now.saturating_sub(record.at) > options.ttl.as_millis() as u64 {
                    continue; // expired
                }
                state.entries.insert(
                    record.id,
                    Entry {
                        at_ms: record.at,
                        frame: record.frame,
                    },
                );
                state.order.push_back(record.id);
            }
        }
        state.evict(options.max_entries);
        let cache = Arc::new(Self {
            path,
            options,
            state: Mutex::new(state),
        });
        // Compact before serving: reclaims the space of dropped records.
        let records = cache.snapshot();
        write_records(&cache.path, &records);
        cache.state.lock().unwrap().file_bytes = records_bytes(&records);
        cache
    }

    /// Where records live.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Decide what one dispatch should do (see [`Claim`]).
    pub fn claim(self: &Arc<Self>, id: CommandId) -> Claim {
        let now = now_ms();
        let ttl_ms = self.options.ttl.as_millis() as u64;
        let mut state = self.state.lock().unwrap();
        let fresh = state
            .entries
            .get(&id)
            .map(|entry| now.saturating_sub(entry.at_ms) <= ttl_ms);
        match fresh {
            Some(true) => {
                let frame = state.entries[&id].frame.clone();
                return Claim::Cached(frame);
            }
            Some(false) => {
                state.entries.remove(&id);
            }
            None => {}
        }
        if let Some(sender) = state.running.get(&id) {
            return Claim::Follower(sender.subscribe());
        }
        let (sender, _rx) = broadcast::channel(1);
        state.running.insert(id, sender);
        Claim::Owner(OwnerGuard {
            cache: Arc::clone(self),
            id,
            armed: true,
        })
    }

    /// Record one settled frame: store it (when it fits), wake followers,
    /// then persist. Call before sending the frame to the server — a hit
    /// must be answerable the moment the result could have been observed.
    pub async fn record(self: &Arc<Self>, id: CommandId, frame: EndpointFrame) {
        let now = now_ms();
        let serialized = serde_json::to_string(&Record {
            v: 1,
            id,
            at: now,
            frame: frame.clone(),
        });
        let line = match serialized {
            Ok(line) if line.len() <= self.options.max_entry_bytes => line,
            Ok(line) => {
                tracing::debug!(
                    command_id = %id,
                    bytes = line.len(),
                    "result too large to record; a re-dispatch will execute again"
                );
                String::new()
            }
            Err(e) => {
                tracing::warn!(command_id = %id, error = %e, "result could not be serialized");
                String::new()
            }
        };
        let sender = {
            let mut state = self.state.lock().unwrap();
            let sender = state.running.remove(&id);
            if !line.is_empty() {
                state.entries.insert(
                    id,
                    Entry {
                        at_ms: now,
                        frame: frame.clone(),
                    },
                );
                state.order.push_back(id);
                state.evict(self.options.max_entries);
            }
            sender
        };
        if let Some(sender) = sender {
            // No receivers is the normal case (the dispatch had no duplicate).
            let _ = sender.send(frame);
        }
        if !line.is_empty() {
            self.append(line).await;
        }
    }

    /// Release followers of an execution that produced no frame.
    fn abandon(&self, id: CommandId) {
        self.state.lock().unwrap().running.remove(&id);
    }

    /// Append one record, compacting when the file budget is exceeded.
    async fn append(self: &Arc<Self>, line: String) {
        let path = self.path.clone();
        let added = line.len() as u64 + 1;
        let grew = {
            let mut state = self.state.lock().unwrap();
            state.file_bytes = state.file_bytes.saturating_add(added);
            state.file_bytes
        };
        let appended = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = open_private(&path, true)?;
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
            file.flush()
        })
        .await;
        match appended {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "result cache append failed; memory-only"),
            Err(e) => tracing::warn!(error = %e, "result cache append task failed"),
        }
        if grew > self.options.max_file_bytes {
            let path = self.path.clone();
            let records = self.snapshot();
            let bytes = records_bytes(&records);
            let _ = tokio::task::spawn_blocking(move || write_records(&path, &records)).await;
            self.state.lock().unwrap().file_bytes = bytes;
        }
    }

    fn snapshot(&self) -> Vec<Record> {
        let state = self.state.lock().unwrap();
        state
            .entries
            .iter()
            .map(|(id, entry)| Record {
                v: 1,
                id: *id,
                at: entry.at_ms,
                frame: entry.frame.clone(),
            })
            .collect()
    }
}

fn records_bytes(records: &[Record]) -> u64 {
    records
        .iter()
        .filter_map(|record| serde_json::to_string(record).ok())
        .map(|line| line.len() as u64 + 1)
        .sum()
}

/// Rewrite the file from `records` (best effort: a failure leaves the
/// current file intact because the replacement is atomic).
fn write_records(path: &Path, records: &[Record]) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("jsonl.tmp");
    let write = || -> std::io::Result<()> {
        let mut file = open_private(&tmp, false)?;
        for record in records {
            let line =
                serde_json::to_string(record).map_err(|e| std::io::Error::other(e.to_string()))?;
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        std::fs::rename(&tmp, path)
    };
    if let Err(e) = write() {
        tracing::warn!(error = %e, "result cache compaction failed");
    }
}

/// Open the cache file so only its owner can read it (it holds command
/// output, exactly as the in-flight result does).
fn open_private(path: &Path, append: bool) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    if append {
        options.append(true).create(true);
    } else {
        options.create(true).truncate(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::seams::ExecResult;

    fn frame(id: CommandId, payload: &str) -> EndpointFrame {
        EndpointFrame::command_ok(
            id,
            ExecResult {
                exit_code: Some(0),
                stdout: payload.into(),
                stderr: String::new(),
                timed_out: false,
                redactions: Vec::new(),
            },
        )
    }

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kian-dedupe-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn cache(path: &Path, options: CacheOptions) -> Arc<ResultCache> {
        ResultCache::load(path, options)
    }

    #[tokio::test]
    async fn recorded_result_is_served_without_executing() {
        let path = temp_path("results.jsonl");
        let cache = cache(&path, CacheOptions::default());
        let id = Uuid::new_v4();
        let Claim::Owner(mut guard) = cache.claim(id) else {
            panic!("first dispatch must own the execution");
        };
        let recorded = frame(id, "done");
        cache.record(id, recorded.clone()).await;
        guard.disarm();

        match cache.claim(id) {
            Claim::Cached(served) => assert_eq!(served, recorded),
            _ => panic!("a recorded id must be answered from the record"),
        }
    }

    #[tokio::test]
    async fn duplicate_dispatch_while_running_follows_the_owner() {
        let path = temp_path("results.jsonl");
        let cache = cache(&path, CacheOptions::default());
        let id = Uuid::new_v4();
        let Claim::Owner(mut guard) = cache.claim(id) else {
            panic!("first dispatch must own the execution");
        };
        let mut follower = match cache.claim(id) {
            Claim::Follower(rx) => rx,
            _ => panic!("a running id must join the execution, never re-run it"),
        };
        let recorded = frame(id, "done");
        cache.record(id, recorded.clone()).await;
        guard.disarm();
        assert_eq!(follower.recv().await.unwrap(), recorded);
    }

    #[tokio::test]
    async fn vanished_owner_releases_followers() {
        let path = temp_path("results.jsonl");
        let cache = cache(&path, CacheOptions::default());
        let id = Uuid::new_v4();
        let Claim::Owner(guard) = cache.claim(id) else {
            panic!("first dispatch must own the execution");
        };
        let mut follower = match cache.claim(id) {
            Claim::Follower(rx) => rx,
            _ => panic!("a running id must join the execution"),
        };
        drop(guard); // no frame: the owner vanished
        assert!(
            follower.recv().await.is_err(),
            "a follower must be released, never left waiting forever"
        );
        assert!(
            matches!(cache.claim(id), Claim::Owner(_)),
            "the id is re-drivable after a vanished owner"
        );
    }

    #[tokio::test]
    async fn cache_survives_a_reload() {
        let path = temp_path("results.jsonl");
        let id = Uuid::new_v4();
        let recorded = frame(id, "durable");
        {
            let cache = cache(&path, CacheOptions::default());
            let Claim::Owner(mut guard) = cache.claim(id) else {
                panic!("first dispatch must own the execution");
            };
            cache.record(id, recorded.clone()).await;
            guard.disarm();
        }
        let reloaded = cache(&path, CacheOptions::default());
        match reloaded.claim(id) {
            Claim::Cached(served) => assert_eq!(served, recorded),
            _ => panic!("a recorded id must survive a worker restart"),
        }
    }

    #[tokio::test]
    async fn oversized_results_are_not_recorded() {
        let path = temp_path("results.jsonl");
        let options = CacheOptions {
            max_entry_bytes: 32,
            ..CacheOptions::default()
        };
        let cache = cache(&path, options);
        let id = Uuid::new_v4();
        let Claim::Owner(mut guard) = cache.claim(id) else {
            panic!("first dispatch must own the execution");
        };
        cache.record(id, frame(id, &"x".repeat(1024))).await;
        guard.disarm();
        assert!(
            matches!(cache.claim(id), Claim::Owner(_)),
            "an oversized result must re-execute rather than be cached wrongly"
        );
    }

    #[tokio::test]
    async fn expired_records_are_dropped_on_load() {
        let path = temp_path("results.jsonl");
        let id = Uuid::new_v4();
        let record = Record {
            v: 1,
            id,
            at: now_ms().saturating_sub(60_000),
            frame: frame(id, "stale"),
        };
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .unwrap();
        let cache = cache(
            &path,
            CacheOptions {
                ttl: Duration::from_secs(1),
                ..CacheOptions::default()
            },
        );
        assert!(
            matches!(cache.claim(id), Claim::Owner(_)),
            "an expired record must not answer a dispatch"
        );
    }

    #[tokio::test]
    async fn corrupt_lines_are_ignored() {
        let path = temp_path("results.jsonl");
        std::fs::write(&path, "not json\n{\"v\":1}\n").unwrap();
        let cache = cache(&path, CacheOptions::default());
        let id = Uuid::new_v4();
        assert!(
            matches!(cache.claim(id), Claim::Owner(_)),
            "a corrupt cache must degrade to a miss, never fail"
        );
    }

    #[tokio::test]
    async fn oldest_entries_are_evicted() {
        let path = temp_path("results.jsonl");
        let cache = cache(
            &path,
            CacheOptions {
                max_entries: 1,
                ..CacheOptions::default()
            },
        );
        let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
        for id in [first, second] {
            let Claim::Owner(mut guard) = cache.claim(id) else {
                panic!("a fresh id must own its execution");
            };
            cache.record(id, frame(id, "done")).await;
            guard.disarm();
        }
        assert!(
            matches!(cache.claim(first), Claim::Owner(_)),
            "the oldest record is evicted once the entry bound is reached"
        );
        assert!(matches!(cache.claim(second), Claim::Cached(_)));
    }
}
