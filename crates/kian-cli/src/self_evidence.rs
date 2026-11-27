//! `kian self evidence` — one symptom, one handoff package (SPEC §3.1).
//!
//! `kian self inspect` is the aggregate view and prints no payloads; a
//! diagnosis then needs the rows themselves. This command names one anchor
//! (`--session`, `--seq`), slices that session's stream `k` rows either
//! side and writes a directory a reviewer — human or agent — can read without
//! the store:
//!
//! - `events.ndjson` — the raw envelopes of the slice, the same bytes
//!   `kian session export` writes;
//! - `session.json` — identity, the projected state and the end writes;
//! - `repo.json` — the checkout the rows came from: commit, branch, dirty
//!   flag and the crate map;
//! - `inspect.json` — the `self inspect` projection over the session's whole
//!   stream, so the pack and the aggregate surface cannot drift;
//! - `manifest.json` — store/config/repo identity plus the sha256 of every
//!   file, written last.
//!
//! Two rules keep this honest. Payload text is the point of a pack, so it is
//! written only under an explicit `--out` and never to stdout: the terminal
//! gets a summary, `--json` gets the manifest. And a pack is either complete
//! or absent — the files are built in a temp sibling, verified by
//! [`verify_pack`], then renamed into place in one step; a refusal (no such
//! seq, a non-empty target) leaves nothing behind.

use std::path::{Path, PathBuf};

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::commands::{self, CmdResult};
use crate::config::ServeConfig;
use crate::self_inspect;

/// Default `--context`: ten rows either side of the anchor.
pub const DEFAULT_CONTEXT: u64 = 10;

/// The widest slice one pack carries. A wider request is refused rather than
/// narrowed — a silently truncated slice is a pack that lies about its span.
pub const MAX_CONTEXT: u64 = 200;

/// The manifest schema version this build writes.
pub const FORMAT_VERSION: u32 = 1;

/// The package family: retention writes `kian-session-archive`, an operator
/// (or an agent) writes this one.
pub const PACK_KIND: &str = "kian-evidence";

/// The payload files, in manifest order. The aggregate digest folds the
/// per-file digests in exactly this order, so the order is schema, not style.
pub const PAYLOAD_FILES: [&str; 4] = ["events.ndjson", "session.json", "repo.json", "inspect.json"];

pub struct Options<'a> {
    pub config_path: &'a Path,
    pub db: Option<&'a Path>,
    pub session: SessionId,
    pub seq: u64,
    pub context: u64,
    pub out: &'a Path,
}

/// The package manifest — the one file a reader opens first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub kind: String,
    pub kian_version: String,
    pub created_at_ms: u64,
    pub anchor: Anchor,
    pub context: u64,
    pub range: Span,
    pub session_event_count: u64,
    pub store: StoreRef,
    pub config: ConfigRef,
    pub repo: RepoRef,
    pub files: Vec<FileRef>,
    pub files_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anchor {
    pub session: String,
    pub seq: u64,
    pub kind: String,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub first_seq: u64,
    pub last_seq: u64,
    pub event_count: u64,
    pub first_ts_ms: u64,
    pub last_ts_ms: u64,
}

/// The store the rows were read from. The database file is never hashed: a
/// live sqlite has a WAL and a page cache behind it, so a digest would be a
/// coin flip rather than an identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRef {
    pub backend: String,
    pub db_path: String,
    pub db_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigRef {
    pub path: String,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoRef {
    pub root: Option<String>,
    pub commit: Option<String>,
    pub branch: Option<String>,
    pub dirty: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRef {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Serialize)]
struct SessionDoc {
    session: String,
    state: String,
    state_error: Option<String>,
    event_count: u64,
    first_seq: u64,
    last_seq: u64,
    first_ts_ms: u64,
    last_ts_ms: u64,
    epoch: u64,
    ends: Vec<EndRef>,
    end_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct EndRef {
    seq: u64,
    ts_ms: u64,
    reason: String,
}

#[derive(Debug, Serialize)]
struct RepoDoc {
    root: Option<String>,
    commit: Option<String>,
    branch: Option<String>,
    dirty: Option<bool>,
    crates: Option<Vec<CrateRef>>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct CrateRef {
    name: String,
    path: String,
}

/// `kian self evidence` — writes the package, returns the summary.
pub async fn run(opts: Options<'_>, json_out: bool) -> CmdResult<String> {
    if opts.context > MAX_CONTEXT {
        return Err(format!(
            "--context {} is wider than the {MAX_CONTEXT}-row bound",
            opts.context
        ));
    }
    refuse_occupied(opts.out)?;

    let db = commands::resolve_db(opts.db, opts.config_path)?;
    let store = crate::commands::open_reader(&db).await?;
    let events = store
        .rebuild(opts.session)
        .await
        .map_err(|e| format!("read events for {}: {e}", opts.session.0))?;
    if events.is_empty() {
        return Err(format!(
            "unknown session {} (no events in this store)",
            opts.session.0
        ));
    }
    let anchor = events
        .iter()
        .position(|e| e.seq == opts.seq)
        .ok_or_else(|| missing_seq(&opts, &events))?;
    let context = usize::try_from(opts.context).unwrap_or(usize::MAX);
    let (lo, hi) = slice_bounds(events.len(), anchor, context);
    let slab = &events[lo..=hi];

    let cfg = ServeConfig::load(opts.config_path).ok();
    let facts = self_inspect::facts_for(
        &store,
        &db,
        opts.config_path,
        cfg.as_ref(),
        Some(opts.session),
        events.first().map(|e| e.ts).unwrap_or(0),
        events.last().map(|e| e.ts).unwrap_or(0),
    )
    .await;
    store.close().await;

    let report = self_inspect::build_report(&facts, &events, &self_inspect::kind_counts(&events));
    let events_ndjson = crate::archive::export_bytes(slab)?;
    let session_bytes = pretty(&session_doc(&opts.session, &events))?;
    let repo_doc = repo_doc();
    let repo_bytes = pretty(&repo_doc)?;
    let inspect_bytes = pretty(&report)?;
    let payloads: [(&str, &[u8]); 4] = [
        (PAYLOAD_FILES[0], events_ndjson.as_bytes()),
        (PAYLOAD_FILES[1], session_bytes.as_slice()),
        (PAYLOAD_FILES[2], repo_bytes.as_slice()),
        (PAYLOAD_FILES[3], inspect_bytes.as_slice()),
    ];
    let (files, files_sha256) = file_refs(&payloads);
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        kind: PACK_KIND.to_string(),
        kian_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ms: now_ms(),
        anchor: Anchor {
            session: opts.session.0.to_string(),
            seq: opts.seq,
            kind: slab[anchor - lo].kind.as_str().to_string(),
            ts_ms: slab[anchor - lo].ts,
        },
        context: opts.context,
        range: Span {
            first_seq: slab.first().map(|e| e.seq).unwrap_or(0),
            last_seq: slab.last().map(|e| e.seq).unwrap_or(0),
            event_count: slab.len() as u64,
            first_ts_ms: slab.first().map(|e| e.ts).unwrap_or(0),
            last_ts_ms: slab.last().map(|e| e.ts).unwrap_or(0),
        },
        session_event_count: events.len() as u64,
        store: StoreRef {
            backend: "sqlite".to_string(),
            db_path: db.display().to_string(),
            db_bytes: std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0),
        },
        config: ConfigRef {
            path: opts.config_path.display().to_string(),
            sha256: std::fs::read(opts.config_path)
                .ok()
                .map(|bytes| crate::archive::sha256_hex(&bytes)),
        },
        repo: RepoRef {
            root: repo_doc.root.clone(),
            commit: repo_doc.commit.clone(),
            branch: repo_doc.branch.clone(),
            dirty: repo_doc.dirty,
        },
        files,
        files_sha256,
    };
    write_pack(opts.out, &payloads, &manifest)?;

    if json_out {
        return serde_json::to_string_pretty(&manifest).map_err(|e| e.to_string());
    }
    Ok(render(&manifest, opts.out))
}

/// The `--out` contract: a missing path is fine, an empty directory is fine
/// (it is filled in one rename), anything else is someone else's data.
fn refuse_occupied(out: &Path) -> Result<(), String> {
    match std::fs::metadata(out) {
        Ok(meta) if meta.is_dir() => {
            let mut entries =
                std::fs::read_dir(out).map_err(|e| format!("read {}: {e}", out.display()))?;
            if entries.next().is_some() {
                return Err(format!(
                    "--out {} is a non-empty directory (refusing to overwrite)",
                    out.display()
                ));
            }
            Ok(())
        }
        Ok(_) => Err(format!(
            "--out {} exists and is not a directory",
            out.display()
        )),
        Err(_) => Ok(()),
    }
}

fn missing_seq(opts: &Options<'_>, events: &[Event]) -> String {
    format!(
        "session {} has no event at seq {} (the stream covers {}..{})",
        opts.session.0,
        opts.seq,
        events.first().map(|e| e.seq).unwrap_or(0),
        events.last().map(|e| e.seq).unwrap_or(0)
    )
}

/// The inclusive slice bounds around `anchor`, clamped to the stream.
fn slice_bounds(len: usize, anchor: usize, context: usize) -> (usize, usize) {
    let last = len.saturating_sub(1);
    (
        anchor.saturating_sub(context),
        anchor.saturating_add(context).min(last),
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn pretty<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec_pretty(value).map_err(|e| format!("serialize package file: {e}"))
}

fn file_refs(payloads: &[(&str, &[u8])]) -> (Vec<FileRef>, String) {
    let files: Vec<FileRef> = payloads
        .iter()
        .map(|(name, bytes)| FileRef {
            name: (*name).to_string(),
            bytes: bytes.len() as u64,
            sha256: crate::archive::sha256_hex(bytes),
        })
        .collect();
    let digests: Vec<&str> = files.iter().map(|f| f.sha256.as_str()).collect();
    let aggregate = crate::archive::sha256_hex(digests.join("\n").as_bytes());
    (files, aggregate)
}

fn session_doc(session: &SessionId, events: &[Event]) -> SessionDoc {
    let mut ends = Vec::new();
    for event in events {
        if let EventKind::SessionEnded { reason } = &event.kind {
            ends.push(EndRef {
                seq: event.seq,
                ts_ms: event.ts,
                reason: self_inspect::end_reason_str(reason).to_string(),
            });
        }
    }
    let (state, state_error) = match kian_core::state::project(events) {
        Ok(state) => (
            serde_json::to_value(state)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_string()),
            None,
        ),
        // A stream the transition table rejects is still the evidence for the
        // corruption: the pack records the rejection instead of not existing.
        Err(error) => ("unknown".to_string(), Some(error.to_string())),
    };
    SessionDoc {
        session: session.0.to_string(),
        state,
        state_error,
        event_count: events.len() as u64,
        first_seq: events.first().map(|e| e.seq).unwrap_or(0),
        last_seq: events.last().map(|e| e.seq).unwrap_or(0),
        first_ts_ms: events.first().map(|e| e.ts).unwrap_or(0),
        last_ts_ms: events.last().map(|e| e.ts).unwrap_or(0),
        epoch: events.last().map(|e| e.epoch.0).unwrap_or(0),
        end_reason: ends.last().map(|e| e.reason.clone()),
        ends,
    }
}

fn repo_doc() -> RepoDoc {
    let Ok(root) = run_git(None, &["rev-parse", "--show-toplevel"]) else {
        return RepoDoc {
            root: None,
            commit: None,
            branch: None,
            dirty: None,
            crates: None,
            error: Some("not a git checkout (git rev-parse --show-toplevel failed)".to_string()),
        };
    };
    let root = root.trim().to_string();
    if root.is_empty() {
        return RepoDoc {
            root: None,
            commit: None,
            branch: None,
            dirty: None,
            crates: None,
            error: Some("git reported no worktree root".to_string()),
        };
    }
    let at = Path::new(&root);
    let (crates, error) = crate_map(at);
    RepoDoc {
        commit: run_git(Some(at), &["rev-parse", "HEAD"])
            .ok()
            .map(|s| s.trim().to_string()),
        branch: run_git(Some(at), &["rev-parse", "--abbrev-ref", "HEAD"])
            .ok()
            .map(|s| s.trim().to_string()),
        dirty: run_git(Some(at), &["status", "--porcelain"])
            .ok()
            .map(|s| !s.trim().is_empty()),
        crates,
        error,
        root: Some(root),
    }
}

/// One git invocation with the inherited repository environment scrubbed
/// (`cron_worktree::git_command`): a nested git that inherits `GIT_DIR`
/// writes to the caller's checkout instead of the one named here.
fn run_git(repo: Option<&Path>, args: &[&str]) -> Result<String, String> {
    let mut command = crate::cron_worktree::git_command();
    if let Some(repo) = repo {
        command.arg("-C").arg(repo);
    }
    let output = command
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("git {}: {e}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The workspace's member crates as `name → path`. No manifest-parser
/// dependency: the members are a quoted list, and each crate's `[package]`
/// table comes before every other table, so its first `name` line is the
/// package name. A parse that cannot complete returns `None` plus the reason —
/// a silently empty map would read as "this workspace has no crates".
fn crate_map(root: &Path) -> (Option<Vec<CrateRef>>, Option<String>) {
    let manifest = root.join("Cargo.toml");
    let members = match std::fs::read_to_string(&manifest) {
        Ok(text) => match workspace_members(&text) {
            Some(members) => members,
            None => {
                return (
                    None,
                    Some(format!("no members list in {}", manifest.display())),
                );
            }
        },
        Err(e) => {
            return (None, Some(format!("read {}: {e}", manifest.display())));
        }
    };
    let mut crates = Vec::with_capacity(members.len());
    for member in members {
        let path = root.join(&member).join("Cargo.toml");
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) => return (None, Some(format!("read {}: {e}", path.display()))),
        };
        match package_name(&text) {
            Some(name) => crates.push(CrateRef { name, path: member }),
            None => {
                return (None, Some(format!("no package name in {}", path.display())));
            }
        }
    }
    (Some(crates), None)
}

fn workspace_members(text: &str) -> Option<Vec<String>> {
    let start = text.find("members")?;
    let rest = &text[start..];
    let open = rest.find('[')?;
    let close = rest[open..].find(']')?;
    let members: Vec<String> = rest[open + 1..open + close]
        .split('"')
        .skip(1)
        .step_by(2)
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .collect();
    if members.is_empty() {
        None
    } else {
        Some(members)
    }
}

fn package_name(text: &str) -> Option<String> {
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("name")
            && let Some(rest) = rest.trim_start().strip_prefix('=')
        {
            let rest = rest.trim_start();
            if let Some(value) = rest.strip_prefix('"').and_then(|v| v.split('"').next()) {
                return Some(value.to_string());
            }
        }
        if line.starts_with('[') && !line.starts_with("[package]") {
            return None;
        }
    }
    None
}

/// Writes the payloads plus the manifest into a temp sibling, verifies it, then
/// renames it into place. A failure removes the temp directory: a pack is
/// complete or absent, never half-written.
fn write_pack(out: &Path, payloads: &[(&str, &[u8])], manifest: &Manifest) -> Result<(), String> {
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    let tmp = tmp_sibling(out);
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).map_err(|e| format!("mkdir {}: {e}", tmp.display()))?;

    let build = || -> Result<(), String> {
        for (name, bytes) in payloads {
            std::fs::write(tmp.join(name), bytes).map_err(|e| format!("write {name}: {e}"))?;
        }
        let manifest_bytes =
            serde_json::to_vec_pretty(manifest).map_err(|e| format!("serialize manifest: {e}"))?;
        std::fs::write(tmp.join("manifest.json"), &manifest_bytes)
            .map_err(|e| format!("write manifest.json: {e}"))?;
        verify_pack(&tmp).map(|_| ())
    };
    if let Err(e) = build() {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(e);
    }
    if out.exists() {
        std::fs::remove_dir(out).map_err(|e| format!("replace {}: {e}", out.display()))?;
    }
    std::fs::rename(&tmp, out).map_err(|e| {
        let _ = std::fs::remove_dir_all(&tmp);
        format!("rename into {}: {e}", out.display())
    })
}

fn tmp_sibling(out: &Path) -> PathBuf {
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = out
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "evidence".to_string());
    parent.join(format!(".{name}.tmp-{}-{}", std::process::id(), now_ms()))
}

/// Re-reads a package from disk and re-derives every digest: the manifest's
/// `files`, the aggregate over them, the byte counts, and the fact that every
/// payload parses. The writer runs this before its rename, so a payload that
/// fails here was written by something other than this build.
pub fn verify_pack(dir: &Path) -> Result<Manifest, String> {
    let manifest_bytes = std::fs::read(dir.join("manifest.json"))
        .map_err(|e| format!("read {}: {e}", dir.join("manifest.json").display()))?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).map_err(|e| format!("parse manifest.json: {e}"))?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(format!(
            "manifest format_version {} (this build writes {FORMAT_VERSION})",
            manifest.format_version
        ));
    }
    if manifest.kind != PACK_KIND {
        return Err(format!(
            "manifest kind `{}` is not `{PACK_KIND}`",
            manifest.kind
        ));
    }
    for name in PAYLOAD_FILES {
        if !manifest.files.iter().any(|f| f.name == name) {
            return Err(format!("manifest does not list {name}"));
        }
    }
    let mut digests = Vec::with_capacity(manifest.files.len());
    for file in &manifest.files {
        if file.name.contains('/') || file.name.contains("..") {
            return Err(format!("manifest name `{}` escapes the package", file.name));
        }
        let bytes =
            std::fs::read(dir.join(&file.name)).map_err(|e| format!("read {}: {e}", file.name))?;
        if bytes.len() as u64 != file.bytes {
            return Err(format!(
                "{}: {} byte(s) on disk, manifest says {}",
                file.name,
                bytes.len(),
                file.bytes
            ));
        }
        let digest = crate::archive::sha256_hex(&bytes);
        if digest != file.sha256 {
            return Err(format!(
                "{}: sha256 mismatch (manifest {}, disk {digest})",
                file.name, file.sha256
            ));
        }
        digests.push(digest);
    }
    let aggregate = crate::archive::sha256_hex(digests.join("\n").as_bytes());
    if aggregate != manifest.files_sha256 {
        return Err(format!(
            "files_sha256 mismatch (manifest {}, recomputed {aggregate})",
            manifest.files_sha256
        ));
    }
    let events = std::fs::read_to_string(dir.join(PAYLOAD_FILES[0]))
        .map_err(|e| format!("read {}: {e}", PAYLOAD_FILES[0]))?;
    for (index, line) in events.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        serde_json::from_str::<Event>(line)
            .map_err(|e| format!("events.ndjson line {}: {e}", index + 1))?;
    }
    for name in &PAYLOAD_FILES[1..] {
        let raw =
            std::fs::read_to_string(dir.join(name)).map_err(|e| format!("read {name}: {e}"))?;
        serde_json::from_str::<Value>(&raw).map_err(|e| format!("parse {name}: {e}"))?;
    }
    Ok(manifest)
}

fn render(manifest: &Manifest, out: &Path) -> String {
    let mut text = String::new();
    text.push_str(&format!("package     {}\n", out.display()));
    text.push_str(&format!(
        "anchor      {} seq {} ({}) at {}\n",
        manifest.anchor.session,
        manifest.anchor.seq,
        manifest.anchor.kind,
        self_inspect::iso(manifest.anchor.ts_ms)
    ));
    text.push_str(&format!(
        "events      {} row(s), seq {}..{} ({} .. {})\n",
        manifest.range.event_count,
        manifest.range.first_seq,
        manifest.range.last_seq,
        self_inspect::iso(manifest.range.first_ts_ms),
        self_inspect::iso(manifest.range.last_ts_ms)
    ));
    text.push_str(&format!(
        "stream      {} event(s) in the session\n",
        manifest.session_event_count
    ));
    text.push_str(&format!("files       {}\n", PAYLOAD_FILES.join(", ")));
    text.push_str(&format!("digest      {}\n", manifest.files_sha256));
    text.push_str(&format!("repo        {}\n", repo_line(&manifest.repo)));
    text.push_str(&format!(
        "store       {} ({})\n",
        manifest.store.db_path,
        self_inspect::human_bytes(manifest.store.db_bytes)
    ));
    text.push_str("payloads    in the package only (never on stdout)\n");
    text
}

fn repo_line(repo: &RepoRef) -> String {
    let Some(root) = &repo.root else {
        return "not a git checkout".to_string();
    };
    let branch = repo.branch.as_deref().unwrap_or("?");
    let commit: String = repo
        .commit
        .as_deref()
        .map(|c| c.chars().take(12).collect())
        .unwrap_or_else(|| "?".to_string());
    let state = match repo.dirty {
        Some(true) => "dirty",
        Some(false) => "clean",
        None => "unknown",
    };
    format!("{branch} @ {commit} ({state}) at {root}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::Epoch;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = std::env::temp_dir().join(format!(
                "kian-evidence-{tag}-{}-{stamp}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn event_line(seq: u64) -> String {
        let event = Event {
            seq,
            session: SessionId::new(),
            epoch: Epoch(1),
            ts: 1_700_000_000_000 + seq,
            kind: EventKind::InputAdmitted,
            bytes: serde_json::json!({"text": "payload"}),
            version: SCHEMA_VERSION,
        };
        serde_json::to_string(&event).expect("serialize event")
    }

    fn payloads<'a>(events: &'a str, doc: &'a str) -> [(&'static str, &'a [u8]); 4] {
        [
            (PAYLOAD_FILES[0], events.as_bytes()),
            (PAYLOAD_FILES[1], doc.as_bytes()),
            (PAYLOAD_FILES[2], doc.as_bytes()),
            (PAYLOAD_FILES[3], doc.as_bytes()),
        ]
    }

    fn manifest_for(payloads: &[(&str, &[u8])]) -> Manifest {
        let (files, files_sha256) = file_refs(payloads);
        Manifest {
            format_version: FORMAT_VERSION,
            kind: PACK_KIND.to_string(),
            kian_version: "test".to_string(),
            created_at_ms: 1,
            anchor: Anchor {
                session: "s".to_string(),
                seq: 1,
                kind: "input/admitted".to_string(),
                ts_ms: 2,
            },
            context: 1,
            range: Span {
                first_seq: 1,
                last_seq: 2,
                event_count: 2,
                first_ts_ms: 2,
                last_ts_ms: 3,
            },
            session_event_count: 2,
            store: StoreRef {
                backend: "sqlite".to_string(),
                db_path: "kian.db".to_string(),
                db_bytes: 0,
            },
            config: ConfigRef {
                path: "kian.yaml".to_string(),
                sha256: None,
            },
            repo: RepoRef {
                root: None,
                commit: None,
                branch: None,
                dirty: None,
            },
            files,
            files_sha256,
        }
    }

    #[test]
    fn slice_bounds_clamps_at_both_ends() {
        assert_eq!(slice_bounds(10, 0, 3), (0, 3));
        assert_eq!(slice_bounds(10, 9, 3), (6, 9));
        assert_eq!(slice_bounds(10, 5, 0), (5, 5));
        assert_eq!(slice_bounds(10, 5, usize::MAX), (0, 9));
        assert_eq!(slice_bounds(1, 0, 10), (0, 0));
    }

    #[test]
    fn refuse_occupied_only_takes_a_free_path() {
        let dir = TempDir::new("occupied");
        assert!(refuse_occupied(&dir.path().join("absent")).is_ok());
        let empty = dir.path().join("empty");
        std::fs::create_dir_all(&empty).expect("mkdir");
        assert!(refuse_occupied(&empty).is_ok());
        std::fs::write(empty.join("keep.txt"), b"mine").expect("write");
        let refusal = refuse_occupied(&empty).expect_err("non-empty dir must be refused");
        assert!(refusal.contains("non-empty directory"), "{refusal}");
        let file = dir.path().join("plain.txt");
        std::fs::write(&file, b"mine").expect("write");
        let refusal = refuse_occupied(&file).expect_err("a file must be refused");
        assert!(refusal.contains("not a directory"), "{refusal}");
        assert_eq!(
            std::fs::read_to_string(empty.join("keep.txt")).expect("still there"),
            "mine"
        );
    }

    #[test]
    fn write_then_verify_round_trips() {
        let dir = TempDir::new("round-trip");
        let out = dir.path().join("pack");
        let events = format!("{}\n{}\n", event_line(1), event_line(2));
        let doc = "{\"ok\": true}";
        let payloads = payloads(&events, doc);
        let manifest = manifest_for(&payloads);
        write_pack(&out, &payloads, &manifest).expect("write");
        let verified = verify_pack(&out).expect("verify");
        assert_eq!(verified.files_sha256, manifest.files_sha256);
        assert_eq!(verified.range.event_count, 2);
        assert!(out.join("manifest.json").is_file());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read parent")
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".pack.tmp-")
            })
            .collect();
        assert!(leftovers.is_empty(), "temp directory left behind");
    }

    #[test]
    fn verify_rejects_a_changed_payload_byte() {
        let dir = TempDir::new("tamper");
        let out = dir.path().join("pack");
        let events = format!("{}\n", event_line(1));
        let payloads = payloads(&events, "{}");
        write_pack(&out, &payloads, &manifest_for(&payloads)).expect("write");
        let victim = out.join(PAYLOAD_FILES[1]);
        let bytes = std::fs::read(&victim).expect("read");
        let mut changed = bytes.clone();
        let at = changed
            .iter()
            .position(|b| *b == b'{')
            .expect("json object");
        changed[at] = b'[';
        std::fs::write(&victim, &changed).expect("write");
        assert_eq!(changed.len(), bytes.len());
        let refusal = verify_pack(&out).expect_err("tampered payload must fail");
        assert!(refusal.contains("sha256 mismatch"), "{refusal}");
    }

    #[test]
    fn verify_rejects_a_changed_digest() {
        let dir = TempDir::new("digest");
        let out = dir.path().join("pack");
        let payloads = payloads("", "{}");
        write_pack(&out, &payloads, &manifest_for(&payloads)).expect("write");
        let path = out.join("manifest.json");
        let mut manifest = verify_pack(&out).expect("verify");
        manifest.files_sha256 = "0".repeat(64);
        std::fs::write(&path, serde_json::to_vec_pretty(&manifest).expect("json")).expect("write");
        let refusal = verify_pack(&out).expect_err("tampered digest must fail");
        assert!(refusal.contains("files_sha256 mismatch"), "{refusal}");
    }

    #[test]
    fn workspace_members_reads_the_quoted_list() {
        let text =
            "[workspace]\nresolver = \"3\"\nmembers = [\n  \"crates/a\",\n  \"crates/b\",\n]\n";
        assert_eq!(
            workspace_members(text),
            Some(vec!["crates/a".to_string(), "crates/b".to_string()])
        );
        assert_eq!(workspace_members("[package]\nname = \"x\"\n"), None);
    }

    #[test]
    fn package_name_takes_the_package_table() {
        let text = "[package]\nname = \"kian-cli\"\nversion.workspace = true\n\n[[bin]]\nname = \"kian\"\n";
        assert_eq!(package_name(text), Some("kian-cli".to_string()));
        let virtual_manifest = "[workspace]\nmembers = [\"crates/a\"]\n";
        assert_eq!(package_name(virtual_manifest), None);
    }

    #[test]
    fn crate_map_reads_this_workspace() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|crates| crates.parent())
            .expect("workspace root");
        let (crates, error) = crate_map(root);
        let crates = crates.expect("crate map");
        assert!(error.is_none(), "{error:?}");
        assert!(crates.iter().any(|c| c.name == "kian-cli"), "{crates:?}");
        let text = std::fs::read_to_string(root.join("Cargo.toml")).expect("read workspace");
        assert_eq!(
            crates.len(),
            workspace_members(&text).expect("members").len()
        );
    }
}
