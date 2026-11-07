//! Export-only retention: the archive package and the `kian archive` /
//! `kian session import` commands (READ-MODEL §7, P2b).
//!
//! A **package** is a directory `<archive_dir>/<session uuid>/`:
//!
//! - `events.jsonl` — one frozen v0 envelope per line, the exact bytes
//!   `kian session export` produces (byte-identical by construction,
//!   so exporting from the package post-archive equals the pre-archive
//!   export — the audit-invariance golden);
//! - `inputs.jsonl` — one full inbox row per line, including the audit
//!   columns (`consumed_by_seq`, `admitted_seq`);
//! - `manifest.json` — the package's identity: format version, session,
//!   event-schema version, first/last seq, counts, per-file sha256, the
//!   aggregate sha256, created_at, store backend, terminal state.
//!
//! The two-phase flow (all idempotent, crash-safe at every window):
//!
//! 1. **write** — build the package from a consistent online read, via
//!    a temp directory + one atomic rename;
//! 2. **verify** — re-read the files, recompute every hash, re-parse
//!    every line, and byte-compare the events file against a fresh
//!    derivation from the stream;
//! 3. **commit** — [`kian_store::EventStore::archive_commit`]: the
//!    archived marker insert (CAS) and the online-row delete in ONE
//!    transaction, fenced on the online event count the package was
//!    verified against.
//!
//! A crash before commit leaves the online rows intact and a retry
//! picks up at verify. Nothing is ever deleted before a verified
//! package exists.

use std::path::{Path, PathBuf};

use kian_core::events::{Event, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_store::EventStore;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::commands::{CmdResult, resolve_db};

/// The package format version this build writes and reads.
pub const PACKAGE_FORMAT_VERSION: u32 = 1;

/// `manifest.json` — the package's identity (READ-MODEL §7).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArchiveManifest {
    pub format_version: u32,
    pub session: String,
    pub schema_version: u32,
    pub first_seq: u64,
    pub last_seq: u64,
    pub event_count: u64,
    pub input_count: u64,
    /// sha256 hex over `events.jsonl` bytes.
    pub events_sha256: String,
    /// sha256 hex over `inputs.jsonl` bytes.
    pub inputs_sha256: String,
    /// sha256 hex over `"<events_sha256>\n<inputs_sha256>"` — covers the
    /// per-file digests as a pair (tampering with either file OR with
    /// the digests themselves trips it).
    pub aggregate_sha256: String,
    pub created_at: u64,
    /// `sqlite` | `postgres` — the backend the package was written from.
    pub store_backend: String,
    /// `completed` | `cancelled` | `interrupted` | `failed`.
    pub terminal_state: String,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut hex = String::with_capacity(out.len() * 2);
    for b in out {
        hex.push_str(&format!("{b:02x}"));
    }
    hex
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The package directory for a session under an archive root.
pub fn package_dir(root: &Path, session: SessionId) -> PathBuf {
    root.join(session.0.to_string())
}

/// The exact bytes `kian session export` writes for these events —
/// shared by the package writer and the export command so package and
/// export cannot drift.
pub fn export_bytes(events: &[Event]) -> Result<String, String> {
    let mut out = String::new();
    for e in events {
        // Identical to run_export's default path: a canonical
        // re-serialization of the stored envelope
        // (persist-what-you-send, byte-identical round trips).
        out.push_str(&serde_json::to_string(e).map_err(|e| format!("serialize event: {e}"))?);
        out.push('\n');
    }
    Ok(out)
}

fn inputs_bytes(inputs: &[kian_store::ArchivedInput]) -> Result<String, String> {
    let mut out = String::new();
    for r in inputs {
        out.push_str(&serde_json::to_string(r).map_err(|e| format!("serialize input row: {e}"))?);
        out.push('\n');
    }
    Ok(out)
}

fn store_backend(store: &EventStore) -> &'static str {
    match store {
        EventStore::Sqlite(_) => "sqlite",
        EventStore::Postgres(_) => "postgres",
    }
}

/// Phase 1 — build and write the package from the online store.
/// Idempotent: an existing verified package for the same session is
/// kept as-is (its `created_at` stays stable across retries).
pub async fn write_package(
    store: &EventStore,
    root: &Path,
    session: SessionId,
    terminal_state: &str,
) -> Result<(PathBuf, ArchiveManifest), String> {
    let events = store
        .rebuild(session)
        .await
        .map_err(|e| format!("read events for {session:?}: {e}"))?;
    if events.is_empty() {
        return Err(format!("unknown session {}", session.0));
    }
    let dir = package_dir(root, session);
    // A verified package already on disk: keep it (retry after a crash
    // between write and commit must not churn bytes).
    if dir.join("manifest.json").is_file()
        && let Ok(pkg) = verify_package(&dir, Some(session)).await
        && pkg.manifest.event_count == events.len() as u64
    {
        return Ok((pkg.dir, pkg.manifest));
    }

    let inputs = store
        .archived_inputs_all(session)
        .await
        .map_err(|e| format!("read inputs for {session:?}: {e}"))?;
    let events_jsonl = export_bytes(&events)?;
    let inputs_jsonl = inputs_bytes(&inputs)?;
    let events_sha = sha256_hex(events_jsonl.as_bytes());
    let inputs_sha = sha256_hex(inputs_jsonl.as_bytes());
    let aggregate_sha = sha256_hex(format!("{events_sha}\n{inputs_sha}").as_bytes());
    let manifest = ArchiveManifest {
        format_version: PACKAGE_FORMAT_VERSION,
        session: session.0.to_string(),
        schema_version: SCHEMA_VERSION,
        first_seq: events.first().map(|e| e.seq).unwrap_or(0),
        last_seq: events.last().map(|e| e.seq).unwrap_or(0),
        event_count: events.len() as u64,
        input_count: inputs.len() as u64,
        events_sha256: events_sha,
        inputs_sha256: inputs_sha,
        aggregate_sha256: aggregate_sha,
        created_at: now_ms(),
        store_backend: store_backend(store).to_string(),
        terminal_state: terminal_state.to_string(),
    };

    // Build in a temp sibling, then one atomic rename — a crash leaves
    // either no package or a complete one, never half of one.
    let tmp = root.join(format!(".tmp-{}-{}", session.0, std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).map_err(|e| format!("mkdir {}: {e}", tmp.display()))?;
    std::fs::write(tmp.join("events.jsonl"), &events_jsonl)
        .map_err(|e| format!("write events.jsonl: {e}"))?;
    std::fs::write(tmp.join("inputs.jsonl"), &inputs_jsonl)
        .map_err(|e| format!("write inputs.jsonl: {e}"))?;
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    std::fs::write(tmp.join("manifest.json"), &manifest_bytes)
        .map_err(|e| format!("write manifest.json: {e}"))?;
    std::fs::create_dir_all(root).map_err(|e| format!("mkdir {}: {e}", root.display()))?;
    if dir.exists() {
        // An unverified leftover (a torn earlier attempt): replace it.
        std::fs::remove_dir_all(&dir).map_err(|e| format!("replace {}: {e}", dir.display()))?;
    }
    std::fs::rename(&tmp, &dir).map_err(|e| format!("rename into {}: {e}", dir.display()))?;
    Ok((dir, manifest))
}

/// A fully verified package: the manifest plus the exact rows
/// verification replayed. [`run_import`] appends **these** rows — the
/// files are never re-read after verification (no verify/use gap).
#[derive(Debug)]
pub struct VerifiedPackage {
    pub dir: PathBuf,
    pub manifest: ArchiveManifest,
    pub events: Vec<Event>,
    pub inputs: Vec<kian_store::ArchivedInput>,
}

/// Phase 2 — verify a package: hashes recomputed, every line re-parsed,
/// **every row's session cross-checked against the manifest's** (the
/// manifest is not self-hashed, so identity is a first-class check, not
/// a hash side effect — R9-08 F1), and counts and seq bounds
/// cross-checked against the manifest. Errors are honest refusals
/// naming the file and line.
pub async fn verify_package(
    dir: &Path,
    expect_session: Option<SessionId>,
) -> Result<VerifiedPackage, String> {
    let manifest_bytes = std::fs::read(dir.join("manifest.json"))
        .map_err(|e| format!("read {}: {e}", dir.join("manifest.json").display()))?;
    let manifest: ArchiveManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| format!("manifest.json is not a manifest: {e}"))?;
    if manifest.format_version != PACKAGE_FORMAT_VERSION {
        return Err(format!(
            "package format version {} is not {} (this build)",
            manifest.format_version, PACKAGE_FORMAT_VERSION
        ));
    }
    if let Some(sid) = expect_session
        && manifest.session != sid.0.to_string()
    {
        return Err(format!(
            "package is for session {}, expected {}",
            manifest.session, sid.0
        ));
    }
    let events_bytes = std::fs::read(dir.join("events.jsonl"))
        .map_err(|e| format!("read {}: {e}", dir.join("events.jsonl").display()))?;
    let inputs_bytes = std::fs::read(dir.join("inputs.jsonl"))
        .map_err(|e| format!("read {}: {e}", dir.join("inputs.jsonl").display()))?;

    // Per-file hashes.
    let events_sha = sha256_hex(&events_bytes);
    if events_sha != manifest.events_sha256 {
        return Err(format!(
            "events.jsonl hash mismatch: manifest {}, actual {events_sha}",
            manifest.events_sha256
        ));
    }
    let inputs_sha = sha256_hex(&inputs_bytes);
    if inputs_sha != manifest.inputs_sha256 {
        return Err(format!(
            "inputs.jsonl hash mismatch: manifest {}, actual {inputs_sha}",
            manifest.inputs_sha256
        ));
    }
    // Aggregate covers the digest pair as a pair.
    let aggregate =
        sha256_hex(format!("{}\n{}", manifest.events_sha256, manifest.inputs_sha256).as_bytes());
    if aggregate != manifest.aggregate_sha256 {
        return Err(format!(
            "aggregate sha mismatch: manifest {}, actual {aggregate}",
            manifest.aggregate_sha256
        ));
    }

    // Replay: every line must parse as its typed row, every row must
    // name the manifest's session (package identity), and the counts /
    // seq bounds must agree with the manifest — no silent truncation.
    let events_text = String::from_utf8(events_bytes).map_err(|e| format!("events.jsonl: {e}"))?;
    let mut events: Vec<Event> = Vec::new();
    for (i, line) in events_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let e: Event = serde_json::from_str(line)
            .map_err(|e| format!("events.jsonl line {}: not an event envelope: {e}", i + 1))?;
        if e.session.0.to_string() != manifest.session {
            return Err(format!(
                "events.jsonl line {}: row session {} does not match manifest session {} \
                 (package identity mismatch)",
                i + 1,
                e.session.0,
                manifest.session
            ));
        }
        events.push(e);
    }
    if events.len() as u64 != manifest.event_count {
        return Err(format!(
            "events.jsonl holds {} events, manifest says {}",
            events.len(),
            manifest.event_count
        ));
    }
    if let (Some(first), Some(last)) = (events.first(), events.last())
        && (first.seq != manifest.first_seq || last.seq != manifest.last_seq)
    {
        return Err(format!(
            "events.jsonl spans {}..={}, manifest says {}..={}",
            first.seq, last.seq, manifest.first_seq, manifest.last_seq
        ));
    }
    let inputs_text = String::from_utf8(inputs_bytes).map_err(|e| format!("inputs.jsonl: {e}"))?;
    let mut input_rows: Vec<kian_store::ArchivedInput> = Vec::new();
    for (i, line) in inputs_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let r: kian_store::ArchivedInput = serde_json::from_str(line).map_err(|e| {
            format!(
                "inputs.jsonl line {}: not an archived input row: {e}",
                i + 1
            )
        })?;
        if r.session.0.to_string() != manifest.session {
            return Err(format!(
                "inputs.jsonl line {}: row session {} does not match manifest session {} \
                 (package identity mismatch)",
                i + 1,
                r.session.0,
                manifest.session
            ));
        }
        input_rows.push(r);
    }
    if input_rows.len() as u64 != manifest.input_count {
        return Err(format!(
            "inputs.jsonl holds {} rows, manifest says {}",
            input_rows.len(),
            manifest.input_count
        ));
    }
    Ok(VerifiedPackage {
        dir: dir.to_path_buf(),
        manifest,
        events,
        inputs: input_rows,
    })
}

/// The manifest's sha256 (the marker's package identity).
pub fn manifest_sha256(manifest: &ArchiveManifest) -> Result<String, String> {
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    Ok(sha256_hex(&bytes))
}

/// The marker a verified package produces.
pub fn marker_from_manifest(
    manifest: &ArchiveManifest,
    package_path: &Path,
    archived_at: u64,
) -> Result<kian_store::ArchivedMarker, String> {
    let sid: uuid::Uuid = manifest
        .session
        .parse()
        .map_err(|e| format!("manifest session `{}` is not a uuid: {e}", manifest.session))?;
    Ok(kian_store::ArchivedMarker {
        session: SessionId(sid),
        last_seq: manifest.last_seq,
        event_count: manifest.event_count,
        input_count: manifest.input_count,
        package_path: package_path.to_string_lossy().into_owned(),
        events_sha256: manifest.events_sha256.clone(),
        inputs_sha256: manifest.inputs_sha256.clone(),
        manifest_sha256: manifest_sha256(manifest)?,
        archived_at,
    })
}

/// Whether a candidate passes the configured online window: every
/// configured threshold must be met (age AND/OR bytes both configured
/// ⇒ both required — the conservative reading; configure one for a
/// single-axis window).
pub fn eligible(
    c: &kian_store::ArchiveCandidate,
    min_age_ms: Option<u64>,
    min_bytes: Option<u64>,
    now_ms: u64,
) -> bool {
    let age_ok = min_age_ms.is_none_or(|a| now_ms.saturating_sub(c.ended_ts) >= a);
    let bytes_ok = min_bytes.is_none_or(|b| c.event_bytes + c.input_bytes >= b);
    age_ok && bytes_ok
}

/// `kian archive run [--config] [--db] [--json]`: one retention pass —
/// write → verify → commit for every eligible terminal session, under
/// the opt-in `retention:` config (fail-closed when not enabled).
pub async fn run_archive_run(
    config_path: &Path,
    db: Option<&Path>,
    json: bool,
) -> CmdResult<String> {
    let config = crate::config::ServeConfig::load(config_path)
        .map_err(|e| format!("config `{}`: {e}", config_path.display()))?;
    if !config.retention.enabled {
        return Err(
            "retention is not enabled (set retention.enabled: true + archive_dir + a threshold \
             in kian.yaml) — nothing was archived"
                .into(),
        );
    }
    let db = resolve_db(db, config_path)?;
    let root = PathBuf::from(&config.retention.archive_dir);
    let store = EventStore::open(&db)
        .await
        .map_err(|e| format!("open {}: {e}", db.display()))?;

    let now = now_ms();
    let candidates = store
        .archive_candidates()
        .await
        .map_err(|e| format!("retention scan: {e}"))?;
    let mut archived = Vec::new();
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    for c in &candidates {
        if !eligible(
            c,
            config.retention.min_age_ms,
            config.retention.min_bytes,
            now,
        ) {
            skipped.push(
                json!({"session": c.session.0.to_string(), "reason": "within the online window"}),
            );
            continue;
        }
        if store
            .archived_session(c.session)
            .await
            .map_err(|e| format!("marker read: {e}"))?
            .is_some()
        {
            skipped.push(json!({"session": c.session.0.to_string(), "reason": "already archived"}));
            continue;
        }
        let outcome = archive_one(&store, &root, c).await;
        match outcome {
            Ok(()) => archived.push(json!({
                "session": c.session.0.to_string(),
                "last_seq": c.last_seq,
                "event_count": c.event_count,
                "input_count": c.input_count,
                "bytes": c.event_bytes + c.input_bytes,
            })),
            Err(e) => failed.push(json!({"session": c.session.0.to_string(), "error": e})),
        }
    }
    store.close().await;

    let report = json!({
        "archive_dir": config.retention.archive_dir,
        "candidates": candidates.len(),
        "archived": archived,
        "skipped": skipped,
        "failed": failed,
    });
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    let mut out = format!(
        "retention pass: {} candidate(s), {} archived, {} skipped, {} failed\narchive dir: {}\n",
        candidates.len(),
        archived.len(),
        skipped.len(),
        failed.len(),
        config.retention.archive_dir
    );
    for a in &archived {
        out.push_str(&format!(
            "  archived {} ({} events, {} inputs)\n",
            a["session"], a["event_count"], a["input_count"]
        ));
    }
    for f in &failed {
        out.push_str(&format!("  FAILED {}: {}\n", f["session"], f["error"]));
    }
    if !failed.is_empty() {
        return Err(out);
    }
    Ok(out)
}

/// write → verify → byte-parity → commit for one candidate. Every
/// failure leaves the online rows intact.
async fn archive_one(
    store: &EventStore,
    root: &Path,
    c: &kian_store::ArchiveCandidate,
) -> Result<(), String> {
    // Phase 1: write (idempotent — an existing verified package is
    // kept byte-for-byte).
    let (dir, manifest) = write_package(store, root, c.session, &c.terminal_state).await?;
    // Phase 2: verify the package on disk.
    let pkg = verify_package(&dir, Some(c.session)).await?;
    if pkg.manifest != manifest {
        return Err("package changed under the writer".into());
    }
    // Byte-parity: the package's events file equals a fresh derivation
    // from the stream (the audit-invariance check, run pre-delete).
    let events = store
        .rebuild(c.session)
        .await
        .map_err(|e| format!("re-read events: {e}"))?;
    if events.len() as u64 != manifest.event_count {
        return Err(format!(
            "online stream moved before commit: {} events online, package covers {} — retry",
            events.len(),
            manifest.event_count
        ));
    }
    let on_disk = std::fs::read_to_string(dir.join("events.jsonl"))
        .map_err(|e| format!("re-read events.jsonl: {e}"))?;
    if on_disk != export_bytes(&events)? {
        return Err("package events differ from the online stream".into());
    }
    // Phase 3: commit — marker CAS + delete in one transaction, fenced
    // on the verified count.
    let marker = marker_from_manifest(&manifest, &dir, now_ms())?;
    let outcome = store
        .archive_commit(&marker, manifest.event_count)
        .await
        .map_err(|e| format!("commit: {e}"))?;
    let _ = outcome; // Committed | AlreadyArchived are both success here
    Ok(())
}

/// `kian archive verify <uuid> [--config] [--db] [--json]`: verify the
/// session's package (from the marker when the session is archived,
/// else from the configured archive dir). When the online rows are
/// still present, they are compared against the package too.
pub async fn run_archive_verify(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = EventStore::open_read_only(&db)
        .await
        .map_err(|e| format!("open {}: {e}", db.display()))?;
    let marker = store
        .archived_session(session)
        .await
        .map_err(|e| format!("marker read: {e}"))?;
    let dir = match &marker {
        Some(m) => PathBuf::from(&m.package_path),
        None => {
            let config = crate::config::ServeConfig::load(config_path)
                .map_err(|e| format!("config `{}`: {e}", config_path.display()))?;
            if config.retention.archive_dir.trim().is_empty() {
                return Err(format!(
                    "session {} is not archived and no retention.archive_dir is configured",
                    session.0
                ));
            }
            package_dir(Path::new(&config.retention.archive_dir), session)
        }
    };
    let pkg = verify_package(&dir, Some(session)).await?;
    let dir = pkg.dir;
    let manifest = pkg.manifest;

    // Online cross-check when the rows are still there.
    let online = store
        .rebuild(session)
        .await
        .map_err(|e| format!("online read: {e}"))?;
    let online_status = if online.is_empty() {
        json!({"online": false})
    } else {
        let parity = export_bytes(&online)?
            == String::from_utf8_lossy(
                &std::fs::read(dir.join("events.jsonl"))
                    .map_err(|e| format!("re-read events.jsonl: {e}"))?,
            );
        json!({"online": true, "byte_identical": parity, "online_events": online.len()})
    };
    store.close().await;

    let report = json!({
        "session": manifest.session,
        "package": dir.display().to_string(),
        "format_version": manifest.format_version,
        "event_count": manifest.event_count,
        "input_count": manifest.input_count,
        "first_seq": manifest.first_seq,
        "last_seq": manifest.last_seq,
        "terminal_state": manifest.terminal_state,
        "store_backend": manifest.store_backend,
        "archived": marker.is_some(),
        "online": online_status,
        "verdict": "verified",
    });
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(format!(
        "package      {} (verified)\nevents       {} (seq {}..={})\ninputs       {}\nterminal     {}\n{}",
        dir.display(),
        manifest.event_count,
        manifest.first_seq,
        manifest.last_seq,
        manifest.input_count,
        manifest.terminal_state,
        if marker.is_some() {
            "archived     yes (online rows removed)"
        } else {
            "archived     no (online rows still present)"
        },
    ))
}

/// `kian session import <package> [--db <sqlite>] [--json]`: verify the
/// package's manifest and restore the session into a store for
/// replay/forensics. A corrupt package or an existing session id is
/// refused; the restored stream projects and re-exports identically.
pub async fn run_import(
    package: &Path,
    db: Option<&Path>,
    config_path: &Path,
    json: bool,
) -> CmdResult<String> {
    if !package.is_dir() {
        return Err(format!(
            "`{}` is not a package directory",
            package.display()
        ));
    }
    let pkg = verify_package(package, None).await?;
    let sid: uuid::Uuid = pkg.manifest.session.parse().map_err(|e| {
        format!(
            "manifest session `{}` is not a uuid: {e}",
            pkg.manifest.session
        )
    })?;
    let session = SessionId(sid);

    // The target: an explicit --db, else the config's sqlite path.
    // Unlike the audit readers, import CREATES the file when missing
    // (restoring into a fresh store is the whole point).
    let target = match db {
        Some(p) => p.to_path_buf(),
        None => {
            let config = crate::config::ServeConfig::load(config_path).map_err(|e| {
                format!(
                    "no --db given and `{}` did not load: {e}",
                    config_path.display()
                )
            })?;
            if config.storage.sqlite.trim().is_empty() {
                return Err(
                    "no --db given and the config selects postgres — pass --db <sqlite>".into(),
                );
            }
            PathBuf::from(config.storage.sqlite)
        }
    };
    let store = EventStore::open(&target)
        .await
        .map_err(|e| format!("open {}: {e}", target.display()))?;

    // Refuse an existing session id — never merge, never overwrite.
    let existing_version = store
        .version(session)
        .await
        .map_err(|e| format!("target read: {e}"))?;
    let existing_inputs = store
        .archived_inputs_all(session)
        .await
        .map_err(|e| format!("target read: {e}"))?;
    let existing_marker = store
        .archived_session(session)
        .await
        .map_err(|e| format!("target read: {e}"))?;
    if existing_version > 0 || !existing_inputs.is_empty() || existing_marker.is_some() {
        return Err(format!(
            "session {} already exists in `{}` — import restores into a fresh store",
            session.0,
            target.display()
        ));
    }

    // Restore the EXACT rows verification replayed (under their own
    // epoch/seq — the import_jsonl doctrine) and the full input rows
    // verbatim. The files are never re-read after verification: what
    // was verified is what is appended (no verify/use gap, R9-08 F1).
    let mut restored = 0u64;
    for (i, event) in pkg.events.iter().enumerate() {
        store
            .append(
                event.session,
                event.clone(),
                kian_store::AppendGuard {
                    epoch: event.epoch,
                    expected_seq: event.seq,
                },
            )
            .await
            .map_err(|e| format!("events.jsonl line {}: append: {e}", i + 1))?;
        restored += 1;
    }
    let inputs_restored = store
        .archive_inputs_insert(&pkg.inputs)
        .await
        .map_err(|e| format!("restore inputs: {e}"))?;
    store.close().await;

    let report = json!({
        "session": pkg.manifest.session,
        "package": package.display().to_string(),
        "target": target.display().to_string(),
        "events_restored": restored,
        "inputs_restored": inputs_restored,
        "terminal_state": pkg.manifest.terminal_state,
    });
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(format!(
        "imported {} events and {} input rows for session {} into {} (terminal: {})\nreplay/inspect against the target with: kian session inspect {} --db {}",
        restored,
        inputs_restored,
        session.0,
        target.display(),
        pkg.manifest.terminal_state,
        session.0,
        target.display()
    ))
}
