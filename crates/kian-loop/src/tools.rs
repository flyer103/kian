//! Built-in local `ToolProvider` + `ExecBackend` (card C11).
//!
//! The Phase-1 built-in tool set is local fs + exec, confined to a root
//! directory. Two disciplines are baked in:
//!
//! - **Log-before-execute** is enforced by the loop (it appends the
//!   `tool/call` event *before* invoking this provider); side-effecting
//!   built-ins (`fs_write`, `exec`) rely on that ordering for their audit
//!   trail.
//! - **Never block a tokio worker**: every fs operation and every local
//!   process runs inside `spawn_blocking`, so a sleeping tool cannot stall
//!   concurrent drains (AGENTS.md async gotcha).

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use kian_core::seams::{
    ExecBackend, ExecCommand, ExecError, ExecResult, ToolCall, ToolError, ToolProvider, ToolResult,
    ToolSchema,
};
use serde_json::{Value, json};

/// Default wall-clock bound for one local exec, milliseconds.
pub const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

/// Poll interval of the exec timeout watchdog.
const EXEC_POLL_MS: u64 = 5;

// ---------------------------------------------------------------------------
// LocalExecBackend (ExecBackend seam built-in, Phase 1 = local)
// ---------------------------------------------------------------------------

/// Local command execution via `spawn_blocking` (DESIGN §6.2 `ExecBackend`
/// built-in). argv is executed directly — never a shell string.
#[derive(Debug, Clone)]
pub struct LocalExecBackend {
    default_timeout_ms: u64,
}

impl LocalExecBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_timeout_ms: DEFAULT_EXEC_TIMEOUT_MS,
        }
    }

    /// Overrides the timeout applied when a command carries none.
    #[must_use]
    pub fn with_default_timeout(mut self, timeout_ms: u64) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }
}

impl Default for LocalExecBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecBackend for LocalExecBackend {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        let default_timeout = self.default_timeout_ms;
        Box::pin(async move {
            // A blocking child process must never run on a runtime worker
            // (AGENTS.md): the whole spawn/poll/collect runs on the
            // dedicated blocking pool.
            tokio::task::spawn_blocking(move || run_blocking(cmd, default_timeout))
                .await
                .map_err(|e| ExecError::Spawn(format!("blocking join failed: {e}")))?
        })
    }
}

/// The blocking-side exec: spawn, watchdog-poll `try_wait`, kill on
/// timeout, collect output via dedicated reader threads (so a child
/// filling the pipe buffer cannot deadlock the watchdog).
fn run_blocking(cmd: ExecCommand, default_timeout_ms: u64) -> Result<ExecResult, ExecError> {
    let argv = &cmd.argv;
    let Some(program) = argv.first() else {
        return Err(ExecError::Spawn("argv must not be empty".into()));
    };
    let mut command = Command::new(program);
    command
        .args(&argv[1..])
        .envs(&cmd.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = &cmd.cwd {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .map_err(|e| ExecError::Spawn(format!("{program}: {e}")))?;

    let stdout_t = ReaderThread::spawn(child.stdout.take());
    let stderr_t = ReaderThread::spawn(child.stderr.take());

    let timeout = Duration::from_millis(cmd.timeout_ms.unwrap_or(default_timeout_ms));
    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let mut exit_code = None;
    loop {
        match child.try_wait().map_err(|e| ExecError::Io(e.to_string()))? {
            Some(status) => {
                exit_code = status.code();
                break;
            }
            None => {
                if Instant::now() >= deadline {
                    timed_out = true;
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(EXEC_POLL_MS));
            }
        }
    }
    Ok(ExecResult {
        exit_code,
        stdout: stdout_t.join(),
        stderr: stderr_t.join(),
        timed_out,
    })
}

/// Reads a piped child stream to EOF on its own OS thread (`None` — an
/// uninherited stream — reads as empty).
struct ReaderThread {
    handle: Option<std::thread::JoinHandle<String>>,
}

impl ReaderThread {
    fn spawn<R: Read + Send + 'static>(reader: Option<R>) -> Self {
        Self {
            handle: Some(std::thread::spawn(move || match reader {
                Some(mut reader) => {
                    let mut buf = Vec::new();
                    // Best effort: a child killed mid-write may close early.
                    let _ = reader.read_to_end(&mut buf);
                    String::from_utf8_lossy(&buf).into_owned()
                }
                None => String::new(),
            })),
        }
    }

    fn join(mut self) -> String {
        self.handle
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// LocalTools (ToolProvider seam built-in: fs + exec)
// ---------------------------------------------------------------------------

/// The built-in local tool set, confined to `root`:
///
/// - `exec` — `{ "argv": [..], "cwd"?: string, "timeout_ms"?: u64 }`
///   (side-effecting; runs via [`LocalExecBackend`]).
/// - `fs_list` — `{ "path": string }` (relative to `root`)
/// - `fs_read` — `{ "path": string }`
/// - `fs_write` — `{ "path": string, "content": string }` (side-effecting)
///
/// Tool names are dotless (kian-61): strict OpenAI-compatible endpoints
/// validate `tools[n].function.name` against `^[a-zA-Z0-9_-]+$` and
/// reject the pre-rename dotted spelling (`fs.list` et al.) with HTTP
/// 400. The dotted legacy names are still ACCEPTED by [`ToolProvider::
/// invoke`] (but never advertised) so sessions recorded before the
/// rename keep working when replayed.
///
/// Tool schemas are exposed sorted by name — the request-body
/// serialization freezes the `tools` array order (C8 contract), so the
/// built-in set must be deterministic.
#[derive(Debug, Clone)]
pub struct LocalTools {
    root: PathBuf,
    exec: LocalExecBackend,
}

impl LocalTools {
    /// Creates the tool set rooted at `root` (the directory is created if
    /// missing). Call this at assembly time, not inside a drain.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .unwrap_or_else(|e| panic!("LocalTools root {root:?} cannot be created: {e}"));
        Self {
            root,
            exec: LocalExecBackend::new(),
        }
    }

    #[must_use]
    pub fn with_exec_timeout(mut self, timeout_ms: u64) -> Self {
        self.exec = self.exec.with_default_timeout(timeout_ms);
        self
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn schema_list() -> Vec<ToolSchema> {
        vec![
            ToolSchema {
                name: "exec".into(),
                description: "Run a local command (argv vector, no shell).".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "argv": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "command and arguments"
                        },
                        "cwd": {"type": "string"},
                        "timeout_ms": {"type": "integer"}
                    },
                    "required": ["argv"]
                }),
            },
            ToolSchema {
                name: "fs_list".into(),
                description: "List entries of a directory under the workspace root.".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            },
            ToolSchema {
                name: "fs_read".into(),
                description: "Read a text file under the workspace root.".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            },
            ToolSchema {
                name: "fs_write".into(),
                description: "Write a text file under the workspace root (side-effecting).".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "content": {"type": "string"}
                    },
                    "required": ["path", "content"]
                }),
            },
        ]
    }
}

/// True iff `rel` never escapes upward (lexical confinement check; the
/// root itself is the only base).
fn is_confined(rel: &str) -> bool {
    !Path::new(rel)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
}

/// Validates that a parsed fs path stays inside the root.
fn confine(rel: String) -> Result<String, ToolError> {
    if is_confined(&rel) {
        Ok(rel)
    } else {
        Err(ToolError::InvalidCall(format!(
            "path {rel:?} must not contain '..'"
        )))
    }
}

impl ToolProvider for LocalTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        let mut schemas = Self::schema_list();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        schemas
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        // Resolve and validate arguments on the calling task (cheap, pure);
        // only the I/O side goes to the blocking pool.
        let parsed: Result<ParsedCall, ToolError> = match call.name.as_str() {
            "exec" => parse_exec(&call.arguments).map(ParsedCall::Exec),
            // Dotted legacy spellings (pre-kian-61 history) stay invocable
            // but are never advertised in `schemas()`.
            "fs_list" | "fs.list" => parse_path_only(&call.arguments)
                .and_then(confine)
                .map(ParsedCall::List),
            "fs_read" | "fs.read" => parse_path_only(&call.arguments)
                .and_then(confine)
                .map(ParsedCall::Read),
            "fs_write" | "fs.write" => parse_write(&call.arguments)
                .and_then(|(p, c)| confine(p).map(|p| (p, c)))
                .map(|(p, c)| ParsedCall::Write(p, c)),
            other => Err(ToolError::InvalidCall(format!("unknown tool {other:?}"))),
        };
        let call_id = call.id;
        let root = self.root.clone();
        let exec = self.exec.clone();
        Box::pin(async move {
            let output = match parsed? {
                ParsedCall::Exec(cmd) => {
                    // Errors of execution itself are honest tool-level
                    // failures, surfaced as is_error results.
                    match exec.exec(cmd).await {
                        Ok(r) => json!({
                            "exit_code": r.exit_code,
                            "stdout": r.stdout,
                            "stderr": r.stderr,
                            "timed_out": r.timed_out,
                        })
                        .with_error_flag(r.exit_code != Some(0)),
                        Err(e) => json!({"error": e.to_string()}).with_error_flag(true),
                    }
                }
                ParsedCall::List(rel) => {
                    blocking_io(move || {
                        let dir = root.join(&rel);
                        let mut names: Vec<String> = Vec::new();
                        for entry in std::fs::read_dir(&dir).map_err(|e| e.to_string())? {
                            let entry = entry.map_err(|e| e.to_string())?;
                            names.push(entry.file_name().to_string_lossy().into_owned());
                        }
                        names.sort();
                        Ok(json!({"entries": names}).with_error_flag(false))
                    })
                    .await
                }
                ParsedCall::Read(rel) => {
                    blocking_io(move || {
                        let path = root.join(&rel);
                        let content = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
                        Ok(json!({"content": content}).with_error_flag(false))
                    })
                    .await
                }
                ParsedCall::Write(rel, content) => {
                    blocking_io(move || {
                        let path = root.join(&rel);
                        if let Some(parent) = path.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                        }
                        std::fs::write(&path, content).map_err(|e| e.to_string())?;
                        Ok(json!({"written": true}).with_error_flag(false))
                    })
                    .await
                }
            };
            let (value, is_error) = output;
            Ok(ToolResult {
                call_id,
                output: value,
                is_error,
            })
        })
    }
}

enum ParsedCall {
    Exec(ExecCommand),
    List(String),
    Read(String),
    Write(String, String),
}

// ---------------------------------------------------------------------------
// FilteredTools (kian-61: config-filterable mounted surface)
// ---------------------------------------------------------------------------

/// A config-driven allowlist over another [`ToolProvider`] (kian-61).
///
/// `AgentDef.tools` names the agent's mounted surface; this wrapper
/// enforces it at the provider edge: schemas not in the list are hidden
/// from the request body (prefix minimization — the model never sees
/// tools the agent may not call), and invoking a hidden name is an
/// honest [`ToolError::InvalidCall`], never a silent passthrough.
///
/// The wrapper is *narrow*: it only ever removes from the inner
/// surface, so the caller-supplied schema order (the frozen C8 tools
/// array order) is preserved.
#[derive(Clone)]
pub struct FilteredTools {
    inner: Arc<dyn ToolProvider>,
    allow: std::collections::BTreeSet<String>,
}

impl FilteredTools {
    /// Wraps `inner` so only `allow`-listed tool names are visible.
    /// Names in `allow` that the inner provider does not mount are
    /// harmless (they simply never surface); use
    /// [`unmatched_tool_names`] for the assembly-time fail-closed check.
    #[must_use]
    pub fn new(inner: Arc<dyn ToolProvider>, allow: &[String]) -> Self {
        Self {
            inner,
            allow: allow.iter().cloned().collect(),
        }
    }
}

impl ToolProvider for FilteredTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        self.inner
            .schemas()
            .into_iter()
            .filter(|s| self.allow.contains(&s.name))
            .collect()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        if !self.allow.contains(&call.name) {
            return Box::pin(std::future::ready(Err(ToolError::InvalidCall(format!(
                "tool {:?} is not in this agent's mounted surface",
                call.name
            )))));
        }
        self.inner.invoke(call)
    }
}

/// The `listed` tool names that appear nowhere in `mounted` — the
/// assembly-time fail-closed check for `AgentDef.tools`: a typo'd name
/// refuses boot instead of silently shrinking the agent's surface.
#[must_use]
pub fn unmatched_tool_names(listed: &[String], mounted: &[ToolSchema]) -> Vec<String> {
    let mounted_names: std::collections::BTreeSet<&str> =
        mounted.iter().map(|s| s.name.as_str()).collect();
    listed
        .iter()
        .filter(|n| !mounted_names.contains(n.as_str()))
        .cloned()
        .collect()
}

/// `Value` helper carrying the honest error flag through the match arms.
trait WithErrorFlag {
    fn with_error_flag(self, is_error: bool) -> (Value, bool);
}

impl WithErrorFlag for Value {
    fn with_error_flag(self, is_error: bool) -> (Value, bool) {
        (self, is_error)
    }
}

/// Runs a blocking fs operation on the blocking pool and converts I/O
/// failures into honest `is_error` tool results.
async fn blocking_io<F>(f: F) -> (Value, bool)
where
    F: FnOnce() -> Result<(Value, bool), String> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => json!({"error": e}).with_error_flag(true),
        Err(e) => (json!({"error": format!("blocking join failed: {e}")}), true),
    }
}

fn parse_exec(args: &Value) -> Result<ExecCommand, ToolError> {
    let argv = args
        .get("argv")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        })
        .ok_or_else(|| ToolError::InvalidCall("'argv' must be a non-empty string array".into()))?;
    Ok(ExecCommand {
        argv,
        env: BTreeMap::new(),
        cwd: args.get("cwd").and_then(Value::as_str).map(str::to_owned),
        timeout_ms: args.get("timeout_ms").and_then(Value::as_u64),
    })
}

fn parse_path_only(args: &Value) -> Result<String, ToolError> {
    args.get("path")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|p| !p.is_empty())
        .ok_or_else(|| ToolError::InvalidCall("'path' must be a non-empty string".into()))
}

fn parse_write(args: &Value) -> Result<(String, String), ToolError> {
    let path = parse_path_only(args)?;
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ToolError::InvalidCall("'content' must be a string".into()))?;
    Ok((path, content))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::{FilteredTools, LocalExecBackend, LocalTools, unmatched_tool_names};
    use kian_core::seams::{ExecBackend, ExecCommand, ToolCall, ToolProvider};

    fn temp_root() -> PathBuf {
        let p = std::env::temp_dir().join(format!("kian-loop-tools-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
        ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
        }
    }

    #[tokio::test]
    async fn fs_read_write_list_round_trip_within_root() {
        let root = temp_root();
        let tools = LocalTools::new(&root);

        let write = tools
            .invoke(call(
                "c1",
                "fs_write",
                serde_json::json!({"path": "notes/a.txt", "content": "hello kian"}),
            ))
            .await
            .unwrap();
        assert!(!write.is_error);
        let read = tools
            .invoke(call(
                "c2",
                "fs_read",
                serde_json::json!({"path": "notes/a.txt"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            read.output.get("content").and_then(|v| v.as_str()),
            Some("hello kian")
        );
        let list = tools
            .invoke(call("c3", "fs_list", serde_json::json!({"path": "notes"})))
            .await
            .unwrap();
        assert_eq!(
            list.output.get("entries"),
            Some(&serde_json::json!(["a.txt"]))
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// kian-61: pre-rename history carries the dotted spellings — they
    /// stay invocable (never advertised) so replayed sessions work.
    #[tokio::test]
    async fn dotted_legacy_tool_names_stay_invocable() {
        let root = temp_root();
        let tools = LocalTools::new(&root);
        let write = tools
            .invoke(call(
                "c1",
                "fs.write",
                serde_json::json!({"path": "legacy.txt", "content": "old dialect"}),
            ))
            .await
            .unwrap();
        assert!(!write.is_error);
        let read = tools
            .invoke(call(
                "c2",
                "fs.read",
                serde_json::json!({"path": "legacy.txt"}),
            ))
            .await
            .unwrap();
        assert_eq!(
            read.output.get("content").and_then(|v| v.as_str()),
            Some("old dialect")
        );
        // ...but the advertised surface is dotless only.
        let advertised: Vec<String> = tools.schemas().into_iter().map(|s| s.name).collect();
        assert!(!advertised.contains(&"fs.read".to_string()));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn fs_path_escape_is_rejected() {
        let root = temp_root();
        let tools = LocalTools::new(&root);
        let err = tools
            .invoke(call(
                "c1",
                "fs_read",
                serde_json::json!({"path": "../escape.txt"}),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, kian_core::seams::ToolError::InvalidCall(_)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn fs_read_of_missing_file_is_an_honest_error_result() {
        let root = temp_root();
        let tools = LocalTools::new(&root);
        let res = tools
            .invoke(call(
                "c1",
                "fs_read",
                serde_json::json!({"path": "nope.txt"}),
            ))
            .await
            .unwrap();
        assert!(res.is_error);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn exec_echo_returns_output() {
        let exec = LocalExecBackend::new();
        let res = exec
            .exec(ExecCommand {
                argv: vec!["echo".into(), "42".into()],
                env: Default::default(),
                cwd: None,
                timeout_ms: Some(5_000),
            })
            .await
            .unwrap();
        assert_eq!(res.exit_code, Some(0));
        assert_eq!(res.stdout.trim(), "42");
        assert!(!res.timed_out);
    }

    #[tokio::test]
    async fn exec_timeout_kills_and_reports_timed_out() {
        let exec = LocalExecBackend::new();
        let started = std::time::Instant::now();
        let res = exec
            .exec(ExecCommand {
                argv: vec!["sleep".into(), "30".into()],
                env: Default::default(),
                cwd: None,
                timeout_ms: Some(150),
            })
            .await
            .unwrap();
        assert!(res.timed_out);
        assert_eq!(res.exit_code, None);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[tokio::test]
    async fn exec_via_tool_reports_nonzero_exit_as_error_result() {
        let root = temp_root();
        let tools = Arc::new(LocalTools::new(&root));
        let res = tools
            .invoke(call("c1", "exec", serde_json::json!({"argv": ["false"]})))
            .await
            .unwrap();
        assert!(res.is_error);
        assert_eq!(res.output.get("exit_code"), Some(&serde_json::json!(1)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn schemas_are_deterministic_and_sorted() {
        let tools = LocalTools::new(temp_root());
        let schemas = tools.schemas();
        let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["exec", "fs_list", "fs_read", "fs_write"]);
        assert_eq!(names, {
            let mut sorted = names.clone();
            sorted.sort();
            sorted
        });
    }

    /// kian-61 regression: strict OpenAI-compatible endpoints validate
    /// `tools[n].function.name` against `^[a-zA-Z0-9_-]{1,64}$` and
    /// answer HTTP 400 otherwise (verified live against real DeepSeek).
    /// Every advertised built-in name must match.
    #[test]
    fn advertised_tool_names_match_the_strict_endpoint_pattern() {
        let tools = LocalTools::new(temp_root());
        for s in tools.schemas() {
            assert!(
                !s.name.is_empty()
                    && s.name.len() <= 64
                    && s.name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "tool name {:?} violates ^[a-zA-Z0-9_-]{{1,64}}$",
                s.name
            );
        }
    }

    #[tokio::test]
    async fn unknown_tool_and_bad_args_are_invalid_calls() {
        let tools = LocalTools::new(temp_root());
        assert!(
            tools
                .invoke(call("c1", "nope", serde_json::json!({})))
                .await
                .is_err()
        );
        assert!(
            tools
                .invoke(call("c2", "fs_read", serde_json::json!({"path": 3})))
                .await
                .is_err()
        );
        assert!(
            tools
                .invoke(call("c3", "exec", serde_json::json!({"argv": []})))
                .await
                .is_err()
        );
    }

    // -- FilteredTools (kian-61) --------------------------------------------

    #[test]
    fn filtered_tools_hides_unlisted_schemas_and_preserves_order() {
        let local = Arc::new(LocalTools::new(temp_root()));
        let allow = vec!["exec".to_string(), "fs_read".to_string()];
        let filtered = FilteredTools::new(local.clone() as Arc<dyn ToolProvider>, &allow);
        let names: Vec<String> = filtered.schemas().into_iter().map(|s| s.name).collect();
        assert_eq!(names, ["exec", "fs_read"], "inner (sorted) order kept");
    }

    #[tokio::test]
    async fn filtered_tools_invoke_is_fail_closed_for_hidden_names() {
        let local = Arc::new(LocalTools::new(temp_root()));
        let allow = vec!["fs_read".to_string()];
        let filtered = FilteredTools::new(local as Arc<dyn ToolProvider>, &allow);
        // Listed name passes through to the inner provider.
        assert!(
            filtered
                .invoke(call("c1", "fs_read", serde_json::json!({"path": 3})))
                .await
                .is_err(),
            "inner validation still applies"
        );
        // Hidden name: honest InvalidCall, never a silent passthrough.
        let err = filtered
            .invoke(call("c2", "exec", serde_json::json!({"argv": ["true"]})))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            kian_core::seams::ToolError::InvalidCall(ref m) if m.contains("mounted surface")
        ));
    }

    #[test]
    fn unmatched_tool_names_flags_typos_for_fail_closed_boot() {
        let mounted = LocalTools::new(temp_root()).schemas();
        assert!(unmatched_tool_names(&["exec".into()], &mounted).is_empty());
        // The pre-rename dotted spelling no longer exists — flagged.
        assert_eq!(
            unmatched_tool_names(&["fs.read".into()], &mounted),
            vec!["fs.read".to_string()]
        );
    }
}
