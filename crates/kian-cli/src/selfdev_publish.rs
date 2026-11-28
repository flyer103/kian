//! The publish port (SELF-EVOLUTION-MODEL §9.4): the conductor's one
//! repository write, behind a port so the bundle never spawns git itself.
//!
//! Every git spawn goes through cron_worktree::git_command(), which scrubs the
//! inherited GIT_* environment: a nested git otherwise writes to the caller's
//! repository instead of the derived worktree.
//!
//! Each command is bounded: the gate runs under a wall-clock timeout, its
//! output is truncated to a tail, and a failure leaves the worktree behind on
//! purpose — it is the only evidence of how far the attempt got (§9.4).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_superagent::job::conduct::{
    GateReceipt, PublishError, PublishPort, PublishReceipt, PublishRequest, PublishTargets,
    PullRequestReceipt, PushReceipt,
};
use serde_json::Value;
use tokio::io::AsyncWriteExt;

use crate::config::SelfdevPublishSection;

/// How long the configured gate may run (§9.5).
pub(crate) const GATE_TIMEOUT_MS: u64 = 60 * 60 * 1000;

/// How much git or gate output a receipt or a failure carries (§9.5).
pub(crate) const OUTPUT_TAIL_BYTES: usize = 4096;

/// The git-backed publish port.
pub(crate) struct GitPublishPort {
    repo: PathBuf,
    remote: String,
    gate: String,
    target_dir: Option<PathBuf>,
    author: Option<(String, String)>,
    token_env: Option<String>,
    gh: PathBuf,
}

impl GitPublishPort {
    pub(crate) fn from_config(publish: &SelfdevPublishSection) -> Self {
        Self {
            repo: PathBuf::from(&publish.repo),
            remote: publish.remote.clone(),
            gate: publish.gate.clone(),
            target_dir: publish.target_dir.as_ref().map(PathBuf::from),
            author: publish
                .author
                .as_ref()
                .map(|author| (author.name.clone(), author.email.clone())),
            token_env: publish.token_env.clone(),
            gh: PathBuf::from("gh"),
        }
    }
}

impl PublishPort for GitPublishPort {
    fn derive(&self, run: SessionId) -> Option<PublishTargets> {
        let worktree = self
            .repo
            .join(".agents/worktrees")
            .join(format!("selfdev-{}", run.0));
        Some(PublishTargets {
            repo: self.repo.display().to_string(),
            worktree: worktree.display().to_string(),
        })
    }

    fn publish(
        &self,
        request: PublishRequest,
    ) -> BoxFuture<'static, Result<PublishReceipt, PublishError>> {
        let repo = self.repo.clone();
        let remote = self.remote.clone();
        let gate = self.gate.clone();
        let target_dir = self.target_dir.clone();
        let author = self.author.clone();
        let token_env = self.token_env.clone();
        let gh = self.gh.clone();
        Box::pin(async move {
            let credential = match &token_env {
                Some(name) if request.push => match std::env::var(name) {
                    Ok(value) => Some((name.clone(), value)),
                    Err(_) => {
                        return Err(PublishError::Failed(format!(
                            "credential {name} is not set"
                        )));
                    }
                },
                _ => None,
            };
            let worktree = PathBuf::from(&request.worktree);
            if worktree.exists() {
                return Err(PublishError::Occupied(format!(
                    "the worktree {} already exists: an earlier attempt or another run left state there",
                    worktree.display()
                )));
            }
            if let Some(parent) = worktree.parent()
                && let Err(err) = std::fs::create_dir_all(parent)
            {
                return Err(PublishError::Unavailable(format!(
                    "the worktree root {} cannot be created: {err}",
                    parent.display()
                )));
            }
            let context = PublishContext {
                repo: &repo,
                remote: &remote,
                gate: &gate,
                target_dir: target_dir.as_deref(),
                author: author.as_ref(),
                credential: credential.as_ref(),
                strip: token_env.as_deref(),
                gh: &gh,
            };
            run_publish(&context, request).await
        })
    }
}

/// Everything a publish sequence reads from configuration, plus the credential
/// resolved for this attempt: only the push and the pull request see it, every
/// other child has the variable removed (§9.4).
struct PublishContext<'a> {
    repo: &'a Path,
    remote: &'a str,
    gate: &'a str,
    target_dir: Option<&'a Path>,
    author: Option<&'a (String, String)>,
    credential: Option<&'a (String, String)>,
    strip: Option<&'a str>,
    gh: &'a Path,
}

/// Steps 6–8 of §9.4 in order: the worktree on the frozen base, the applied
/// patch, the commit, the gate, then the one push and the one pull request. A
/// failure names how far it got.
async fn run_publish(
    context: &PublishContext<'_>,
    request: PublishRequest,
) -> Result<PublishReceipt, PublishError> {
    let worktree_path = PathBuf::from(&request.worktree);
    let worktree = worktree_path.as_path();
    let strip = context.strip;
    git_ok(
        context.repo,
        &[
            "worktree",
            "add",
            "-b",
            &request.branch,
            &request.worktree,
            &request.base_commit,
        ],
        strip,
    )
    .await
    .map_err(|err| PublishError::Failed(format!("git worktree add failed: {err}")))?;
    apply_patch(worktree, &request.patch, strip)
        .await
        .map_err(|err| PublishError::Failed(format!("the patch does not apply: {err}")))?;
    let commit = commit_patch(worktree, &request, context.author, strip)
        .await
        .map_err(|err| PublishError::Failed(format!("the commit failed: {err}")))?;
    let gate_receipt = if request.gates {
        match run_gate(worktree, context.gate, context.target_dir, strip).await {
            Ok(receipt) => Some(receipt),
            Err(err) => {
                return Err(match err {
                    PublishError::Unknown(detail) => PublishError::Unknown(format!(
                        "{detail}: commit {commit} exists in {} and its gate did not finish",
                        worktree.display()
                    )),
                    other => other,
                });
            }
        }
    } else {
        None
    };
    let gate_ok = gate_receipt
        .as_ref()
        .is_none_or(|receipt| receipt.exit_code == 0);
    let push = if request.push && gate_ok {
        Some(
            push_branch(
                worktree,
                context.remote,
                &request.branch,
                context.credential,
            )
            .await?,
        )
    } else {
        None
    };
    let pull_request = if request.pull_request && push.is_some() {
        Some(open_pull_request(worktree, context, &request, &commit).await?)
    } else {
        None
    };
    Ok(PublishReceipt {
        commit,
        branch: request.branch,
        worktree: request.worktree,
        gate: gate_receipt,
        push,
        pull_request,
    })
}

/// Push the branch once, never forced (§9.4): an existing branch refuses the
/// push, which is exactly the rule.
async fn push_branch(
    worktree: &Path,
    remote: &str,
    branch: &str,
    credential: Option<&(String, String)>,
) -> Result<PushReceipt, PublishError> {
    let reference = format!("refs/heads/{branch}");
    let target = format!("HEAD:{reference}");
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    command
        .arg("-C")
        .arg(worktree)
        .args(["push", remote, target.as_str()])
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some((name, value)) = credential {
        command.env(name, value);
    }
    let output = command
        .output()
        .await
        .map_err(|err| PublishError::Failed(format!("cannot run git push: {err}")))?;
    if !output.status.success() {
        return Err(PublishError::Failed(format!(
            "the push was refused: {}",
            tail_text(&output.stderr)
        )));
    }
    Ok(PushReceipt {
        remote: remote.to_string(),
        reference,
    })
}

/// Open one pull request from the pushed branch (§9.4). The push already
/// landed, so any failure here is partial: an operator has to finish or undo
/// it (§12).
async fn open_pull_request(
    worktree: &Path,
    context: &PublishContext<'_>,
    request: &PublishRequest,
    commit: &str,
) -> Result<PullRequestReceipt, PublishError> {
    let partial = |detail: String| {
        PublishError::Partial(format!(
            "the branch {} is on {} at {commit}, but {detail}",
            request.branch, context.remote
        ))
    };
    let mut command = tokio::process::Command::new(context.gh);
    command
        .current_dir(worktree)
        .args([
            "pr",
            "create",
            "--base",
            &request.base,
            "--head",
            &request.branch,
            "--body-file",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some((name, value)) = context.credential {
        command.env(name, value);
    }
    let mut child = command
        .spawn()
        .map_err(|err| partial(format!("the pull request tool could not start: {err}")))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| partial("the pull request tool opened no stdin".to_string()))?;
    let body = pull_request_body(request);
    stdin
        .write_all(body.as_bytes())
        .await
        .map_err(|err| partial(format!("the pull request body could not be written: {err}")))?;
    drop(stdin);
    let waited = tokio::time::timeout(
        Duration::from_millis(GATE_TIMEOUT_MS),
        child.wait_with_output(),
    )
    .await;
    let output = match waited {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => {
            return Err(partial(format!(
                "the pull request output could not be read: {err}"
            )));
        }
        Err(_) => {
            return Err(partial(format!(
                "the pull request did not finish within {GATE_TIMEOUT_MS} ms"
            )));
        }
    };
    if !output.status.success() {
        return Err(partial(format!(
            "the pull request failed: {}",
            tail_text(&output.stderr)
        )));
    }
    let url = String::from_utf8_lossy(&output.stdout)
        .lines()
        .rev()
        .find(|line| line.trim().starts_with("http"))
        .map(|line| line.trim().to_string());
    Ok(PullRequestReceipt {
        base: request.base.clone(),
        head: request.branch.clone(),
        url,
    })
}

/// The body carries the audit chain (§11): the run, its plan digest, the
/// artifact and the pack, then the trailers the commit already holds.
fn pull_request_body(request: &PublishRequest) -> String {
    let field = |key: &str| {
        request
            .from
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let bytes = request
        .from
        .get("bytes")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let trailer = |key: &str| {
        request
            .trailers
            .iter()
            .find(|(name, _)| name == key)
            .map_or_else(String::new, |(_, value)| value.clone())
    };
    let mut body = format!(
        "The proposal from {} ({}).

- artifact: {} from step {}, {} bytes
- base commit: {}
- branch: {}

The commit carries the audit chain:

",
        trailer("Kian-Run"),
        trailer("Kian-Plan-Digest"),
        field("name"),
        field("step"),
        bytes,
        request.base_commit,
        request.branch,
    );
    for (key, value) in &request.trailers {
        body.push_str(&format!(
            "- {key}: {value}
"
        ));
    }
    body.push_str(
        "
This is a proposal for review; the runtime does not merge it.
",
    );
    body
}

fn strip_credential(command: &mut tokio::process::Command, strip: Option<&str>) {
    if let Some(name) = strip {
        command.env_remove(name);
    }
}

async fn apply_patch(worktree: &Path, patch: &str, strip: Option<&str>) -> Result<(), String> {
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    command
        .arg("-C")
        .arg(worktree)
        .args(["apply", "--index", "-"])
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    strip_credential(&mut command, strip);
    let mut child = command
        .spawn()
        .map_err(|err| format!("cannot run git apply: {err}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "git apply opened no stdin".to_string())?;
    stdin
        .write_all(patch.as_bytes())
        .await
        .map_err(|err| format!("cannot write the patch: {err}"))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .await
        .map_err(|err| format!("cannot read git apply: {err}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(tail_text(&output.stderr))
    }
}

async fn commit_patch(
    worktree: &Path,
    request: &PublishRequest,
    author: Option<&(String, String)>,
    strip: Option<&str>,
) -> Result<String, String> {
    let trailers = request
        .trailers
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    if let Some((name, email)) = author {
        command.arg("-c").arg(format!("user.name={name}"));
        command.arg("-c").arg(format!("user.email={email}"));
    }
    command
        .arg("-C")
        .arg(worktree)
        .arg("commit")
        .arg("-m")
        .arg(&request.subject)
        .arg("-m")
        .arg(&trailers)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null());
    strip_credential(&mut command, strip);
    let output = command
        .output()
        .await
        .map_err(|err| format!("cannot run git commit: {err}"))?;
    if !output.status.success() {
        return Err(tail_text(&output.stderr));
    }
    let head = git_output(worktree, &["rev-parse", "HEAD"], strip).await?;
    if !head.status.success() {
        return Err(tail_text(&head.stderr));
    }
    Ok(String::from_utf8_lossy(&head.stdout).trim().to_string())
}

async fn run_gate(
    worktree: &Path,
    gate: &str,
    target_dir: Option<&Path>,
    strip: Option<&str>,
) -> Result<GateReceipt, PublishError> {
    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(gate)
        .current_dir(worktree)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(dir) = target_dir {
        command.env("CARGO_TARGET_DIR", dir);
    }
    strip_credential(&mut command, strip);
    let child = command
        .spawn()
        .map_err(|err| PublishError::Unknown(format!("the gate could not start: {err}")))?;
    let waited = tokio::time::timeout(
        Duration::from_millis(GATE_TIMEOUT_MS),
        child.wait_with_output(),
    )
    .await;
    let output = match waited {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => {
            return Err(PublishError::Unknown(format!(
                "the gate's output could not be read: {err}"
            )));
        }
        Err(_) => {
            return Err(PublishError::Unknown(format!(
                "the gate did not finish within {GATE_TIMEOUT_MS} ms"
            )));
        }
    };
    let mut bytes = output.stdout;
    bytes.extend_from_slice(&output.stderr);
    Ok(GateReceipt {
        command: gate.to_string(),
        exit_code: output.status.code().unwrap_or(-1),
        digest: crate::archive::sha256_hex(&bytes),
        output_tail: tail_text(&bytes),
    })
}

async fn git_ok(dir: &Path, args: &[&str], strip: Option<&str>) -> Result<(), String> {
    let output = git_output(dir, args, strip).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git {}: {}",
            args.join(" "),
            tail_text(&output.stderr)
        ))
    }
}

async fn git_output(
    dir: &Path,
    args: &[&str],
    strip: Option<&str>,
) -> Result<std::process::Output, String> {
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    command
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    strip_credential(&mut command, strip);
    command
        .output()
        .await
        .map_err(|err| format!("cannot run git {}: {err}", args.join(" ")))
}

fn tail_text(bytes: &[u8]) -> String {
    let start = bytes.len().saturating_sub(OUTPUT_TAIL_BYTES);
    String::from_utf8_lossy(&bytes[start..]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_superagent::job::conduct::publish_branch;
    use serde_json::json;

    fn git(dir: &Path, args: &[&str]) -> String {
        let output = crate::cron_worktree::git_command()
            .arg("-C")
            .arg(dir)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn temp_repo(name: &str) -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("kian-publish-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(base.join("src")).expect("the temp repo");
        git(&base, &["init", "-q", "-b", "main"]);
        git(&base, &["config", "user.name", "Kian Test"]);
        git(&base, &["config", "user.email", "test@kian.dev"]);
        std::fs::write(base.join("src/lib.rs"), "old\n").expect("the file");
        std::fs::write(base.join("gate.sh"), "test -f src/lib.rs\n").expect("the gate");
        git(&base, &["add", "."]);
        git(&base, &["commit", "-q", "-m", "init"]);
        base
    }

    fn port(repo: &Path, gate: &str) -> GitPublishPort {
        port_with(repo, gate, "origin", None, PathBuf::from("gh"))
    }

    fn port_with(
        repo: &Path,
        gate: &str,
        remote: &str,
        token_env: Option<&str>,
        gh: PathBuf,
    ) -> GitPublishPort {
        GitPublishPort {
            repo: repo.to_path_buf(),
            remote: remote.to_string(),
            gate: gate.to_string(),
            target_dir: None,
            author: Some(("Kian Test".to_string(), "test@kian.dev".to_string())),
            token_env: token_env.map(str::to_string),
            gh,
        }
    }

    fn temp_remote(name: &str) -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "kian-publish-remote-{name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&base).expect("the bare remote");
        git(&base, &["init", "-q", "--bare"]);
        base
    }

    fn stub_gh(dir: &Path) -> PathBuf {
        let script = format!(
            "#!/bin/sh
set -eu
printf '%s\n' \"$@\" > {}
cat > {}
env > {}
echo https://example.test/pr/1
",
            dir.join("args.txt").display(),
            dir.join("body.txt").display(),
            dir.join("gh-env.txt").display(),
        );
        let path = dir.join("gh");
        std::fs::write(&path, script).expect("the stub");
        let status = std::process::Command::new("chmod")
            .arg("+x")
            .arg(&path)
            .status()
            .expect("chmod runs");
        assert!(status.success(), "the stub is executable");
        path
    }

    const DIFF: &str = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n";

    fn request(
        run: SessionId,
        repo: &Path,
        worktree: &str,
        patch: &str,
        gates: bool,
    ) -> PublishRequest {
        PublishRequest {
            step: "publish".to_string(),
            attempt: 1,
            from: json!({"step": "patch", "name": "patch", "sha256": "sha256:00"}),
            patch: patch.to_string(),
            base_commit: git(repo, &["rev-parse", "HEAD"]),
            branch: publish_branch(run),
            worktree: worktree.to_string(),
            subject: "selfdev: a test proposal".to_string(),
            trailers: vec![
                ("Kian-Run".to_string(), "self-dev@manual:T1".to_string()),
                ("Kian-Patch-Sha256".to_string(), "sha256:00".to_string()),
            ],
            base: "main".to_string(),
            gates,
            push: false,
            pull_request: false,
        }
    }

    #[tokio::test]
    async fn the_port_commits_the_patch_on_a_derived_branch_inside_the_worktree() {
        let repo = temp_repo("commit");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port(&repo, "sh gate.sh");
        let targets = port.derive(run).expect("a configured port derives paths");
        let worktree = PathBuf::from(&targets.worktree);
        let receipt = port
            .publish(request(run, &repo, &targets.worktree, DIFF, true))
            .await
            .expect("the publish succeeds");

        assert_eq!(receipt.branch, publish_branch(run));
        assert_eq!(receipt.worktree, targets.worktree);
        assert_eq!(receipt.commit.len(), 40);
        assert_eq!(git(&worktree, &["rev-parse", "HEAD"]), receipt.commit);
        assert_eq!(
            git(&worktree, &["rev-parse", "--abbrev-ref", "HEAD"]),
            receipt.branch
        );
        assert_eq!(
            git(&worktree, &["show", "HEAD:src/lib.rs"]),
            "new",
            "the commit carries exactly the artifact bytes"
        );
        let message = git(&worktree, &["log", "-1", "--format=%B"]);
        assert!(
            message.contains("Kian-Run: self-dev@manual:T1"),
            "{message}"
        );
        assert!(
            message.contains("Kian-Patch-Sha256: sha256:00"),
            "{message}"
        );
        let gate = receipt.gate.expect("a gate ran");
        assert_eq!(gate.exit_code, 0);
        assert_eq!(gate.command, "sh gate.sh");
        assert!(!gate.digest.is_empty());
        assert_eq!(
            std::fs::read_to_string(repo.join("src/lib.rs")).expect("the checkout"),
            "old\n",
            "the operator's checkout is untouched"
        );
    }

    #[tokio::test]
    async fn a_patch_that_does_not_apply_fails_and_leaves_the_worktree() {
        let repo = temp_repo("reject");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port(&repo, "./gate.sh");
        let targets = port.derive(run).expect("paths");
        let bad = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-nothing-like-this\n+new\n";
        let error = port
            .publish(request(run, &repo, &targets.worktree, bad, true))
            .await
            .expect_err("the patch is refused");
        match error {
            PublishError::Failed(detail) => {
                assert!(detail.contains("does not apply"), "{detail}");
            }
            other => panic!("a patch that does not apply is a failure: {other:?}"),
        }
        assert!(
            PathBuf::from(&targets.worktree).exists(),
            "the worktree is the evidence of how far the attempt got"
        );
    }

    #[tokio::test]
    async fn an_occupied_worktree_is_refused_before_anything_is_written() {
        let repo = temp_repo("occupied");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port(&repo, "./gate.sh");
        let targets = port.derive(run).expect("paths");
        std::fs::create_dir_all(&targets.worktree).expect("the occupied path");
        let error = port
            .publish(request(run, &repo, &targets.worktree, DIFF, true))
            .await
            .expect_err("occupied");
        assert!(matches!(error, PublishError::Occupied(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_failing_gate_is_a_receipt_not_an_error() {
        let repo = temp_repo("gate");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port(&repo, "exit 3");
        let targets = port.derive(run).expect("paths");
        let receipt = port
            .publish(request(run, &repo, &targets.worktree, DIFF, true))
            .await
            .expect("the sequence finished");
        assert_eq!(receipt.gate.expect("a gate ran").exit_code, 3);
        assert_eq!(receipt.commit.len(), 40);
    }

    #[tokio::test]
    async fn a_plan_that_runs_no_gate_carries_no_gate_receipt() {
        let repo = temp_repo("nogate");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port(&repo, "exit 3");
        let targets = port.derive(run).expect("paths");
        let receipt = port
            .publish(request(run, &repo, &targets.worktree, DIFF, false))
            .await
            .expect("the sequence finished");
        assert!(receipt.gate.is_none());
    }

    #[tokio::test]
    async fn the_pushed_branch_arrives_on_the_remote_once() {
        let repo = temp_repo("push");
        let remote = temp_remote("push");
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port_with(
            &repo,
            "sh gate.sh",
            &remote.display().to_string(),
            None,
            PathBuf::from("gh"),
        );
        let targets = port.derive(run).expect("paths");
        let mut request = request(run, &repo, &targets.worktree, DIFF, true);
        request.push = true;
        let receipt = port.publish(request).await.expect("the publish succeeds");

        let pushed = receipt.push.expect("a push receipt");
        assert_eq!(pushed.remote, remote.display().to_string());
        assert_eq!(pushed.reference, format!("refs/heads/{}", receipt.branch));
        assert_eq!(
            git(&remote, &["rev-parse", &pushed.reference]),
            receipt.commit,
            "the remote branch is the published commit"
        );
        assert!(receipt.pull_request.is_none(), "no pull request was asked");
    }

    #[tokio::test]
    async fn a_second_publish_on_the_same_branch_is_refused() {
        let repo = temp_repo("refuse");
        let remote = temp_remote("refuse");
        let run = SessionId(uuid::Uuid::new_v4());
        let remote_path = remote.display().to_string();
        let port = port_with(&repo, "sh gate.sh", &remote_path, None, PathBuf::from("gh"));
        let targets = port.derive(run).expect("paths");
        let mut first_request = request(run, &repo, &targets.worktree, DIFF, true);
        first_request.push = true;
        let first = port
            .publish(first_request)
            .await
            .expect("the first push succeeds");
        let reference = first.push.expect("a push receipt").reference;

        let second_worktree = format!("{}-second", targets.worktree);
        let mut second_request = request(run, &repo, &second_worktree, DIFF, true);
        second_request.push = true;
        let error = port
            .publish(second_request)
            .await
            .expect_err("the existing branch refuses the attempt");
        match error {
            PublishError::Failed(detail) => {
                assert!(detail.contains("already exists"), "{detail}");
            }
            other => panic!("an existing branch refuses the attempt: {other:?}"),
        }
        assert_eq!(
            git(&remote, &["rev-parse", &reference]),
            first.commit,
            "the refusal left the remote branch where it was"
        );
        assert!(
            !PathBuf::from(&second_worktree).exists(),
            "the branch check fails before a second worktree is written"
        );
    }

    #[tokio::test]
    async fn a_diverged_branch_is_never_force_pushed() {
        let repo = temp_repo("diverge");
        let remote = temp_remote("diverge");
        let run = SessionId(uuid::Uuid::new_v4());
        let remote_path = remote.display().to_string();
        let port = port_with(&repo, "sh gate.sh", &remote_path, None, PathBuf::from("gh"));
        let targets = port.derive(run).expect("paths");
        let mut first_request = request(run, &repo, &targets.worktree, DIFF, true);
        first_request.push = true;
        let first = port
            .publish(first_request)
            .await
            .expect("the first push succeeds");
        let reference = first
            .push
            .as_ref()
            .expect("a push receipt")
            .reference
            .clone();
        let worktree = PathBuf::from(&targets.worktree);
        std::fs::write(worktree.join("src/later.rs"), "// a later edit\n").expect("the edit");
        git(&worktree, &["add", "src/later.rs"]);
        git(&worktree, &["commit", "--amend", "--no-edit"]);

        let error = push_branch(&worktree, &remote_path, &first.branch, None)
            .await
            .expect_err("a rewritten history is refused");
        match error {
            PublishError::Failed(detail) => {
                assert!(detail.contains("push was refused"), "{detail}");
            }
            other => panic!("a refused push is a failure: {other:?}"),
        }
        assert_eq!(
            git(&remote, &["rev-parse", &reference]),
            first.commit,
            "the push never rewrites the remote branch"
        );
    }

    #[tokio::test]
    async fn a_pull_request_body_carries_the_audit_chain() {
        let repo = temp_repo("pr");
        let remote = temp_remote("pr");
        let audit = repo.join("audit");
        std::fs::create_dir_all(&audit).expect("the audit dir");
        let gh = stub_gh(&audit);
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port_with(&repo, "sh gate.sh", &remote.display().to_string(), None, gh);
        let targets = port.derive(run).expect("paths");
        let mut request = request(run, &repo, &targets.worktree, DIFF, true);
        request.push = true;
        request.pull_request = true;
        request.trailers = vec![
            ("Kian-Run".to_string(), "self-dev@manual:T1".to_string()),
            ("Kian-Plan-Digest".to_string(), "sha256:aa".to_string()),
            ("Kian-Patch-Sha256".to_string(), "sha256:bb".to_string()),
            ("Kian-Pack".to_string(), "sha256:cc".to_string()),
        ];
        let receipt = port.publish(request).await.expect("the publish succeeds");

        let pull_request = receipt.pull_request.expect("a pull request receipt");
        assert_eq!(pull_request.base, "main");
        assert_eq!(pull_request.head, receipt.branch);
        assert_eq!(
            pull_request.url.as_deref(),
            Some("https://example.test/pr/1")
        );
        let body = std::fs::read_to_string(audit.join("body.txt")).expect("the body");
        for line in [
            "Kian-Run: self-dev@manual:T1",
            "Kian-Plan-Digest: sha256:aa",
            "Kian-Patch-Sha256: sha256:bb",
            "Kian-Pack: sha256:cc",
        ] {
            assert!(body.contains(line), "{body}");
        }
        assert!(body.contains("does not merge it"), "{body}");
        let args = std::fs::read_to_string(audit.join("args.txt")).expect("the args");
        let args: Vec<&str> = args.lines().collect();
        assert!(args.contains(&"pr") && args.contains(&"create"), "{args:?}");
        assert!(args.contains(&"--base"), "{args:?}");
        assert!(args.contains(&"main"), "{args:?}");
        assert!(args.contains(&"--head"), "{args:?}");
        assert!(args.contains(&receipt.branch.as_str()), "{args:?}");
    }

    #[tokio::test]
    async fn only_the_push_and_the_pull_request_see_the_credential() {
        let repo = temp_repo("credential");
        let remote = temp_remote("credential");
        let audit = repo.join("audit");
        std::fs::create_dir_all(&audit).expect("the audit dir");
        let gh = stub_gh(&audit);
        let gate_env = audit.join("gate-env.txt");
        let gate = format!("env > {}", gate_env.display());
        let name = format!("KIAN_TEST_TOKEN_{}", uuid::Uuid::new_v4().simple());
        // SAFETY: the name is unique to this test and no other test reads it.
        unsafe { std::env::set_var(&name, "s3cret") };
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port_with(&repo, &gate, &remote.display().to_string(), Some(&name), gh);
        let targets = port.derive(run).expect("paths");
        let mut first_request = request(run, &repo, &targets.worktree, DIFF, true);
        first_request.push = true;
        first_request.pull_request = true;
        let receipt = port
            .publish(first_request)
            .await
            .expect("the publish succeeds");
        // SAFETY: same unique name.
        unsafe { std::env::remove_var(&name) };

        assert!(receipt.push.is_some());
        let gate_env = std::fs::read_to_string(&gate_env).expect("the gate env");
        assert!(
            !gate_env.contains("s3cret"),
            "the gate must not see the credential"
        );
        let gh_env = std::fs::read_to_string(audit.join("gh-env.txt")).expect("the gh env");
        assert!(
            gh_env.contains("s3cret"),
            "the pull request tool is one of the two children that see it"
        );

        let second = SessionId(uuid::Uuid::new_v4());
        let second_gate_env = audit.join("gates-only-env.txt");
        let second_gate = format!("env > {}", second_gate_env.display());
        // SAFETY: same unique name, no concurrent reader.
        unsafe { std::env::set_var(&name, "s3cret") };
        let port = port_with(
            &repo,
            &second_gate,
            &remote.display().to_string(),
            Some(&name),
            audit.join("gh"),
        );
        let targets = port.derive(second).expect("paths");
        let second_request = request(second, &repo, &targets.worktree, DIFF, true);
        port.publish(second_request)
            .await
            .expect("the publish succeeds");
        // SAFETY: same unique name.
        unsafe { std::env::remove_var(&name) };
        let env = std::fs::read_to_string(&second_gate_env).expect("the gate env");
        assert!(
            !env.contains("s3cret"),
            "a gates-only publish strips the variable too"
        );
    }

    #[tokio::test]
    async fn a_missing_credential_fails_before_the_worktree() {
        let repo = temp_repo("nocredential");
        let name = format!("KIAN_MISSING_{}", uuid::Uuid::new_v4().simple());
        let run = SessionId(uuid::Uuid::new_v4());
        let port = port_with(
            &repo,
            "sh gate.sh",
            "origin",
            Some(&name),
            PathBuf::from("gh"),
        );
        let targets = port.derive(run).expect("paths");
        let mut request = request(run, &repo, &targets.worktree, DIFF, true);
        request.push = true;
        let error = port
            .publish(request)
            .await
            .expect_err("the credential is absent");
        match error {
            PublishError::Failed(detail) => {
                assert!(
                    detail.contains(&format!("credential {name} is not set")),
                    "{detail}"
                );
            }
            other => panic!("a missing credential is a failure: {other:?}"),
        }
        assert!(
            !PathBuf::from(&targets.worktree).exists(),
            "nothing is written before the credential is known"
        );
    }
}
