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
};
use tokio::io::AsyncWriteExt;

use crate::config::SelfdevPublishSection;

/// How long the configured gate may run (§9.5).
pub(crate) const GATE_TIMEOUT_MS: u64 = 60 * 60 * 1000;

/// How much git or gate output a receipt or a failure carries (§9.5).
pub(crate) const OUTPUT_TAIL_BYTES: usize = 4096;

/// The git-backed publish port.
pub(crate) struct GitPublishPort {
    repo: PathBuf,
    gate: String,
    target_dir: Option<PathBuf>,
    author: Option<(String, String)>,
}

impl GitPublishPort {
    pub(crate) fn from_config(publish: &SelfdevPublishSection) -> Self {
        Self {
            repo: PathBuf::from(&publish.repo),
            gate: publish.gate.clone(),
            target_dir: publish.target_dir.as_ref().map(PathBuf::from),
            author: publish
                .author
                .as_ref()
                .map(|author| (author.name.clone(), author.email.clone())),
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
        let gate = self.gate.clone();
        let target_dir = self.target_dir.clone();
        let author = self.author.clone();
        Box::pin(async move {
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
            run_publish(
                &repo,
                &worktree,
                &gate,
                target_dir.as_deref(),
                author.as_ref(),
                request,
            )
            .await
        })
    }
}

/// Steps 6–8 of §9.4 in order: the worktree on the frozen base, the applied
/// patch, the commit, then the gate. A failure names how far it got.
async fn run_publish(
    repo: &Path,
    worktree: &Path,
    gate: &str,
    target_dir: Option<&Path>,
    author: Option<&(String, String)>,
    request: PublishRequest,
) -> Result<PublishReceipt, PublishError> {
    git_ok(
        repo,
        &[
            "worktree",
            "add",
            "-b",
            &request.branch,
            &request.worktree,
            &request.base_commit,
        ],
    )
    .await
    .map_err(|err| PublishError::Failed(format!("git worktree add failed: {err}")))?;
    apply_patch(worktree, &request.patch)
        .await
        .map_err(|err| PublishError::Failed(format!("the patch does not apply: {err}")))?;
    let commit = commit_patch(worktree, &request, author)
        .await
        .map_err(|err| PublishError::Failed(format!("the commit failed: {err}")))?;
    let gate_receipt = if request.gates {
        match run_gate(worktree, gate, target_dir).await {
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
    Ok(PublishReceipt {
        commit,
        branch: request.branch,
        worktree: request.worktree,
        gate: gate_receipt,
    })
}

async fn apply_patch(worktree: &Path, patch: &str) -> Result<(), String> {
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    command
        .arg("-C")
        .arg(worktree)
        .args(["apply", "--index", "-"])
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
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
    let output = command
        .output()
        .await
        .map_err(|err| format!("cannot run git commit: {err}"))?;
    if !output.status.success() {
        return Err(tail_text(&output.stderr));
    }
    let head = git_output(worktree, &["rev-parse", "HEAD"]).await?;
    if !head.status.success() {
        return Err(tail_text(&head.stderr));
    }
    Ok(String::from_utf8_lossy(&head.stdout).trim().to_string())
}

async fn run_gate(
    worktree: &Path,
    gate: &str,
    target_dir: Option<&Path>,
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

async fn git_ok(dir: &Path, args: &[&str]) -> Result<(), String> {
    let output = git_output(dir, args).await?;
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

async fn git_output(dir: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    let mut command = tokio::process::Command::from(crate::cron_worktree::git_command());
    command
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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
        GitPublishPort {
            repo: repo.to_path_buf(),
            gate: gate.to_string(),
            target_dir: None,
            author: Some(("Kian Test".to_string(), "test@kian.dev".to_string())),
        }
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
}
