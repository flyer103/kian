//! `kian self verify` — the read-only landing verifier (SELF-EVOLUTION §18).
//!
//! It recomputes the audit chain of one self-dev run from its committed
//! streams and the local repository named by `selfdev.publish.repo`. It
//! writes nothing, fetches nothing and takes no credential: a commit that
//! exists only on the remote is reported missing.

use std::path::Path;
use std::process::Stdio;

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use kian_superagent::job::conduct::{
    ARTIFACT_CALL_PREFIX, PUBLISH_CALL_PREFIX, PUBLISH_TOOL, publish_branch, publish_tool_calls,
};
use kian_superagent::job::{plan, status};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::commands::CmdResult;

/// One `kian self verify` invocation.
#[derive(Debug, Clone)]
pub struct Options {
    /// The serve config.
    pub config: std::path::PathBuf,
    /// The run id, `job@slot`.
    pub run: String,
    /// The base branch; defaults to `selfdev.publish.base`.
    pub base: Option<String>,
    /// Require the artifact commit to be an ancestor of the base.
    pub merged: bool,
}

/// The rendered outcome: stdout always carries the report, stderr carries
/// exactly the one-line failure when the chain is broken.
#[derive(Debug)]
pub struct Report {
    /// Whether every link held.
    pub ok: bool,
    /// What to print on stdout.
    pub stdout: String,
    /// What to print on stderr.
    pub stderr: String,
}

/// One link of the audit chain.
struct Link {
    link: &'static str,
    ok: bool,
    detail: String,
}

/// The chain stopped at the last pushed link.
struct Broken;

/// Recompute one run's audit chain.
pub async fn run(options: Options, json_output: bool) -> CmdResult<Report> {
    let config = crate::config::ServeConfig::load(&options.config)
        .map_err(|error| format!("config {}: {error}", options.config.display()))?;
    let Some(publish) = config
        .selfdev
        .as_ref()
        .and_then(|section| section.publish.as_ref())
    else {
        return Err("landing is not configured".to_string());
    };
    let Some((job_id, slot)) = crate::job_cli::split_run_id(&options.run) else {
        return Err(format!("{} is not a run id (job@slot)", options.run));
    };
    let base = options
        .base
        .clone()
        .filter(|base| !base.trim().is_empty())
        .unwrap_or_else(|| publish.base.clone());
    let run = plan::run_id(job_id, slot);
    let session = plan::run_session(job_id, slot);

    let store = crate::job_cli::open_store(&config, true).await?;
    let events = store
        .rebuild(session)
        .await
        .map_err(|error| format!("run {run}: {error}"))?;
    let mut links = Vec::new();
    let result = check(
        &store,
        Path::new(&publish.repo),
        &publish.remote,
        &base,
        &run,
        session,
        &events,
        publish,
        options.merged,
        &mut links,
    )
    .await;
    store.close().await;

    let ok = result.is_ok();
    let merged = if options.merged {
        links
            .iter()
            .find(|link| link.link == "merged")
            .map(|link| link.ok)
    } else {
        None
    };
    let stdout = if json_output {
        render_json(&run, &links, merged)
    } else if ok {
        render_text(&run, &links)
    } else {
        String::new()
    };
    let stderr = if !json_output && !ok {
        let failed = links
            .last()
            .expect("a broken chain always names its last link");
        format!("audit chain broken at {}: {}\n", failed.link, failed.detail)
    } else {
        String::new()
    };
    Ok(Report { ok, stdout, stderr })
}

#[allow(clippy::too_many_arguments)]
async fn check(
    store: &EventStore,
    repo: &Path,
    remote: &str,
    base: &str,
    run: &str,
    session: SessionId,
    events: &[Event],
    publish: &crate::config::SelfdevPublishSection,
    require_merged: bool,
    links: &mut Vec<Link>,
) -> Result<(), Broken> {
    let Some(job) = events
        .iter()
        .rev()
        .filter(|event| event.kind == EventKind::InputAdmitted)
        .find_map(|event| event.bytes.get(status::JOB_KEY))
    else {
        return Err(broken(
            links,
            "selfdev",
            "the run stream carries no job marker",
        ));
    };
    if job.get("run_id").and_then(Value::as_str) != Some(run) {
        return Err(broken(
            links,
            "selfdev",
            format!(
                "the marker names run {}, not {run}",
                job.get("run_id")
                    .and_then(Value::as_str)
                    .unwrap_or("(none)")
            ),
        ));
    }
    let Some(selfdev) = job.get("selfdev") else {
        return Err(broken(
            links,
            "selfdev",
            "the admitted input row carries no job.selfdev",
        ));
    };
    let Some(pack) = selfdev.get("pack") else {
        return Err(broken(links, "selfdev", "job.selfdev carries no pack"));
    };
    let Some(manifest) = recorded_digest(pack, "manifest_sha256") else {
        return Err(broken(
            links,
            "selfdev",
            "job.selfdev.pack.manifest_sha256 is not sha256: plus 64 lowercase hex",
        ));
    };
    let Some(files) = recorded_digest(pack, "files_sha256") else {
        return Err(broken(
            links,
            "selfdev",
            "job.selfdev.pack.files_sha256 is not sha256: plus 64 lowercase hex",
        ));
    };
    let Some(plan_digest) = job.get("plan_digest").and_then(Value::as_str) else {
        return Err(broken(
            links,
            "selfdev",
            "the marker carries no plan_digest",
        ));
    };
    if !is_digest(plan_digest) {
        return Err(broken(
            links,
            "selfdev",
            format!("the marker's plan_digest {plan_digest} is not sha256: plus 64 lowercase hex"),
        ));
    }
    let Some(base_commit) = selfdev.get("base_commit").and_then(Value::as_str) else {
        return Err(broken(
            links,
            "selfdev",
            "job.selfdev carries no base_commit",
        ));
    };
    if !is_full_commit(base_commit) {
        return Err(broken(
            links,
            "selfdev",
            format!("the frozen base_commit {base_commit} is not a full 40-hex commit"),
        ));
    }
    links.push(Link {
        link: "selfdev",
        ok: true,
        detail: format!("pack {manifest}, files {files}, base {base_commit}"),
    });

    let Some(plan_value) = job.get("plan") else {
        return Err(broken(links, "plan", "the marker carries no frozen plan"));
    };
    let plan: plan::JobPlan = match serde_json::from_value(plan_value.clone()) {
        Ok(plan) => plan,
        Err(error) => {
            return Err(broken(
                links,
                "plan",
                format!("the frozen plan does not parse: {error}"),
            ));
        }
    };
    let computed = plan.digest();
    if computed != plan_digest {
        return Err(broken(
            links,
            "plan",
            format!("the frozen plan hashes to {computed}, not {plan_digest}"),
        ));
    }
    // ADR-084: a frozen plan may still carry the conductor's publish step (the
    // replay of a run that predates the retirement); a tool-based plan declares
    // publish_ok on an agent step whose child calls the publish tool.
    let publish_step = match plan.steps.iter().find(|step| step.is_publish()) {
        Some(step) => step,
        None => match publish_tool_step(&plan) {
            Some(step) => step,
            None => {
                return Err(broken(
                    links,
                    "plan",
                    "the frozen plan declares no publish step",
                ));
            }
        },
    };
    let frozen_publish = publish_step.is_publish();
    let Some(input) = publish_step.inputs.first() else {
        return Err(broken(
            links,
            "plan",
            "the publish step names no input artifact",
        ));
    };
    let spec = publish_step.publish();
    if frozen_publish && spec.is_none() {
        return Err(broken(
            links,
            "plan",
            "the publish step carries no publish block",
        ));
    }
    links.push(Link {
        link: "plan",
        ok: true,
        detail: format!(
            "{} step(s), publish step {} reads {}.{}",
            plan.steps.len(),
            publish_step.id,
            input.step,
            input.name
        ),
    });

    let artifact = match artifact_receipt(events, &input.step, &input.name) {
        Ok(artifact) => artifact,
        Err(detail) => return Err(broken(links, "artifact", detail)),
    };
    let Some(sha) = artifact.get("sha256").and_then(Value::as_str) else {
        return Err(broken(links, "artifact", "the artifact records no sha256"));
    };
    if !is_digest(sha) {
        return Err(broken(
            links,
            "artifact",
            format!("the artifact's sha256 {sha} is not sha256: plus 64 lowercase hex"),
        ));
    }
    let Some(bytes) = artifact.get("bytes").and_then(Value::as_u64) else {
        return Err(broken(
            links,
            "artifact",
            "the artifact records no byte length",
        ));
    };
    let Some(ref_session) = artifact
        .get("ref")
        .and_then(|reference| reference.get("session"))
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
    else {
        return Err(broken(
            links,
            "artifact",
            "the artifact's ref names no producer session",
        ));
    };
    let Some(ref_seq) = artifact
        .get("ref")
        .and_then(|reference| reference.get("seq"))
        .and_then(Value::as_u64)
    else {
        return Err(broken(
            links,
            "artifact",
            "the artifact's ref names no producer seq",
        ));
    };
    let producer = match store.rebuild(SessionId(ref_session)).await {
        Ok(events) => events,
        Err(error) => {
            return Err(broken(
                links,
                "artifact",
                format!("the producer stream {ref_session} cannot be read: {error}"),
            ));
        }
    };
    let Some(event) = producer.iter().find(|event| event.seq == ref_seq) else {
        return Err(broken(
            links,
            "artifact",
            format!("the producer stream has no event at seq {ref_seq}"),
        ));
    };
    if event.kind != EventKind::AssistantMessage {
        return Err(broken(
            links,
            "artifact",
            format!("the producer event at seq {ref_seq} is not an assistant message"),
        ));
    }
    let Some(text) = message_text(event) else {
        return Err(broken(
            links,
            "artifact",
            format!("the producer message at seq {ref_seq} carries no text"),
        ));
    };
    let computed = plan::digest_text(text);
    if computed != sha {
        return Err(broken(
            links,
            "artifact",
            format!("the producer text hashes to {computed}, not {sha}"),
        ));
    }
    if text.len() as u64 != bytes {
        return Err(broken(
            links,
            "artifact",
            format!(
                "the producer text is {} bytes, not the recorded {bytes}",
                text.len()
            ),
        ));
    }
    links.push(Link {
        link: "artifact",
        ok: true,
        detail: format!("{} bytes, {sha}, producer seq {ref_seq}", text.len()),
    });

    let branch = publish_branch(session);
    let commit = if frozen_publish {
        let spec = spec.expect("a frozen publish step carries a publish block");
        let Some((intent, call_id)) = publish_intent(events, &publish_step.id) else {
            return Err(broken(
                links,
                "publish",
                format!(
                    "the run carries no job.publish intent for step {}",
                    publish_step.id
                ),
            ));
        };
        if intent.get("from") != Some(&artifact) {
            return Err(broken(
                links,
                "publish",
                "the publish intent names a different artifact than the harvested receipt",
            ));
        }
        if intent.get("base_commit").and_then(Value::as_str) != Some(base_commit) {
            return Err(broken(
                links,
                "publish",
                format!("the publish intent does not freeze the base {base_commit}"),
            ));
        }
        if intent.get("branch").and_then(Value::as_str) != Some(branch.as_str()) {
            return Err(broken(
                links,
                "publish",
                format!("the publish intent does not name {branch}"),
            ));
        }
        let Some(receipt) = publish_receipt(events, call_id) else {
            return Err(broken(
                links,
                "publish",
                format!("the run carries no job.publish receipt for {call_id}"),
            ));
        };
        validate_publish_receipt(
            links,
            &receipt,
            &branch,
            spec.gates,
            spec.push,
            spec.pull_request,
        )?
    } else {
        let receipt = match publish_tool_receipt(store, events, &publish_step.id).await {
            Ok(receipt) => receipt,
            Err(detail) => return Err(broken(links, "publish", detail)),
        };
        validate_publish_receipt(
            links,
            &receipt,
            &branch,
            publish.gates,
            publish.push,
            publish.pull_request,
        )?
    };

    let exists = format!("{commit}^{{commit}}");
    if git_bytes(repo, &["cat-file", "-e", &exists]).is_err() {
        return Err(broken(
            links,
            "commit",
            format!("{commit} is not in the local object database (the verifier does not fetch)"),
        ));
    }
    let parents = match git_text(repo, &["rev-list", "--parents", "-n", "1", commit.as_str()]) {
        Ok(parents) => parents,
        Err(error) => return Err(broken(links, "commit", error)),
    };
    let mut parts = parents.split_whitespace();
    let head = parts.next().unwrap_or_default();
    if head != commit {
        return Err(broken(
            links,
            "commit",
            format!("git resolves {commit} to {head}"),
        ));
    }
    let Some(first) = parts.next() else {
        return Err(broken(
            links,
            "commit",
            format!("{commit} has no parent, so it cannot descend from the frozen base"),
        ));
    };
    if first != base_commit {
        return Err(broken(
            links,
            "commit",
            format!("the first parent is {first}, not the frozen base {base_commit}"),
        ));
    }
    let diff = match git_bytes(
        repo,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            base_commit,
            commit.as_str(),
        ],
    ) {
        Ok(diff) => diff,
        Err(error) => return Err(broken(links, "commit", error)),
    };
    if diff != text.as_bytes() {
        return Err(broken(
            links,
            "commit",
            format!(
                "git diff {base_commit} {commit} is {} bytes, not the artifact's {}",
                diff.len(),
                text.len()
            ),
        ));
    }
    let message = match git_text(repo, &["log", "-1", "--format=%B", commit.as_str()]) {
        Ok(message) => message,
        Err(error) => return Err(broken(links, "commit", error)),
    };
    let trailers = [
        ("Kian-Run", run),
        ("Kian-Plan-Digest", plan_digest),
        ("Kian-Patch-Sha256", sha),
        ("Kian-Pack", manifest),
    ];
    for (key, want) in trailers {
        match trailer(&message, key) {
            Some(found) if found == want => {}
            Some(found) => {
                return Err(broken(
                    links,
                    "commit",
                    format!("the {key} trailer is {found}, not {want}"),
                ));
            }
            None => {
                return Err(broken(
                    links,
                    "commit",
                    format!("{commit} carries no {key} trailer"),
                ));
            }
        }
    }
    links.push(Link {
        link: "commit",
        ok: true,
        detail: format!("{commit} descends from {base_commit}; its diff is the artifact"),
    });

    let local_ref = format!("refs/heads/{branch}");
    match git_text(repo, &["rev-parse", "--verify", "--quiet", &local_ref]) {
        Ok(oid) => {
            let oid = oid.trim();
            if oid != commit {
                return Err(broken(
                    links,
                    "branch",
                    format!("{local_ref} points at {oid}, not {commit}"),
                ));
            }
            links.push(Link {
                link: "branch",
                ok: true,
                detail: local_ref,
            });
        }
        Err(_) => {
            let remote_ref = format!("refs/remotes/{remote}/{branch}");
            match git_text(repo, &["rev-parse", "--verify", "--quiet", &remote_ref]) {
                Ok(oid) => {
                    let oid = oid.trim();
                    if oid != commit {
                        return Err(broken(
                            links,
                            "branch",
                            format!("{remote_ref} points at {oid}, not {commit}"),
                        ));
                    }
                    links.push(Link {
                        link: "branch",
                        ok: true,
                        detail: remote_ref,
                    });
                }
                Err(_) if require_merged => {
                    links.push(Link {
                        link: "branch",
                        ok: true,
                        detail: format!(
                            "{branch} is gone; --merged checks the commit's reachability"
                        ),
                    });
                }
                Err(_) => {
                    return Err(broken(
                        links,
                        "branch",
                        format!(
                            "{branch} is neither local nor a remote-tracking ref (a missing branch is allowed only with --merged)"
                        ),
                    ));
                }
            }
        }
    }

    if !require_merged {
        return Ok(());
    }
    match git_code(
        repo,
        &["merge-base", "--is-ancestor", commit.as_str(), base],
    ) {
        Ok(0) => links.push(Link {
            link: "merged",
            ok: true,
            detail: format!("{commit} is an ancestor of {base}"),
        }),
        Ok(1) => {
            return Err(broken(links, "merged", format!("not merged into {base}")));
        }
        Ok(code) => {
            return Err(broken(
                links,
                "merged",
                format!("git merge-base exited {code} checking {base}"),
            ));
        }
        Err(error) => return Err(broken(links, "merged", error)),
    }
    Ok(())
}

fn broken(links: &mut Vec<Link>, link: &'static str, detail: impl Into<String>) -> Broken {
    links.push(Link {
        link,
        ok: false,
        detail: detail.into(),
    });
    Broken
}

fn render_text(run: &str, links: &[Link]) -> String {
    let mut out = format!("run {run}\n");
    for link in links {
        out.push_str(&format!("{}: ok ({})\n", link.link, link.detail));
    }
    out
}

fn render_json(run: &str, links: &[Link], merged: Option<bool>) -> String {
    let mut body = Map::new();
    body.insert("ok".to_string(), json!(links.iter().all(|link| link.ok)));
    body.insert("run".to_string(), json!(run));
    body.insert(
        "links".to_string(),
        Value::Array(
            links
                .iter()
                .map(|link| json!({"link": link.link, "ok": link.ok, "detail": link.detail}))
                .collect(),
        ),
    );
    if let Some(merged) = merged {
        body.insert("merged".to_string(), json!(merged));
    }
    format!("{}\n", Value::Object(body))
}

fn recorded_digest<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| is_digest(value))
}

/// `sha256:` plus 64 lowercase hex characters.
fn is_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    })
}

fn is_full_commit(value: &str) -> bool {
    value.len() == 40 && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn trailer<'a>(message: &'a str, key: &str) -> Option<&'a str> {
    message.lines().rev().find_map(|line| {
        line.strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(':'))
            .map(str::trim)
    })
}

fn message_text(event: &Event) -> Option<&str> {
    let content = event.bytes.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text);
    }
    content.get("text").and_then(Value::as_str)
}

fn publish_tool_step(plan: &plan::JobPlan) -> Option<&plan::StepPlan> {
    plan.steps.iter().find(|step| {
        step.verify
            .iter()
            .any(|check| matches!(check, plan::JobCheck::PublishOk))
    })
}

/// The receipt a tool-based publish child committed (ADR-084): the call names no
/// coordinates, so the step, attempt and patch all come from the committed
/// streams, and the run's dispatch record names the child that called it.
async fn publish_tool_receipt(
    store: &EventStore,
    events: &[Event],
    step: &str,
) -> Result<Value, String> {
    let Some(dispatch) = status::dispatches(events)
        .into_iter()
        .rev()
        .find(|record| record.step == step)
    else {
        return Err(format!(
            "the run carries no dispatch record for the publish step {step}"
        ));
    };
    let child = store
        .rebuild(dispatch.child)
        .await
        .map_err(|error| format!("the publish child {}: {error}", dispatch.child.0))?;
    let calls = publish_tool_calls(&child);
    match calls.as_slice() {
        [] => Err("the publish child never called the publish tool".to_string()),
        [call] => {
            if !call
                .arguments
                .as_object()
                .is_some_and(|arguments| arguments.is_empty())
            {
                return Err(
                    "the publish tool call carries arguments; the tool derives every coordinate from the streams"
                        .to_string(),
                );
            }
            let Some(result) = &call.result else {
                return Err(
                    "the publish call has no receipt: the worktree may exist and an operator must inspect"
                        .to_string(),
                );
            };
            if result.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "the publish tool did not succeed: {}",
                    result
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("no receipt")
                ));
            }
            match result.get("result") {
                Some(receipt) => Ok(receipt.clone()),
                None => Err("the publish tool receipt carries no result body".to_string()),
            }
        }
        _ => Err(format!(
            "the publish child called the publish tool {} times; a run publishes once",
            calls.len()
        )),
    }
}

fn validate_publish_receipt(
    links: &mut Vec<Link>,
    receipt: &Value,
    branch: &str,
    gates: bool,
    push: bool,
    pull_request: bool,
) -> Result<String, Broken> {
    if receipt.get("branch").and_then(Value::as_str) != Some(branch) {
        return Err(broken(
            links,
            "publish",
            format!(
                "the publish receipt names branch {}, not {branch}",
                receipt
                    .get("branch")
                    .and_then(Value::as_str)
                    .unwrap_or("(none)")
            ),
        ));
    }
    let Some(commit) = receipt.get("commit").and_then(Value::as_str) else {
        return Err(broken(
            links,
            "publish",
            "the publish receipt names no commit",
        ));
    };
    if !is_full_commit(commit) {
        return Err(broken(
            links,
            "publish",
            format!("the publish commit {commit} is not a full 40-hex commit"),
        ));
    }
    if gates {
        let Some(exit_code) = receipt
            .get("gate")
            .and_then(|gate| gate.get("exit_code"))
            .and_then(Value::as_i64)
        else {
            return Err(broken(
                links,
                "publish",
                "a gate was required but the publish receipt carries none",
            ));
        };
        if exit_code != 0 {
            return Err(broken(
                links,
                "publish",
                format!("the gate exited {exit_code}, not 0"),
            ));
        }
    }
    if push {
        let reference = receipt
            .get("push")
            .and_then(|push| push.get("ref"))
            .and_then(Value::as_str);
        let want = format!("refs/heads/{branch}");
        if reference != Some(want.as_str()) {
            return Err(broken(
                links,
                "publish",
                format!(
                    "the publish receipt pushed {}, not {want}",
                    reference.unwrap_or("nothing")
                ),
            ));
        }
    }
    if pull_request {
        let head = receipt
            .get("pull_request")
            .and_then(|pull_request| pull_request.get("head"))
            .and_then(Value::as_str);
        if head != Some(branch) {
            return Err(broken(
                links,
                "publish",
                format!(
                    "the pull request names head {}, not {branch}",
                    head.unwrap_or("(none)")
                ),
            ));
        }
    }
    links.push(Link {
        link: "publish",
        ok: true,
        detail: format!("{branch} at {commit}"),
    });
    Ok(commit.to_string())
}

fn artifact_receipt(events: &[Event], step: &str, name: &str) -> Result<Value, String> {
    let prefix = format!("{ARTIFACT_CALL_PREFIX}{step}:");
    for event in events.iter().rev() {
        if event.kind != EventKind::ToolResult {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        if !call_id.starts_with(&prefix) {
            continue;
        }
        if event.bytes.get("ok").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let Some(artifact) = event
            .bytes
            .get("result")
            .and_then(|result| result.get("artifact"))
        else {
            continue;
        };
        if artifact.get("step").and_then(Value::as_str) != Some(step) {
            continue;
        }
        if artifact.get("name").and_then(Value::as_str) != Some(name) {
            continue;
        }
        return Ok(artifact.clone());
    }
    Err(format!(
        "the run carries no successful job.artifact receipt for {step}.{name}"
    ))
}

fn publish_intent<'a>(events: &'a [Event], step: &str) -> Option<(&'a Value, &'a str)> {
    let prefix = format!("{PUBLISH_CALL_PREFIX}{step}:");
    for event in events.iter().rev() {
        if event.kind != EventKind::ToolCall {
            continue;
        }
        if event.bytes.get("tool").and_then(Value::as_str) != Some(PUBLISH_TOOL) {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        if !call_id.starts_with(&prefix) {
            continue;
        }
        let Some(args) = event.bytes.get("args") else {
            continue;
        };
        return Some((args, call_id));
    }
    None
}

fn publish_receipt(events: &[Event], call_id: &str) -> Option<Value> {
    let mut failure = None;
    for event in events.iter().rev() {
        if event.kind != EventKind::ToolResult {
            continue;
        }
        if event.bytes.get("call_id").and_then(Value::as_str) != Some(call_id) {
            continue;
        }
        if event.bytes.get("ok").and_then(Value::as_bool) == Some(true) {
            return event.bytes.get("result").cloned();
        }
        failure = Some(
            event
                .bytes
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("the attempt settled failed")
                .to_string(),
        );
    }
    let _ = failure;
    None
}

fn git_bytes(repo: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let mut command = crate::cron_worktree::git_command();
    command
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null());
    let output = command
        .output()
        .map_err(|error| format!("git {}: {error}", args.join(" ")))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn git_text(repo: &Path, args: &[&str]) -> Result<String, String> {
    let bytes = git_bytes(repo, args)?;
    String::from_utf8(bytes).map_err(|error| format!("git {}: {error}", args.join(" ")))
}

fn git_code(repo: &Path, args: &[&str]) -> Result<i32, String> {
    let mut command = crate::cron_worktree::git_command();
    command
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null());
    let output = command
        .output()
        .map_err(|error| format!("git {}: {error}", args.join(" ")))?;
    match output.status.code() {
        Some(code) => Ok(code),
        None => Err(format!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::process::Stdio;

    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::Epoch;
    use kian_store::AppendGuard;
    use kian_superagent::job::conduct::PUBLISH_AGENT_TOOL;
    use kian_superagent::job::plan::{self, JobPlan};
    use serde_json::Value;

    use super::*;

    const JOB: &str = "verify-job";
    const SLOT: &str = "manual:01J0VERIFY0000000000000000";
    const PRODUCER_SEQ: u64 = 2;
    const SUBJECT: &str = "self-dev: fixture proposal";

    struct Fixture {
        repo: PathBuf,
        worktree: PathBuf,
        config: PathBuf,
        db: PathBuf,
        run: SessionId,
        producer: SessionId,
        base_commit: String,
        commit: String,
        branch: String,
        plan: JobPlan,
        plan_digest: Option<String>,
        patch: String,
        artifact: Value,
        receipt: Option<Value>,
        selfdev: Option<Value>,
        producer_text: String,
        intent: bool,
        child: Option<SessionId>,
        tool_receipt: Option<Value>,
    }

    fn temp_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("kian-self-verify-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = crate::cron_worktree::git_command()
            .arg("-C")
            .arg(repo)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("spawn git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn git_with_stdin(repo: &Path, args: &[&str], stdin: &[u8]) {
        use std::io::Write;
        let mut child = crate::cron_worktree::git_command()
            .arg("-C")
            .arg(repo)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn git");
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(stdin)
            .expect("write patch");
        let output = child.wait_with_output().expect("wait for git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn identity() -> [&'static str; 4] {
        [
            "-c",
            "user.name=Kian Test",
            "-c",
            "user.email=kian@example.test",
        ]
    }

    fn commit_args<'a>(message: &'a str, body: &'a str) -> Vec<&'a str> {
        let mut args: Vec<&str> = identity().to_vec();
        args.extend(["commit", "-m", message, "-m", body]);
        args
    }

    fn fixture(name: &str, gates: bool) -> Fixture {
        build(name, gates, false)
    }

    fn tool_fixture(name: &str) -> Fixture {
        build(name, true, true)
    }

    fn tool_plan() -> JobPlan {
        serde_json::from_value(json!({
            "id": JOB,
            "steps": [{
                "id": "patch",
                "prompt": "propose a patch",
                "produces": {"name": "patch", "max_bytes": 65536},
                "verify": [
                    {"check": "artifact_within_cap", "name": "patch"},
                    {
                        "check": "patch_paths_allowed",
                        "artifact": "patch",
                        "allow": ["src/**"],
                        "deny": []
                    }
                ]
            }, {
                "id": "publish",
                "prompt": "call the publish tool once; it takes no arguments",
                "depends_on": ["patch"],
                "inputs": [{"step": "patch", "name": "patch"}],
                "effect": "write",
                "retries": 0,
                "verify": [{"check": "publish_ok"}]
            }]
        }))
        .expect("a valid tool publish plan")
    }

    fn build(name: &str, gates: bool, tool: bool) -> Fixture {
        let root = temp_root(name);
        let repo = root.join("repo");
        std::fs::create_dir_all(repo.join("src")).expect("create repo");
        git(&repo, &["init", "-b", "main", "."]);
        std::fs::write(repo.join("src/lib.rs"), "old\n").expect("write base");
        let mut args = identity().to_vec();
        args.extend(["add", "src/lib.rs"]);
        git(&repo, &args);
        let body = "base";
        git(&repo, &commit_args("base", body));
        let base_commit = git(&repo, &["rev-parse", "HEAD"]);

        std::fs::write(repo.join("src/lib.rs"), "new\n").expect("write change");
        let patch = format!("{}\n", git(&repo, &["diff"]));
        git(&repo, &["checkout", "--", "src/lib.rs"]);
        assert!(patch.contains("+new"), "patch body: {patch}");

        let plan: JobPlan = serde_json::from_value(json!({
            "id": JOB,
            "steps": [{
                "id": "patch",
                "prompt": "propose a patch",
                "produces": {"name": "patch", "max_bytes": 65536},
                "verify": [
                    {"check": "artifact_within_cap", "name": "patch"},
                    {
                        "check": "patch_paths_allowed",
                        "artifact": "patch",
                        "allow": ["src/**"],
                        "deny": []
                    }
                ]
            }, {
                // ADR-084: a frozen replay fixture; new plans cannot declare this action.
                "id": "publish",
                "action": "publish",
                "depends_on": ["patch"],
                "inputs": [{"step": "patch", "name": "patch"}],
                "publish": {"base": "main", "gates": gates, "push": false, "pull_request": false},
                "effect": "write",
                "verify": [{"check": "publish_ok"}]
            }]
        }))
        .expect("a valid publish plan");
        let plan = if tool { tool_plan() } else { plan };

        let run = plan::run_session(JOB, SLOT);
        let producer = SessionId(uuid::Uuid::new_v4());
        let branch = publish_branch(run);
        let worktree = root.join("worktree");
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                &branch,
                worktree.to_str().unwrap(),
                &base_commit,
            ],
        );
        git_with_stdin(&worktree, &["apply", "--index", "-"], patch.as_bytes());
        let sha = plan::digest_text(&patch);
        let manifest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let trailers = format!(
            "Kian-Run: {}\nKian-Plan-Digest: {}\nKian-Patch-Sha256: {}\nKian-Pack: {}",
            plan::run_id(JOB, SLOT),
            plan.digest(),
            sha,
            manifest
        );
        git(&worktree, &commit_args(SUBJECT, &trailers));
        let commit = git(&worktree, &["rev-parse", "HEAD"]);

        let artifact = json!({
            "step": "patch",
            "name": "patch",
            "media": "text",
            "bytes": patch.len(),
            "sha256": sha,
            "ref": {"session": producer.0.to_string(), "seq": PRODUCER_SEQ},
            "producer": {"run": run.0.to_string(), "step": "patch", "attempt": 1}
        });
        let mut receipt = json!({
            "branch": branch,
            "worktree": worktree.to_str().unwrap(),
            "commit": commit
        });
        if gates {
            receipt["gate"] = json!({
                "command": "true",
                "exit_code": 0,
                "digest": sha,
                "output_tail": ""
            });
        }
        let selfdev = json!({
            "pack": {"dir": root.join("pack").to_str().unwrap(), "manifest_sha256": manifest, "files_sha256": manifest},
            "base_commit": base_commit,
            "kian_version": "0.3.0"
        });
        let db = root.join("kian.sqlite");
        let config = root.join("kian.yaml");
        let yaml = format!(
            r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{db}"
agent:
  id: verify
  model_slots:
    main:
      provider: mock
      model: mock
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: MOCK_KEY
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
selfdev:
  publish:
    repo: "{repo}"
    base: main
    gate: "true"
"#,
            db = db.display(),
            repo = repo.display()
        );
        std::fs::write(&config, yaml).expect("write config");

        let tool_receipt = if tool {
            Some(json!({"ok": true, "result": receipt.clone()}))
        } else {
            None
        };
        let child = if tool {
            Some(SessionId(uuid::Uuid::new_v4()))
        } else {
            None
        };
        let receipt = if tool { None } else { Some(receipt) };

        Fixture {
            repo,
            worktree,
            config,
            db,
            run,
            producer,
            base_commit,
            commit,
            branch,
            plan,
            plan_digest: None,
            patch: patch.clone(),
            artifact,
            receipt,
            selfdev: Some(selfdev),
            producer_text: patch,
            intent: !tool,
            child,
            tool_receipt,
        }
    }

    fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(1),
            ts: 1_760_000_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    async fn seed(fixture: &Fixture) {
        let store = EventStore::open(&fixture.db).await.expect("open store");
        let guard = |seq: u64| AppendGuard {
            epoch: Epoch(1),
            expected_seq: seq,
        };
        store
            .append(
                fixture.producer,
                event(
                    fixture.producer,
                    1,
                    EventKind::SessionCreated,
                    json!({"agent": "verify"}),
                ),
                guard(1),
            )
            .await
            .expect("seed producer created");
        store
            .append(
                fixture.producer,
                event(
                    fixture.producer,
                    PRODUCER_SEQ,
                    EventKind::AssistantMessage,
                    json!({"role": "assistant", "content": {"text": fixture.producer_text}}),
                ),
                guard(PRODUCER_SEQ),
            )
            .await
            .expect("seed producer message");

        let mut marker = status::marker_payload_with_selfdev(
            JOB,
            SLOT,
            "manual",
            &fixture.plan,
            fixture.selfdev.clone(),
        );
        if let Some(digest) = &fixture.plan_digest {
            marker["plan_digest"] = json!(digest);
        }
        store
            .append(
                fixture.run,
                event(
                    fixture.run,
                    1,
                    EventKind::InputAdmitted,
                    json!({"role": "user", "content": "verify the proposal", "job": marker}),
                ),
                guard(1),
            )
            .await
            .expect("seed run input");

        let artifact_call = format!("{ARTIFACT_CALL_PREFIX}patch:1");
        store
            .append(
                fixture.run,
                event(
                    fixture.run,
                    2,
                    EventKind::ToolCall,
                    json!({"tool": ARTIFACT_CALL_PREFIX.trim_end_matches(':'), "call_id": artifact_call, "args": {"artifact": fixture.artifact}}),
                ),
                guard(2),
            )
            .await
            .expect("seed artifact call");
        store
            .append(
                fixture.run,
                event(
                    fixture.run,
                    3,
                    EventKind::ToolResult,
                    json!({"call_id": artifact_call, "ok": true, "result": {"artifact": fixture.artifact}}),
                ),
                guard(3),
            )
            .await
            .expect("seed artifact result");

        if let Some(child) = fixture.child {
            store
                .append(
                    fixture.run,
                    event(
                        fixture.run,
                        4,
                        EventKind::ThreadStarted,
                        json!({
                            "thread_id": uuid::Uuid::new_v4().to_string(),
                            "worker": child.0.to_string(),
                            "job": {
                                "run": plan::run_id(JOB, SLOT),
                                "step": "publish",
                                "attempt": 1
                            }
                        }),
                    ),
                    guard(4),
                )
                .await
                .expect("seed dispatch record");
            store
                .append(
                    child,
                    event(
                        child,
                        1,
                        EventKind::ToolCall,
                        json!({
                            "role": "assistant",
                            "content": {"id": "call-1", "name": PUBLISH_AGENT_TOOL, "arguments": {}}
                        }),
                    ),
                    guard(1),
                )
                .await
                .expect("seed publish call");
            if let Some(output) = &fixture.tool_receipt {
                store
                    .append(
                        child,
                        event(
                            child,
                            2,
                            EventKind::ToolResult,
                            json!({"role": "tool", "content": {"call_id": "call-1", "output": output, "is_error": false}}),
                        ),
                        guard(2),
                    )
                    .await
                    .expect("seed publish receipt");
            }
        }

        if fixture.intent {
            store
                .append(
                    fixture.run,
                    event(
                        fixture.run,
                        4,
                        EventKind::ToolCall,
                        json!({
                            "tool": PUBLISH_TOOL,
                            "call_id": format!("{PUBLISH_CALL_PREFIX}publish:1"),
                            "args": {
                                "step": "publish",
                                "attempt": 1,
                                "from": fixture.artifact,
                                "base_commit": fixture.base_commit,
                                "branch": fixture.branch,
                                "worktree": fixture.worktree.to_str().unwrap()
                            }
                        }),
                    ),
                    guard(4),
                )
                .await
                .expect("seed publish intent");
        }
        if let Some(receipt) = &fixture.receipt {
            store
                .append(
                    fixture.run,
                    event(
                        fixture.run,
                        5,
                        EventKind::ToolResult,
                        json!({
                            "call_id": format!("{PUBLISH_CALL_PREFIX}publish:1"),
                            "ok": true,
                            "result": receipt
                        }),
                    ),
                    guard(5),
                )
                .await
                .expect("seed publish receipt");
        }
        store.close().await;
    }

    fn options(fixture: &Fixture, merged: bool) -> Options {
        Options {
            config: fixture.config.clone(),
            run: format!("{JOB}@{SLOT}"),
            base: None,
            merged,
        }
    }

    async fn verify(fixture: &Fixture, merged: bool) -> Report {
        seed(fixture).await;
        run(options(fixture, merged), false)
            .await
            .expect("the verifier runs")
    }

    async fn verify_json(fixture: &Fixture, merged: bool) -> Report {
        seed(fixture).await;
        run(options(fixture, merged), true)
            .await
            .expect("the verifier runs")
    }

    fn broken(report: &Report) -> (&str, &str) {
        assert!(!report.ok, "expected a broken chain: {}", report.stdout);
        assert!(report.stdout.is_empty(), "a failure prints no stdout");
        let line = report.stderr.trim_end();
        let rest = line
            .strip_prefix("audit chain broken at ")
            .unwrap_or_else(|| panic!("unexpected stderr: {line}"));
        let (link, detail) = rest.split_once(": ").expect("link and detail");
        (link, detail)
    }

    fn merge(fixture: &Fixture) {
        let mut args: Vec<&str> = identity().to_vec();
        args.extend([
            "merge",
            "--no-ff",
            "-m",
            "merge the proposal",
            &fixture.branch,
        ]);
        git(&fixture.repo, &args);
    }

    fn squash(fixture: &Fixture) {
        git(&fixture.repo, &["merge", "--squash", &fixture.branch]);
        git(&fixture.repo, &commit_args("squash the proposal", ""));
    }

    #[test]
    fn digests_are_sha256_plus_lowercase_hex() {
        assert!(is_digest(&plan::digest_text("x")));
        assert!(!is_digest("sha256:XYZ"));
        assert!(!is_digest(&format!("sha256:{}", "A".repeat(64))));
        assert!(!is_digest(&format!("sha256:{}", "a".repeat(63))));
        assert!(is_full_commit(&"a".repeat(40)));
        assert!(!is_full_commit(&"a".repeat(39)));
        assert!(!is_full_commit(&"g".repeat(40)));
    }

    #[test]
    fn trailers_take_the_last_value_on_a_line_boundary() {
        let message =
            "subject\n\nKian-Run: first\nKian-Patch-Sha256: sha256:aa\nKian-Run: second\n";
        assert_eq!(trailer(message, "Kian-Run"), Some("second"));
        assert_eq!(trailer(message, "Kian-Patch-Sha256"), Some("sha256:aa"));
        assert_eq!(trailer(message, "Kian-Pack"), None);
        assert_eq!(trailer("Kian-Run:no space", "Kian-Run"), Some("no space"));
    }

    #[tokio::test]
    async fn a_complete_chain_verifies() {
        let fixture = fixture("complete", false);
        let report = verify(&fixture, false).await;
        assert!(report.ok, "{}", report.stderr);
        assert!(report.stderr.is_empty());
        assert!(
            report
                .stdout
                .starts_with(&format!("run {}", plan::run_id(JOB, SLOT)))
        );
        for link in ["selfdev", "plan", "artifact", "publish", "commit", "branch"] {
            assert!(
                report.stdout.contains(&format!("{link}: ok (")),
                "missing {link}: {}",
                report.stdout
            );
        }
        assert!(!report.stdout.contains("merged: ok"));
    }

    #[tokio::test]
    async fn merged_passes_only_after_the_merge_commit() {
        let fixture = fixture("merged", false);
        seed(&fixture).await;
        let before = run(options(&fixture, true), false)
            .await
            .expect("the verifier runs");
        assert_eq!(broken(&before), ("merged", "not merged into main"));
        merge(&fixture);
        let report = run(options(&fixture, true), false)
            .await
            .expect("the verifier runs");
        assert!(report.ok, "{}", report.stderr);
        assert!(report.stdout.contains("merged: ok ("));
    }

    #[tokio::test]
    async fn a_squash_merge_is_reported_as_unmerged() {
        let fixture = fixture("squash", false);
        squash(&fixture);
        let report = verify(&fixture, true).await;
        assert_eq!(broken(&report), ("merged", "not merged into main"));
    }

    #[tokio::test]
    async fn a_deleted_branch_needs_merged() {
        let fixture = fixture("deleted", false);
        merge(&fixture);
        git(
            &fixture.repo,
            &[
                "worktree",
                "remove",
                "--force",
                fixture.worktree.to_str().expect("path"),
            ],
        );
        git(&fixture.repo, &["branch", "-D", &fixture.branch]);
        seed(&fixture).await;
        let strict = run(options(&fixture, false), false)
            .await
            .expect("the verifier runs");
        let (link, detail) = broken(&strict);
        assert_eq!(link, "branch");
        assert!(detail.contains("a missing branch is allowed only with --merged"));
        let merged = run(options(&fixture, true), false)
            .await
            .expect("the verifier runs");
        assert!(merged.ok, "{}", merged.stderr);
        assert!(merged.stdout.contains("branch: ok ("));
    }

    #[tokio::test]
    async fn a_tampered_producer_text_breaks_the_artifact_link() {
        let mut fixture = fixture("tampered-text", false);
        fixture.producer_text = "a different patch\n".to_string();
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "artifact");
        assert!(detail.contains("hashes to"), "{detail}");
    }

    #[tokio::test]
    async fn a_short_commit_breaks_the_publish_link() {
        let mut fixture = fixture("short-commit", false);
        fixture.receipt.as_mut().expect("receipt")["commit"] = json!("abc123");
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "publish");
        assert!(detail.contains("not a full 40-hex commit"), "{detail}");
    }

    #[tokio::test]
    async fn a_foreign_branch_breaks_the_publish_link() {
        let mut fixture = fixture("foreign-branch", false);
        fixture.receipt.as_mut().expect("receipt")["branch"] = json!("selfdev/someone-else");
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "publish");
        assert!(
            detail.contains("names branch selfdev/someone-else"),
            "{detail}"
        );
    }

    #[tokio::test]
    async fn a_failing_gate_breaks_the_publish_link() {
        let mut fixture = fixture("failing-gate", true);
        fixture.receipt.as_mut().expect("receipt")["gate"]["exit_code"] = json!(1);
        let report = verify(&fixture, false).await;
        assert_eq!(broken(&report), ("publish", "the gate exited 1, not 0"));
    }

    #[tokio::test]
    async fn a_tampered_plan_digest_breaks_the_plan_link() {
        let mut fixture = fixture("bad-plan", false);
        fixture.plan_digest = Some(plan::digest_text("not the frozen plan"));
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "plan");
        assert!(detail.contains("hashes to"), "{detail}");
    }

    #[tokio::test]
    async fn a_run_without_selfdev_breaks_the_selfdev_link() {
        let mut fixture = fixture("no-selfdev", false);
        fixture.selfdev = None;
        let report = verify(&fixture, false).await;
        assert_eq!(
            broken(&report),
            ("selfdev", "the admitted input row carries no job.selfdev")
        );
    }

    #[tokio::test]
    async fn a_malformed_recorded_digest_breaks_the_selfdev_link() {
        let mut fixture = fixture("bad-digest", false);
        fixture.selfdev.as_mut().expect("selfdev")["pack"]["manifest_sha256"] =
            json!("sha256:not-hex");
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "selfdev");
        assert!(detail.contains("manifest_sha256"), "{detail}");
    }

    #[tokio::test]
    async fn a_run_without_a_publish_receipt_breaks_the_publish_link() {
        let mut fixture = fixture("no-receipt", false);
        fixture.receipt = None;
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "publish");
        assert!(detail.contains("no job.publish receipt"), "{detail}");
    }

    #[tokio::test]
    async fn a_mismatched_trailer_breaks_the_commit_link() {
        let mut fixture = fixture("trailer", false);
        let trailers = format!(
            "Kian-Run: someone-else\nKian-Plan-Digest: {}\nKian-Patch-Sha256: {}\nKian-Pack: {}",
            fixture.plan.digest(),
            plan::digest_text(&fixture.patch),
            fixture.selfdev.as_ref().expect("selfdev")["pack"]["manifest_sha256"]
                .as_str()
                .expect("manifest")
        );
        let mut amend: Vec<&str> = identity().to_vec();
        amend.extend(["commit", "--amend", "-m", SUBJECT, "-m", &trailers]);
        git(&fixture.worktree, &amend);
        let commit = git(&fixture.worktree, &["rev-parse", "HEAD"]);
        fixture.commit = commit.clone();
        fixture.receipt.as_mut().expect("receipt")["commit"] = json!(commit);
        let report = verify(&fixture, false).await;
        let (link, detail) = broken(&report);
        assert_eq!(link, "commit");
        assert!(
            detail.contains("Kian-Run trailer is someone-else"),
            "{detail}"
        );
    }

    #[tokio::test]
    async fn json_reports_the_links_and_the_merge() {
        let fixture = fixture("json", false);
        merge(&fixture);
        let report = verify_json(&fixture, true).await;
        assert!(report.ok, "{}", report.stderr);
        let body: Value = serde_json::from_str(&report.stdout).expect("json report");
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["run"], json!(plan::run_id(JOB, SLOT)));
        assert_eq!(body["merged"], json!(true));
        let links = body["links"].as_array().expect("links");
        assert_eq!(links.len(), 7);
        assert_eq!(links[6]["link"], json!("merged"));
        assert_eq!(links[6]["ok"], json!(true));
    }

    #[tokio::test]
    async fn landing_is_not_configured_without_a_publish_section() {
        let fixture = fixture("unconfigured", false);
        let yaml = std::fs::read_to_string(&fixture.config).expect("read config");
        let head = yaml
            .split("selfdev:")
            .next()
            .expect("config has a selfdev section");
        std::fs::write(&fixture.config, head).expect("rewrite config");
        let error = run(options(&fixture, false), false)
            .await
            .expect_err("landing is not configured");
        assert_eq!(error, "landing is not configured");
    }

    #[tokio::test]
    async fn a_tool_published_run_verifies_from_the_child_receipt() {
        let fixture = tool_fixture("tool-complete");
        let report = verify(&fixture, false).await;
        assert!(report.ok, "{}", report.stderr);
        assert!(report.stdout.contains("publish: ok ("), "{}", report.stdout);
        assert!(!report.stdout.contains("job.publish"));
    }

    #[tokio::test]
    async fn a_tool_run_without_a_receipt_breaks_the_publish_link() {
        let mut fixture = tool_fixture("tool-no-receipt");
        fixture.tool_receipt = None;
        let report = verify(&fixture, false).await;
        assert_eq!(
            broken(&report),
            (
                "publish",
                "the publish call has no receipt: the worktree may exist and an operator must inspect"
            )
        );
    }
}
