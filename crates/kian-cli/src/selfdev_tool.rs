//! The publish tool (SELF-EVOLUTION-MODEL §9.4, ADR-084 migration): a worker
//! child of a job run asks this process to publish the patch its run already
//! committed. The call carries no coordinates — the run, step, attempt, patch
//! bytes and base commit are all derived from the committed streams — so the
//! model can never name a repository, branch or worktree.

use std::sync::Arc;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::{ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema};
use kian_store::EventStore;
use kian_superagent::job::conduct::{
    self, PUBLISH_AGENT_TOOL, PublishError, PublishPort, PublishRequest,
};
use kian_superagent::job::plan::JobCheck;
use kian_superagent::job::status;
use kian_superagent::worker::WorkerStreamsPort;
use serde_json::{Value, json};

use crate::config::SelfdevPublishSection;
use crate::selfdev_publish::GitPublishPort;
use crate::superagent::CombinedTools;

/// The one tool a configured self-dev deployment adds to its worker loop.
pub(crate) const PUBLISH_TOOL_NAME: &str = PUBLISH_AGENT_TOOL;

fn is_full_commit(commit: &str) -> bool {
    commit.len() == 40 && commit.chars().all(|c| c.is_ascii_hexdigit())
}

/// The publish tool bound to one worker session: the caller's identity is the
/// authority, never an argument.
pub(crate) struct PublishTool {
    store: EventStore,
    streams: Arc<dyn WorkerStreamsPort>,
    publish: SelfdevPublishSection,
    session: SessionId,
}

impl PublishTool {
    pub(crate) fn new(
        store: EventStore,
        streams: Arc<dyn WorkerStreamsPort>,
        publish: SelfdevPublishSection,
        session: SessionId,
    ) -> Self {
        Self {
            store,
            streams,
            publish,
            session,
        }
    }
}

impl ToolProvider for PublishTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: PUBLISH_TOOL_NAME.to_string(),
            description: "Publish this job step's patch: the repository, branch, worktree, base commit and patch bytes are derived from the run's committed events. Takes no arguments. Call it at most once; a call whose receipt is missing is an unresolved write an operator must inspect.".to_string(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let store = self.store.clone();
        let streams = Arc::clone(&self.streams);
        let publish = self.publish.clone();
        let session = self.session;
        Box::pin(async move { Ok(run(&store, streams, &publish, session, call).await) })
    }
}

/// Mounts [`PublishTool`] over a worker template's tool set once the operator
/// configured `selfdev.publish`; every other deployment keeps the unchanged
/// base provider.
pub(crate) struct SelfdevToolScope {
    base: Arc<dyn ToolProvider>,
    store: EventStore,
    streams: Arc<dyn WorkerStreamsPort>,
    publish: SelfdevPublishSection,
}

impl SelfdevToolScope {
    pub(crate) fn new(
        base: Arc<dyn ToolProvider>,
        store: EventStore,
        publish: SelfdevPublishSection,
    ) -> Self {
        let streams = crate::superagent::StoreWorkerStreams::new(store.clone());
        Self {
            base,
            store,
            streams,
            publish,
        }
    }
}

impl kian_loop::ToolScope for SelfdevToolScope {
    fn tools_for(&self, session: SessionId) -> Arc<dyn ToolProvider> {
        let tool: Arc<dyn ToolProvider> = Arc::new(PublishTool::new(
            self.store.clone(),
            Arc::clone(&self.streams),
            self.publish.clone(),
            session,
        ));
        Arc::new(CombinedTools::new(vec![Arc::clone(&self.base), tool]))
    }
}

fn refusal(call_id: String, error: impl Into<String>) -> ToolResult {
    ToolResult {
        call_id,
        output: json!({"ok": false, "error": error.into()}),
        is_error: true,
        redactions: Vec::new(),
    }
}

async fn run(
    store: &EventStore,
    streams: Arc<dyn WorkerStreamsPort>,
    publish: &SelfdevPublishSection,
    session: SessionId,
    call: ToolCall,
) -> ToolResult {
    let call_id = call.id.clone();
    if !call
        .arguments
        .as_object()
        .is_some_and(|arguments| arguments.is_empty())
    {
        return refusal(
            call_id,
            "the publish tool takes no arguments: the run, step, attempt and patch are derived from the committed streams",
        );
    }
    let child = match store.rebuild(session).await {
        Ok(events) => events,
        Err(error) => {
            return refusal(
                call_id,
                format!("the calling session's stream is unreadable: {error}"),
            );
        }
    };
    // The loop commits this call before invoking the tool, so every earlier
    // publish call is a prior attempt: a receipt replays, a bare intent blocks.
    let prior = conduct::publish_tool_calls(&child)
        .into_iter()
        .filter(|candidate| candidate.call_id != call_id)
        .collect::<Vec<_>>();
    if let Some(last) = prior.last() {
        return match &last.result {
            Some(result) => ToolResult {
                call_id,
                is_error: result.get("ok").and_then(Value::as_bool) != Some(true),
                output: result.clone(),
                redactions: Vec::new(),
            },
            None => refusal(
                call_id,
                "a prior publish attempt is unresolved: the worktree or branch may exist and what landed is unknown; an operator must inspect before another publish",
            ),
        };
    }
    let Some(run_session) = conduct::child_run_session(&child) else {
        return refusal(
            call_id,
            "this session is not a job child: publish runs only from a dispatched job step",
        );
    };
    let run_events = match store.rebuild(run_session).await {
        Ok(events) => events,
        Err(error) => return refusal(call_id, format!("the run's stream is unreadable: {error}")),
    };
    let marker = match status::marker(&run_events) {
        Some(Ok(marker)) => marker,
        Some(Err(error)) => {
            return refusal(
                call_id,
                format!("the run carries no authoritative plan: {error}"),
            );
        }
        None => return refusal(call_id, "the run carries no job marker"),
    };
    let Some(selfdev) = marker.selfdev.as_ref() else {
        return refusal(call_id, "this run is not a self-dev run");
    };
    let base_commit = selfdev
        .get("base_commit")
        .and_then(Value::as_str)
        .filter(|commit| is_full_commit(commit))
        .map(str::to_string);
    let Some(base_commit) = base_commit else {
        return refusal(
            call_id,
            "the run's frozen selfdev.base_commit is missing or not a full commit",
        );
    };
    let Some(dispatch) = status::dispatches(&run_events)
        .into_iter()
        .rev()
        .find(|record| record.child == session)
    else {
        return refusal(
            call_id,
            "this session is not dispatched by the run: publish has no step authority",
        );
    };
    let Some(step) = marker.plan.step(&dispatch.step) else {
        return refusal(
            call_id,
            format!(
                "the dispatched step {:?} is not in the frozen plan",
                dispatch.step
            ),
        );
    };
    if !step
        .verify
        .iter()
        .any(|check| matches!(check, JobCheck::PublishOk))
    {
        return refusal(
            call_id,
            format!(
                "step {:?} does not declare publish_ok: publish is not that step's action",
                step.id
            ),
        );
    }
    let [input] = step.inputs.as_slice() else {
        return refusal(
            call_id,
            format!("step {:?} names no single input patch to publish", step.id),
        );
    };
    let Some(artifact) = conduct::artifact_intent(&run_events, &input.step, &input.name) else {
        return refusal(
            call_id,
            format!(
                "the run carries no committed artifact {} from step {} to publish",
                input.name, input.step
            ),
        );
    };
    let patch = match conduct::verified_artifact_text(streams.as_ref(), &artifact).await {
        Ok(text) => text,
        Err((kind, detail)) => return refusal(call_id, format!("{kind}: {detail}")),
    };
    let port = GitPublishPort::from_config(publish);
    let Some(targets) = port.derive(run_session) else {
        return refusal(call_id, conduct::PUBLISH_NOT_CONFIGURED);
    };
    let request = PublishRequest {
        step: dispatch.step.clone(),
        attempt: dispatch.attempt,
        from: artifact.clone(),
        patch,
        base_commit,
        branch: conduct::publish_branch(run_session),
        worktree: targets.worktree,
        subject: format!("selfdev: the proposal from run {}", run_session.0),
        trailers: conduct::publish_trailers(&marker, &artifact),
        base: publish.base.clone(),
        gates: publish.gates,
        push: publish.push,
        pull_request: publish.pull_request,
    };
    match port.publish(request).await {
        Ok(receipt) => ToolResult {
            call_id,
            output: json!({
                "ok": true,
                "step": dispatch.step,
                "attempt": dispatch.attempt,
                "result": conduct::publish_receipt_json(&receipt),
            }),
            is_error: false,
            redactions: Vec::new(),
        },
        Err(error) => {
            let needs_operator = matches!(
                error,
                PublishError::Occupied(_) | PublishError::Unknown(_) | PublishError::Partial(_)
            );
            refusal(
                call_id,
                format!(
                    "{}{}",
                    error.detail(),
                    if needs_operator {
                        " (needs operator: what landed is unknown)"
                    } else {
                        ""
                    }
                ),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::Epoch;
    use kian_loop::ToolScope;
    use kian_store::AppendGuard;

    struct EmptyTools;

    impl ToolProvider for EmptyTools {
        fn schemas(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                name: "read".to_string(),
                description: "read a file".to_string(),
                parameters: json!({"type": "object"}),
            }]
        }

        fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
            let call_id = call.id;
            Box::pin(async move {
                Ok(ToolResult {
                    call_id,
                    output: json!({}),
                    is_error: false,
                    redactions: Vec::new(),
                })
            })
        }
    }

    fn temp_db(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kian-publish-tool-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("kian.sqlite")
    }

    fn publish_config(db: &std::path::Path) -> SelfdevPublishSection {
        let repo = db.parent().expect("db parent").join("repo");
        serde_yaml::from_str(&format!("repo: \"{}\"", repo.display())).expect("publish config")
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

    async fn append(
        store: &EventStore,
        session: SessionId,
        seq: u64,
        kind: EventKind,
        bytes: Value,
    ) {
        store
            .append(
                session,
                event(session, seq, kind, bytes),
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: seq,
                },
            )
            .await
            .expect("append event");
    }

    fn call(id: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            name: PUBLISH_TOOL_NAME.to_string(),
            arguments,
        }
    }

    #[tokio::test]
    async fn the_scope_mounts_the_publish_tool_beside_the_base_tools() {
        let db = temp_db("scope");
        let store = EventStore::open(&db).await.expect("open store");
        let scope = SelfdevToolScope::new(Arc::new(EmptyTools), store, publish_config(&db));
        let names = scope
            .tools_for(SessionId(uuid::Uuid::new_v4()))
            .schemas()
            .into_iter()
            .map(|schema| schema.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"read".to_string()), "{names:?}");
        assert!(names.contains(&PUBLISH_TOOL_NAME.to_string()), "{names:?}");
    }

    #[tokio::test]
    async fn the_tool_refuses_coordinates_and_a_caller_without_run_authority() {
        let db = temp_db("refuse");
        let store = EventStore::open(&db).await.expect("open store");
        let session = SessionId(uuid::Uuid::new_v4());
        let streams: Arc<dyn WorkerStreamsPort> =
            crate::superagent::StoreWorkerStreams::new(store.clone());
        let tool = PublishTool::new(store, streams, publish_config(&db), session);

        let result = tool
            .invoke(call("c1", json!({"repo": "/elsewhere"})))
            .await
            .expect("invoke");
        assert!(result.is_error);
        assert!(
            result.output["error"]
                .as_str()
                .is_some_and(|error| error.contains("takes no arguments")),
            "{}",
            result.output
        );

        let result = tool.invoke(call("c2", json!({}))).await.expect("invoke");
        assert!(result.is_error);
        assert!(
            result.output["error"]
                .as_str()
                .is_some_and(|error| error.contains("not a job child")),
            "{}",
            result.output
        );
    }

    #[tokio::test]
    async fn an_unresolved_publish_blocks_and_a_receipt_replays() {
        let db = temp_db("resume");
        let store = EventStore::open(&db).await.expect("open store");
        let session = SessionId(uuid::Uuid::new_v4());
        let run = SessionId(uuid::Uuid::new_v4());
        append(
            &store,
            session,
            1,
            EventKind::ThreadStarted,
            json!({"worker": session.0.to_string(), "parent": run.0.to_string()}),
        )
        .await;
        append(
            &store,
            session,
            2,
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c0", "name": PUBLISH_TOOL_NAME, "arguments": {}}}),
        )
        .await;
        let streams: Arc<dyn WorkerStreamsPort> =
            crate::superagent::StoreWorkerStreams::new(store.clone());
        let tool = PublishTool::new(store.clone(), streams, publish_config(&db), session);

        let blocked = tool.invoke(call("c1", json!({}))).await.expect("invoke");
        assert!(blocked.is_error);
        assert!(
            blocked.output["error"]
                .as_str()
                .is_some_and(|error| error.contains("a prior publish attempt is unresolved")),
            "{}",
            blocked.output
        );

        let receipt =
            json!({"ok": true, "result": {"branch": "selfdev/x", "commit": "a".repeat(40)}});
        append(
            &store,
            session,
            3,
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c0", "output": receipt, "is_error": false}}),
        )
        .await;

        let replayed = tool.invoke(call("c2", json!({}))).await.expect("invoke");
        assert!(!replayed.is_error);
        assert_eq!(replayed.output, receipt);
    }
}
