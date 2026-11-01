//! C48 github-channel integration tests: webhook-body fixtures mapped
//! through the adapter, and send proven against a scripted in-process
//! HTTP mock of the GitHub REST API — zero credentials, zero network.
//! The live smoke test at the bottom is env-gated
//! (`KIAN_GITHUB_TOKEN`), `#[ignore]`d out of CI, and skips (never
//! fails) without the token.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Json;
use axum::extract::{FromRequest, Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_core::seams::{ChannelAdapter, ChannelDelivery};
use kian_orchestrator::WakeOutcome;
use kian_superagent::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
use kian_superagent::rules::{CorrelateBy, Route, RuleMatcher};
use kian_superagent::{
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, GitHubChannel, GitHubConfig,
    SubscriptionRule, SubscriptionRuleSet, SubscriptionService,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The scripted GitHub mock: an in-process axum server capturing every
// REST call (method, path, auth, body) and answering from canned
// state (no credentials — the "token" is a fixture string).
// ---------------------------------------------------------------------------

/// One captured REST call: (method, path, auth, api version, body).
type CapturedCall = (String, String, String, String, Value);

struct GitHubMock {
    calls: Mutex<Vec<CapturedCall>>,
    /// The (status, json) the API answers with, when scripted.
    respond: Mutex<Option<(u16, Value)>>,
}

impl GitHubMock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            respond: Mutex::new(None),
        })
    }

    fn calls(&self) -> Vec<(String, String, String, String, Value)> {
        self.calls.lock().unwrap().clone()
    }

    fn reject_with(&self, status: u16, body: Value) {
        *self.respond.lock().unwrap() = Some((status, body));
    }

    async fn serve(self: Arc<Self>) -> String {
        let app = axum::Router::new()
            .fallback(capture_endpoint)
            .with_state(self);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("mock serve") });
        format!("http://{addr}")
    }
}

async fn capture_endpoint(State(state): State<Arc<GitHubMock>>, req: Request) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let api_version = req
        .headers()
        .get("x-github-api-version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = axum::body::Bytes::from_request(req, &state)
        .await
        .expect("mock body");
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    state
        .calls
        .lock()
        .unwrap()
        .push((method, path, auth, api_version, body));
    let (status, body) = state
        .respond
        .lock()
        .unwrap()
        .clone()
        .unwrap_or((201, json!({"id": 4242, "node_id": "R_1"})));
    (
        StatusCode::from_u16(status).expect("mock status"),
        Json(body),
    )
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

fn github_config(api_base: &str) -> GitHubConfig {
    GitHubConfig {
        api_base: api_base.to_string(),
        topics: BTreeMap::from([("flyer103/kian".to_string(), "ci".to_string())]),
        events: BTreeSet::new(),
        untrusted: true,
        send: true,
        token: Some("ghp_mock".into()),
    }
}

fn send_config(api_base: &str) -> GitHubConfig {
    github_config(api_base)
}

/// A `sender`-bearing webhook body for `flyer103/kian`.
fn delivery_body() -> Value {
    json!({
        "action": "created",
        "issue": {
            "number": 9,
            "title": "build broken on main",
            "body": "the CI log shows a panic",
            "user": {"login": "reporter"}
        },
        "comment": {
            "id": 99,
            "body": "please take a look",
            "user": {"login": "reporter"}
        },
        "repository": {"id": 1, "full_name": "flyer103/kian", "name": "kian"},
        "sender": {"login": "reporter", "type": "User", "id": 7}
    })
}

/// The macOS system proxy intercepts localhost (AGENTS.md gotcha).
fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }
}

/// Awaits `f` for at most `secs` — bounded waits, never sleep-races.
async fn within<T>(secs: u64, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(value) = f() {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Receive: fixture webhook body → ChannelEvent (via ingest, the C49
// receiver's hand-off point).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_delivery_ingests_as_a_provenance_carrying_event() {
    let channel = GitHubChannel::new("github", github_config("https://api.github.invalid"))
        .expect("valid config");
    let mut stream = channel.subscribe();
    let surfaced = channel.ingest_delivery("issue_comment", "d-abc123", &delivery_body());
    assert_eq!(surfaced, 1);
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event within 5s")
        .expect("stream open");

    // The full mapping contract: channel, delivery id as the
    // idempotency key, topic from the mapping table, repo → group,
    // verbatim sender object, verbatim payload.
    assert_eq!(event.channel, "github");
    assert_eq!(event.event_id, "d-abc123");
    assert_eq!(event.topic.as_deref(), Some("ci"));
    assert_eq!(event.group.as_deref(), Some("flyer103/kian"));
    assert_eq!(
        event.sender,
        Some(json!({"login": "reporter", "type": "User", "id": 7}))
    );
    assert_eq!(
        event.payload,
        delivery_body(),
        "verbatim, no adapter fields"
    );
    assert!(event.ts_ms > 0);
}

#[tokio::test]
async fn delivery_id_idempotency_across_ingests_and_subscribers() {
    let channel = GitHubChannel::new("github", github_config("https://api.github.invalid"))
        .expect("valid config");
    let mut s1 = channel.subscribe();
    let mut s2 = channel.subscribe();

    assert_eq!(
        channel.ingest_delivery("issue_comment", "d-dup", &delivery_body()),
        1
    );
    for stream in [&mut s1, &mut s2] {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("event within 5s")
            .expect("stream open");
        assert_eq!(event.event_id, "d-dup");
    }

    // The same delivery id redelivered (GitHub retries) surfaces on
    // neither stream: one durable row per delivery, never two.
    assert_eq!(
        channel.ingest_delivery("issue_comment", "d-dup", &delivery_body()),
        0
    );
    for stream in [&mut s1, &mut s2] {
        let none = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            none.is_err() || none.unwrap().is_none(),
            "a redelivered id must not resurface"
        );
    }

    // A genuinely new delivery surfaces normally.
    assert_eq!(
        channel.ingest_delivery("issues", "d-new", &delivery_body()),
        1
    );
    let fresh = tokio::time::timeout(Duration::from_secs(5), s1.next())
        .await
        .expect("fresh within 5s")
        .expect("s1 open");
    assert_eq!(fresh.event_id, "d-new");
}

#[tokio::test]
async fn events_filter_unsupported_types_and_unmapped_repos_never_surface() {
    let mut config = github_config("https://api.github.invalid");
    // Filter: only the CI train (check_run/status) — the group-chat
    // family (issues/comments/PRs) is deliberately out.
    config.events = BTreeSet::from(["check_run".to_string(), "status".to_string()]);
    let channel = GitHubChannel::new("github", config).expect("valid config");
    let mut stream = channel.subscribe();

    // Filtered out…
    assert_eq!(
        channel.ingest_delivery("issue_comment", "d-f1", &delivery_body()),
        0
    );
    // …unsupported…
    assert_eq!(
        channel.ingest_delivery("deployment", "d-f2", &delivery_body()),
        0
    );
    // …unmapped repo (the mapping table is the receive allowlist)…
    let mut stranger = delivery_body();
    stranger["repository"]["full_name"] = json!("someone/else");
    assert_eq!(channel.ingest_delivery("check_run", "d-f3", &stranger), 0);

    // …while an admitted CI event surfaces with the same provenance
    // contract.
    let check_run = json!({
        "action": "completed",
        "check_run": {"id": 5, "conclusion": "failure", "output": {"title": "tests failed"}},
        "repository": {"full_name": "flyer103/kian"},
        "sender": {"login": "github-actions", "type": "Bot"}
    });
    assert_eq!(channel.ingest_delivery("check_run", "d-ok", &check_run), 1);
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("event within 5s")
        .expect("stream open");
    assert_eq!(event.event_id, "d-ok");
    assert_eq!(event.topic.as_deref(), Some("ci"));
    assert_eq!(event.sender.as_ref().unwrap()["login"], "github-actions");

    let none = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    assert!(none.is_err() || none.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Send: resolves only on API acceptance; every other outcome is an
// honest error.
// ---------------------------------------------------------------------------

fn sections() -> Value {
    json!({
        "background": "flaky test on linux",
        "changes": "retry with backoff",
        "verification": "cargo test green"
    })
}
const RENDERED_SECTIONS: &str = "## background\n\nflaky test on linux\n\n## changes\n\nretry \
     with backoff\n\n## verification\n\ncargo test green";

#[tokio::test]
async fn send_resolves_only_after_the_api_accepts() {
    force_no_proxy();
    let mock = GitHubMock::new();
    let base = mock.clone().serve().await;
    let channel = GitHubChannel::new("github", send_config(&base)).expect("valid config");

    // Issue comment: POST /repos/{o}/{r}/issues/{n}/comments.
    let receipt = channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "issue_comment"
            }),
            content: sections(),
        })
        .await
        .expect("send accepted");
    assert_eq!(receipt.message_id, "4242");
    assert!(receipt.ts_ms > 0);
    let calls = mock.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "POST");
    assert_eq!(calls[0].1, "/repos/flyer103/kian/issues/9/comments");
    assert_eq!(calls[0].2, "Bearer ghp_mock");
    assert_eq!(calls[0].3, "2022-11-28");
    // The §4.2 structured sections render as markdown sections.
    assert_eq!(calls[0].4["body"], RENDERED_SECTIONS);

    // PR review comment: POST /repos/{o}/{r}/pulls/{n}/reviews.
    channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "pr_review"
            }),
            content: json!("plain string content passes through"),
        })
        .await
        .expect("send accepted");
    let calls = mock.calls();
    assert_eq!(calls[1].0, "POST");
    assert_eq!(calls[1].1, "/repos/flyer103/kian/pulls/9/reviews");
    assert_eq!(calls[1].4["body"], "plain string content passes through");
    assert_eq!(calls[1].4["event"], "COMMENT");

    // PR description: PATCH /repos/{o}/{r}/pulls/{n}.
    channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "pr_description"
            }),
            content: sections(),
        })
        .await
        .expect("send accepted");
    let calls = mock.calls();
    assert_eq!(calls[2].0, "PATCH");
    assert_eq!(calls[2].1, "/repos/flyer103/kian/pulls/9");
    assert_eq!(calls[2].4["body"], RENDERED_SECTIONS);

    // Rejection → an honest error naming the HTTP status and the
    // API's message, never a fabricated receipt.
    mock.reject_with(422, json!({"message": "Validation Failed"}));
    let err = channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "issue_comment"
            }),
            content: sections(),
        })
        .await
        .expect_err("rejection must error");
    assert!(matches!(
        err,
        kian_core::seams::ChannelError::DeliveryFailed(_)
    ));
    assert!(
        err.to_string().contains("422") && err.to_string().contains("Validation Failed"),
        "the honest error names status and message: {err}"
    );

    // Accepted status without an id is not acceptance.
    mock.reject_with(200, json!({"message": "no id here"}));
    let err = channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "pr_description"
            }),
            content: sections(),
        })
        .await
        .expect_err("id-less acceptance must error");
    assert!(matches!(
        err,
        kian_core::seams::ChannelError::DeliveryFailed(_)
    ));
}

#[tokio::test]
async fn receive_only_channel_and_degenerate_targets_error_before_any_wire() {
    force_no_proxy();
    let mock = GitHubMock::new();
    let base = mock.clone().serve().await;

    // Receive-only construction is fine without a token…
    let mut config = send_config(&base);
    config.send = false;
    config.token = None;
    let receive_only = GitHubChannel::new("github", config).expect("receive-only config");
    assert!(!receive_only.config().send);
    // …but send is an honest error, and no wire traffic is spent.
    let err = receive_only
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "issue_comment"
            }),
            content: sections(),
        })
        .await
        .expect_err("send without a token must error");
    assert!(matches!(
        err,
        kian_core::seams::ChannelError::DeliveryFailed(_)
    ));
    assert!(err.to_string().contains("KIAN_GITHUB_TOKEN"));
    assert!(
        mock.calls().is_empty(),
        "no wire traffic for a tokenless send"
    );

    // Degenerate targets/contents error before any wire traffic too.
    let channel = GitHubChannel::new("github", send_config(&base)).expect("valid config");
    for bad in [
        json!("not an object"),
        json!({}),
        json!({"owner": "", "repo": "kian", "number": 9, "kind": "issue_comment"}),
        json!({"owner": "flyer103", "repo": "kian", "number": 0, "kind": "issue_comment"}),
        json!({"owner": "flyer103", "repo": "kian", "number": 9, "kind": "merge"}),
    ] {
        let err = channel
            .send(ChannelDelivery {
                channel: "github".into(),
                target: bad.clone(),
                content: sections(),
            })
            .await
            .expect_err("bad target must error");
        assert!(
            matches!(err, kian_core::seams::ChannelError::DeliveryFailed(_)),
            "target `{bad}`"
        );
    }
    let err = channel
        .send(ChannelDelivery {
            channel: "github".into(),
            target: json!({
                "owner": "flyer103",
                "repo": "kian",
                "number": 9,
                "kind": "issue_comment"
            }),
            content: json!(42),
        })
        .await
        .expect_err("non-renderable content must error");
    assert!(matches!(
        err,
        kian_core::seams::ChannelError::DeliveryFailed(_)
    ));
    assert!(
        mock.calls().is_empty(),
        "no wire traffic on degenerate shapes"
    );
}

// ---------------------------------------------------------------------------
// The full subscription path: classes + rules + provenance, with a
// real github event flowing through the adapter.
// ---------------------------------------------------------------------------

/// The scripted inbox from the lark tests: records every admit
/// (session, payload, idempotency key) and wake.
struct RecordingInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl RecordingInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

#[tokio::test]
async fn github_event_flows_through_classes_rules_and_provenance_untrusted() {
    let router = SessionId(uuid::Uuid::new_v4());
    let inbox = RecordingInbox::new();
    let service = Arc::new(
        SubscriptionService::with_classes(
            router,
            SubscriptionRuleSet::new(vec![SubscriptionRule {
                channel: "github".into(),
                matcher: RuleMatcher::Topic("ci".into()),
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            }]),
            AdmissionClassTable::new(vec![AdmissionClassRule {
                channel: "github".into(),
                topic: Some("ci".into()),
                sender: None,
                class: AdmissionClass::ExplicitOnly,
            }]),
            Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        )
        // C43/C48: github is default-untrusted — assembly passes the
        // channel name in; the wrapper gains the bit.
        .with_untrusted_channels(BTreeSet::from(["github".to_string()])),
    );

    let channel = Arc::new(
        GitHubChannel::new("github", github_config("https://api.github.invalid"))
            .expect("valid config"),
    );
    let _consumers = service.spawn(vec![Arc::clone(&channel) as Arc<dyn ChannelAdapter>]);

    // A mention-carrying comment (the explicit-request signal) — the
    // adapter declares `explicit: true` from the @-token in the body.
    let mut mention = delivery_body();
    mention["comment"]["body"] = json!("@kian-agent please fix the build");
    assert_eq!(
        channel.ingest_delivery("issue_comment", "d-yes", &mention),
        1
    );
    // A second, mentionless comment: explicit-only must drop it before
    // any rule, any row, any wake.
    assert_eq!(
        channel.ingest_delivery("issue_comment", "d-no", &delivery_body()),
        1
    );

    // Exactly one admit must land (the mention) — wait for it, then
    // give the second (mentionless) event a clear window to be wrongly
    // admitted before freezing the assertions.
    within(5, || {
        let admits = inbox.admits.lock().unwrap();
        (!admits.is_empty()).then(|| admits[0].clone())
    })
    .await
    .expect("one admit within 5s");
    tokio::time::sleep(Duration::from_millis(250)).await;

    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(
        admits.len(),
        1,
        "explicit-only dropped the mentionless delivery before any row: {admits:?}"
    );
    let (target, payload, key) = &admits[0];
    assert_eq!(*target, router); // empty-set passthrough fallback
    assert_eq!(key.as_deref(), Some("d-yes")); // delivery-id idempotency key
    assert_eq!(inbox.wakes.lock().unwrap().len(), 1);

    // Provenance: the wrapper is the C33/C43 `{"kian":…}` channel-event
    // wrapper with the untrusted bit set, and the verbatim github raw
    // (with the adapter-declared explicit flag) inside.
    let kian = &payload["kian"];
    assert_eq!(kian["type"], "channel_event");
    assert_eq!(kian["channel"], "github");
    assert_eq!(kian["event_id"], "d-yes");
    assert_eq!(kian["topic"], "ci");
    assert_eq!(kian["untrusted"], json!(true));
    assert_eq!(kian["raw"]["repository"]["full_name"], "flyer103/kian");
    assert_eq!(kian["raw"]["sender"]["login"], "reporter");
    assert_eq!(kian["raw"]["explicit"], json!(true));
}

// ---------------------------------------------------------------------------
// Live smoke — env-gated, CI-excluded, skips (never fails) without the
// token.
// ---------------------------------------------------------------------------

/// One manual pass against the real GitHub REST API. Skips (does not
/// fail) without credentials:
///
/// ```text
/// KIAN_GITHUB_TOKEN=ghp_xxx KIAN_GITHUB_SEND_TARGET='{"owner":"o","repo":"r","number":1,"kind":"issue_comment"}' \
/// cargo test -p kian-superagent --test github_channel -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "env-gated: set KIAN_GITHUB_TOKEN (and optionally KIAN_GITHUB_SEND_TARGET) and run with --ignored"]
async fn github_live_smoke_env_gated() {
    let Ok(token) = std::env::var("KIAN_GITHUB_TOKEN") else {
        eprintln!(
            "skipping: KIAN_GITHUB_TOKEN not set (live github smoke is manual, recorded run)"
        );
        return;
    };
    let config = GitHubConfig {
        api_base: kian_superagent::GITHUB_DEFAULT_API_BASE.into(),
        topics: BTreeMap::from([("flyer103/kian".to_string(), "ci".to_string())]),
        events: BTreeSet::new(),
        untrusted: true,
        send: true,
        token: Some(token),
    };
    config.validate().expect("live config validates");
    let channel = GitHubChannel::new("github", config).expect("live channel builds");

    match std::env::var("KIAN_GITHUB_SEND_TARGET").ok() {
        Some(target) => {
            let target: Value =
                serde_json::from_str(&target).expect("KIAN_GITHUB_SEND_TARGET is JSON");
            let receipt = channel
                .send(ChannelDelivery {
                    channel: "github".into(),
                    target,
                    content: json!("kian github live smoke (C48)"),
                })
                .await
                .expect("live send accepted");
            eprintln!("live send ok: id={}", receipt.message_id);
        }
        None => eprintln!("live send skipped: KIAN_GITHUB_SEND_TARGET not set"),
    }
}
