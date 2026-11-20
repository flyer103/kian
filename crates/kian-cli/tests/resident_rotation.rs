//! ADR-040 phase 0: boot-time resident rotation.
//!
//! A pre-seeded idle Router stream over the threshold is retired by the
//! assembly before any service binds: the predecessor ends `rotated`,
//! the deterministic successor is bound everywhere, and the successor's
//! inbox order is the predecessor's pending message rows, then the
//! bounded handoff, then the judge prompt (the memory index would come
//! first when `memory:` is configured; ADR-038's e2e pins that).

use kian_cli::config::ServeConfig;
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::state::{SessionState, project};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::rearm::successor_session;
use serde_json::json;
use uuid::Uuid;

fn ev(
    session: SessionId,
    seq: u64,
    epoch: u64,
    kind: EventKind,
    bytes: serde_json::Value,
) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(epoch),
        ts: 1_761_900_000_000 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

#[tokio::test]
async fn boot_rotation_retires_the_router_and_bootstraps_the_successor() {
    let dir = std::env::temp_dir().join(format!("kian-rotation-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let sqlite = dir.join("kian.sqlite");
    let pinned = Uuid::new_v4();
    let sid = SessionId(pinned);

    // The predecessor: an idle, resumable stream over the threshold plus
    // one pending message row that must reach the successor.
    {
        let store = EventStore::open(&sqlite).await.expect("store");
        let events = vec![
            ev(sid, 1, 1, EventKind::SessionCreated, json!({})),
            ev(
                sid,
                2,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "the first task"}),
            ),
            ev(
                sid,
                3,
                1,
                EventKind::DrainClaimed,
                json!({"holder": "seed", "epoch": 1}),
            ),
            ev(sid, 4, 1, EventKind::TurnStart, json!({"iteration": 0})),
            ev(
                sid,
                5,
                1,
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": {"text": "did the first task"}}),
            ),
            ev(sid, 6, 1, EventKind::TurnEnd, json!({})),
            ev(
                sid,
                7,
                1,
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
                json!({}),
            ),
        ];
        store
            .append_batch(
                sid,
                &events,
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: 1,
                },
            )
            .await
            .expect("seed stream");
        store
            .admit(InputRow {
                id: Uuid::new_v4(),
                session: sid,
                payload: json!({"role": "user", "content": "the pending follow-up"}),
                admitted_at: 0,
                eligible_at: 0,
                idempotency_key: Some("pending:follow-up".into()),
            })
            .await
            .expect("seed pending row");
        assert_eq!(
            project(&store.rebuild(sid).await.expect("rebuild")).expect("project"),
            SessionState::Idle
        );
    }

    // The provider construction reads the key at boot; no request is made.
    unsafe {
        std::env::set_var("KIAN_E2E_API_KEY", "test-key");
    }
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
plugins: ["openai-compat", "local-tools", "superagent"]
storage:
  sqlite: "{sqlite}"
agent:
  id: e2e-agent
  model_slots:
    main: {{ provider: mock, model: mock-model }}
  tools: ["fs_read"]
agents:
  router:
    id: router
    version: 1
    model_slots:
      router: {{ provider: mock, model: mock-model }}
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker-agent:
    id: worker-agent
    version: 1
    model_slots:
      main: {{ provider: mock, model: mock-model }}
    tools: ["exec"]
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
drain:
  max_iterations: 8
superagent:
  router:
    session_id: "{pinned}"
    agent: router
    slot: router
    rotate_after_events: 2
  worker:
    agent: worker-agent
  channels:
    mock:
      type: mock-im
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        pinned = pinned,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    let config = ServeConfig::load(&path).expect("config loads");
    let _server = Server::from_config(config).await.expect("server boots");

    let store = EventStore::open_read_only(&sqlite).await.expect("reader");
    let events = store.rebuild(sid).await.expect("predecessor stream");
    let end = events.last().expect("an end write");
    let EventKind::SessionEnded {
        reason: EndReason::Rotated,
    } = end.kind
    else {
        panic!("the predecessor must end rotated: {:?}", end.kind);
    };
    let successor = successor_session(sid);
    assert_eq!(
        end.bytes.get("rotated_to").and_then(|v| v.as_str()),
        Some(successor.0.to_string().as_str()),
        "the end payload names the successor"
    );

    // The successor was bound everywhere (the judge prompt went to it)
    // and its inbox order is: migrated row, handoff, judge prompt.
    let rows = store
        .eligible_inputs(successor, u64::MAX / 2)
        .await
        .expect("successor inbox");
    assert_eq!(rows.len(), 3, "rows: {rows:#?}");
    assert_eq!(
        rows[0].payload,
        json!({"role": "user", "content": "the pending follow-up"}),
        "the pending message row migrates first"
    );
    assert!(
        rows[0]
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.starts_with("rotate:")),
        "the migrated row carries its re-admit key: {:?}",
        rows[0].idempotency_key
    );
    assert_eq!(
        rows[1].payload.get("synthetic"),
        Some(&json!(true)),
        "the handoff is a synthetic user turn: {}",
        rows[1].payload
    );
    assert!(
        rows[1]
            .payload
            .get("content")
            .and_then(|v| v.as_str())
            .is_some_and(|text| text.contains("[rotation]")),
        "the handoff names the rotation: {}",
        rows[1].payload
    );
    assert!(
        rows[2]
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.contains("re-arm")),
        "the judge prompt is the successor's re-arm bootstrap: {:?}",
        rows[2].idempotency_key
    );
}
