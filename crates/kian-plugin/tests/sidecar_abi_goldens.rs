//! ABI golden tests (PLUGIN-SPEC §7.2) — every frame shape, byte for
//! byte, including the epoch-absent-on-admission rule.
//!
//! The goldens pin **compact JSON in the spec's field order**: a
//! field-order or presence drift is a silent ABI break for every
//! deployed sidecar (K2's discipline applied to the wire tier).

use kian_plugin::hooks::ids::HookId;
use kian_plugin::hooks::registry::ReplyErrCode;
use kian_plugin::hooks::sidecar::frame::{
    Hello, HookFrame, ReplyFrame, decode_ready, decode_reply, encode_bye, encode_err,
};
use kian_plugin::hooks::views::FrameInfo;

const SESSION: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

fn sid() -> kian_core::model::SessionId {
    serde_json::from_str(&format!("\"{SESSION}\"")).unwrap()
}

fn hello() -> Hello {
    Hello {
        manifest: "/etc/kian/plugins/approval-gate.toml".to_string(),
        name: "approval-gate".to_string(),
        version: "0.1.0".to_string(),
        abi: 1,
        hooks: vec!["tool/pre".to_string(), "tool/post".to_string()],
        capabilities: vec!["observe:tool".to_string(), "veto:tool".to_string()],
        deadline_default_ms: 500,
        max_frame_bytes: 262_144,
    }
}

#[test]
fn hello_golden() {
    assert_eq!(
        hello().encode(),
        concat!(
            r#"{"v":1,"manifest":"/etc/kian/plugins/approval-gate.toml","#,
            r#""plugin":{"name":"approval-gate","version":"0.1.0"},"abi":1,"#,
            r#""hooks":["tool/pre","tool/post"],"capabilities":["observe:tool","veto:tool"],"#,
            r#""deadline_default_ms":500,"max_frame_bytes":262144}"#
        )
    );
}

#[test]
fn ready_golden() {
    // §7.2's ready: the identity restatement without hello's two
    // bound fields.
    let line = concat!(
        r#"{"v":1,"manifest":"/etc/kian/plugins/approval-gate.toml","#,
        r#""plugin":{"name":"approval-gate","version":"0.1.0"},"abi":1,"#,
        r#""hooks":["tool/post","tool/pre"],"capabilities":["veto:tool","observe:tool"]}"#
    );
    let ready = decode_ready(line).expect("ready decodes");
    // Set-equal, order-insensitive (§7.3 step 2).
    assert_eq!(ready.name, "approval-gate");
    assert_eq!(ready.version, "0.1.0");
    assert_eq!(ready.abi, 1);
    assert_eq!(ready.hooks.len(), 2);
    assert_eq!(ready.capabilities.len(), 2);
}

fn tool_pre_frame(epoch: Option<u64>) -> HookFrame {
    let frame = FrameInfo {
        session: sid(),
        worker: "node-zulu".to_string(),
        seq: 41,
        epoch,
    };
    HookFrame::new(
        HookId::ToolPre,
        &frame,
        "sc-1".to_string(),
        500,
        serde_json::json!({"call_id":"call_abc","name":"exec"}),
    )
    .expect("drain-phase frame carries an epoch")
}

#[test]
fn hook_frame_golden_drain_phase_carries_epoch() {
    assert_eq!(
        tool_pre_frame(Some(7)).encode(),
        concat!(
            r#"{"id":"sc-1","hook":"tool/pre","session":"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b","#,
            r#""worker":"node-zulu","seq":41,"epoch":7,"deadline_ms":500,"mode":"bail","#,
            r#""payload":{"call_id":"call_abc","name":"exec"}}"#
        )
    );
}

#[test]
fn hook_frame_golden_admission_omits_the_epoch_key_entirely() {
    // §7.2: `admit/pre` and `admit/post` are consulted before a drain
    // exists, so their frames omit `epoch` — no null, no sentinel, no 0.
    let frame = FrameInfo {
        session: sid(),
        worker: String::new(),
        seq: 0,
        epoch: None,
    };
    let hook = HookFrame::new(
        HookId::AdmitPre,
        &frame,
        "sc-2".to_string(),
        500,
        serde_json::json!({"origin":"channel"}),
    )
    .expect("admission frame omits epoch");
    let encoded = hook.encode();
    assert_eq!(
        encoded,
        concat!(
            r#"{"id":"sc-2","hook":"admit/pre","session":"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b","#,
            r#""worker":"","seq":0,"deadline_ms":500,"mode":"flow","#,
            r#""payload":{"origin":"channel"}}"#
        )
    );
    assert!(!encoded.contains("epoch"), "the key is absent, not null");

    // The negative rules are enforced at construction, not by hoping:
    // a drain-phase hook without an epoch is refused…
    assert!(
        HookFrame::new(
            HookId::ToolPre,
            &FrameInfo {
                session: sid(),
                worker: String::new(),
                seq: 0,
                epoch: None,
            },
            "sc-3".to_string(),
            500,
            serde_json::json!({}),
        )
        .is_err()
    );
    // …and an admission hook with one is refused too.
    assert!(
        HookFrame::new(
            HookId::AdmitPre,
            &FrameInfo {
                session: sid(),
                worker: String::new(),
                seq: 0,
                epoch: Some(1),
            },
            "sc-4".to_string(),
            500,
            serde_json::json!({}),
        )
        .is_err()
    );
}

#[test]
fn reply_goldens_ack_flow_bail_err() {
    // ack, bare and with a note.
    assert_eq!(
        ReplyFrame::Ack { note: None }.encode("sc-1"),
        r#"{"id":"sc-1","ack":true}"#
    );
    assert_eq!(
        ReplyFrame::Ack {
            note: Some("seen".to_string())
        }
        .encode("sc-1"),
        r#"{"id":"sc-1","ack":true,"note":"seen"}"#
    );
    // flow next / stop.
    assert_eq!(
        ReplyFrame::FlowNext.encode("sc-1"),
        r#"{"id":"sc-1","flow":"next"}"#
    );
    assert_eq!(
        ReplyFrame::FlowStop {
            reason: "off-hours".to_string()
        }
        .encode("sc-1"),
        r#"{"id":"sc-1","flow":"stop","reason":"off-hours"}"#
    );
    // bail null / deny.
    assert_eq!(
        ReplyFrame::BailNone.encode("sc-1"),
        r#"{"id":"sc-1","bail":null}"#
    );
    assert_eq!(
        ReplyFrame::BailDeny {
            reason: "blocked".to_string()
        }
        .encode("sc-1"),
        r#"{"id":"sc-1","bail":{"decision":"deny","reason":"blocked"}}"#
    );
    // err (flat {id?, code, message}).
    assert_eq!(
        ReplyFrame::Err {
            code: ReplyErrCode::Deadline,
            message: "slow".to_string(),
        }
        .encode("sc-1"),
        r#"{"id":"sc-1","code":"deadline","message":"slow"}"#
    );
    assert_eq!(
        encode_err(
            Some("sc-1"),
            ReplyErrCode::UnsupportedHook,
            "not subscribed"
        ),
        r#"{"id":"sc-1","code":"unsupported_hook","message":"not subscribed"}"#
    );

    // Round-trip: the spec's spellings decode back to the same frames.
    assert_eq!(
        decode_reply(r#"{"id":"sc-1","ack":true}"#).unwrap().1,
        ReplyFrame::Ack { note: None }
    );
    assert_eq!(
        decode_reply(r#"{"id":"sc-1","flow":"stop","reason":"off-hours"}"#)
            .unwrap()
            .1,
        ReplyFrame::FlowStop {
            reason: "off-hours".to_string()
        }
    );
    assert_eq!(
        decode_reply(r#"{"id":"sc-1","bail":{"decision":"deny","reason":"blocked"}}"#)
            .unwrap()
            .1,
        ReplyFrame::BailDeny {
            reason: "blocked".to_string()
        }
    );
    assert_eq!(
        decode_reply(r#"{"id":"sc-1","code":"deadline","message":"slow"}"#)
            .unwrap()
            .1,
        ReplyFrame::Err {
            code: ReplyErrCode::Deadline,
            message: "slow".to_string(),
        }
    );
}

#[test]
fn bye_golden() {
    assert_eq!(encode_bye(), "{}");
}
