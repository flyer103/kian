//! KiandProto v0 wire types — the **server half's independent declaration** of
//! the wire contract (DESIGN §4.3).
//!
//! This is a deliberate mirror of the endpoint-side crate's `proto`
//! module, not a shared dependency: the two halves of the protocol are two
//! independent implementations pinned to the same contract by their own
//! golden tests (byte-identical JSON). Any wire-frame change must update both
//! crates' goldens and bump `PROTOCOL_VERSION` in both, in the same commit —
//! zero-churn is the expected outcome while v0 is provisional.
//!
//! Frame directions over one bidirectional stream:
//! - [`ServerFrame`] — server → endpoint (welcome, command, ping, bye);
//! - [`EndpointFrame`] — endpoint → server (hello, heartbeat, result).
//!
//! Every message is serde-serialized JSON, one frame per line.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use kian_core::seams::{ExecCommand, ExecResult};

/// Version of the KiandProto wire protocol implemented by this crate.
/// Must equal `kian_endpoint::proto::PROTOCOL_VERSION` — asserted by the
/// integration pairing tests.
pub const PROTOCOL_VERSION: u32 = 1;

/// The closed register-refusal reason set (v1). Exactly five variants;
/// a worker sending an unknown string gets a decode error, never a
/// silently-accepted frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalReason {
    VersionMismatch,
    BadToken,
    DuplicateStatic,
    IdConflict,
    Disabled,
}

/// The shared worker token as it travels inside a register frame.
///
/// Redacting on purpose (P3): `Debug`/`Display` print `<redacted>` so
/// `{:?}` of a whole frame — the common accidental leak path — can never
/// print the value. Serialization is the bare string; deserialization is
/// from a string.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterToken(String);

impl RegisterToken {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The raw value — compare constant-time; never log or echo it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RegisterToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl std::fmt::Display for RegisterToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Correlation id for one command execution. Scopes exactly one in-flight
/// execution — never persisted, never a session key (brain–hand
/// separation).
pub type CommandId = Uuid;

/// Serializable command failure mirroring the exec seam's `ExecError`
/// (which is not serde-shaped; it stays a core type).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandError {
    pub code: ErrorCode,
    pub message: String,
}

/// Failure categories, pre-aligned with `kian_core::seams::ExecError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Spawn,
    Timeout,
    Io,
}

/// Server → endpoint frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Handshake answer: the server accepts the endpoint at version `v`.
    /// `heartbeat_ms`, when present, directs the endpoint's heartbeat
    /// cadence for this connection (server-directed liveness).
    Welcome {
        #[serde(default)]
        v: u32,
        heartbeat_ms: Option<u64>,
    },
    /// Execute one command; answer with an [`EndpointFrame::CommandResult`]
    /// carrying the same `id`.
    Command { id: CommandId, cmd: ExecCommand },
    /// Liveness probe; the endpoint answers with an immediate heartbeat.
    Ping,
    /// Graceful close.
    Bye,
    /// Typed register refusal (v1): the connection is closed right after
    /// this frame. `detail` is server-generated from fixed templates that
    /// may interpolate only the `endpoint_id` and protocol versions.
    Rejected {
        reason: RefusalReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// Endpoint → server frames.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EndpointFrame {
    /// Handshake opener: the endpoint announces its protocol version and
    /// concurrency cap. `max_concurrent` lets the server cap its own
    /// in-flight pressure without an extra round trip.
    Hello {
        v: u32,
        endpoint_id: String,
        max_concurrent: u32,
    },
    /// Worker-dial registration handshake (v1): the worker announces its
    /// identity, optional description, the shared worker token, its
    /// protocol revision (`version` — deliberately spelled differently
    /// from `hello`'s `v`; both are frozen wire names) and its concurrency
    /// cap. Answered by `welcome` or a typed `rejected`.
    Register {
        endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        token: RegisterToken,
        version: u32,
        max_concurrent: u32,
    },
    /// Liveness signal, emitted on the negotiated interval and on ping.
    /// `inflight` reports this connection's executing commands.
    Heartbeat {
        endpoint_id: String,
        ts_ms: u64,
        inflight: u32,
    },
    /// Result of one [`ServerFrame::Command`]. Exactly one of
    /// `result` / `error` is present.
    #[serde(rename = "result")]
    CommandResult {
        id: CommandId,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<ExecResult>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<CommandError>,
    },
    /// Graceful leave (v1): same bytes as the server's `bye`.
    Bye,
}

impl EndpointFrame {
    /// Build a successful command result frame.
    pub fn command_ok(id: CommandId, result: ExecResult) -> Self {
        Self::CommandResult {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build a failed command result frame.
    pub fn command_failed(id: CommandId, error: CommandError) -> Self {
        Self::CommandResult {
            id,
            result: None,
            error: Some(error),
        }
    }

    /// The correlation id when this is a result frame.
    pub fn command_id(&self) -> Option<CommandId> {
        match self {
            Self::CommandResult { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// Split a result frame into its outcome parts.
    pub fn result_parts(&self) -> Option<(CommandId, Option<ExecResult>, Option<CommandError>)> {
        match self {
            Self::CommandResult { id, result, error } => Some((*id, result.clone(), error.clone())),
            _ => None,
        }
    }
}

/// Serialize a server frame as one JSON line (no trailing newline).
pub fn encode_server_frame(frame: &ServerFrame) -> String {
    serde_json::to_string(frame).expect("server frames always serialize")
}

/// Serialize an endpoint frame as one JSON line (no trailing newline).
pub fn encode_endpoint_frame(frame: &EndpointFrame) -> String {
    serde_json::to_string(frame).expect("endpoint frames always serialize")
}

/// Parse one server frame line.
pub fn decode_server_frame(line: &str) -> Result<ServerFrame, serde_json::Error> {
    serde_json::from_str(line)
}

/// Parse one endpoint frame line.
pub fn decode_endpoint_frame(line: &str) -> Result<EndpointFrame, serde_json::Error> {
    serde_json::from_str(line)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    /// v1 golden: the server-side register-refusal frame, minimal form —
    /// exactly `{"type":"rejected","reason":"bad_token"}` (no detail key).
    #[test]
    fn v1_golden_rejected_minimal() {
        let frame = ServerFrame::Rejected {
            reason: RefusalReason::BadToken,
            detail: None,
        };
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({"type": "rejected", "reason": "bad_token"})
        );
    }

    /// v1 golden: version_mismatch refusal with the fixed detail template
    /// (interpolates only version numbers, never a token).
    #[test]
    fn v1_golden_rejected_version_mismatch_with_detail() {
        let frame = ServerFrame::Rejected {
            reason: RefusalReason::VersionMismatch,
            detail: Some("server speaks KiandProto 1; worker announced 0".into()),
        };
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({
                "type": "rejected",
                "reason": "version_mismatch",
                "detail": "server speaks KiandProto 1; worker announced 0"
            })
        );
    }

    /// v1 golden: a register frame decodes with the token carried as a
    /// bare string and the description key absent when omitted.
    #[test]
    fn v1_golden_register_decodes() {
        let frame: EndpointFrame = serde_json::from_value(json!({
            "type": "register",
            "endpoint_id": "builder-1",
            "token": "t0ken-not-a-secret",
            "version": 1,
            "max_concurrent": 4
        }))
        .unwrap();
        match frame {
            EndpointFrame::Register {
                endpoint_id,
                description,
                token,
                version,
                max_concurrent,
            } => {
                assert_eq!(endpoint_id, "builder-1");
                assert_eq!(description, None);
                assert_eq!(token.as_str(), "t0ken-not-a-secret");
                assert_eq!(version, 1);
                assert_eq!(max_concurrent, 4);
            }
            other => panic!("expected register, got {other:?}"),
        }
    }

    /// v1: the endpoint-side bye frame is the same bytes as the server's.
    #[test]
    fn v1_golden_endpoint_bye() {
        assert_eq!(
            serde_json::to_value(&EndpointFrame::Bye).unwrap(),
            json!({"type": "bye"})
        );
    }

    /// P3: the token newtype redacts in Debug — `{:?}` of a whole frame is
    /// the common accidental leak path and must never print the value.
    #[test]
    fn v1_register_token_debug_is_redacted() {
        let token = RegisterToken::new("t0ken-not-a-secret".into());
        assert!(!format!("{token:?}").contains("t0ken-not-a-secret"));
        assert!(format!("{token:?}").contains("<redacted>"));
        assert!(!format!("{token}").contains("t0ken-not-a-secret"));
        // But the wire form is the bare string.
        assert_eq!(
            serde_json::to_value(&token).unwrap(),
            json!("t0ken-not-a-secret")
        );

        let frame = EndpointFrame::Register {
            endpoint_id: "builder-1".into(),
            description: None,
            token: token.clone(),
            version: 1,
            max_concurrent: 4,
        };
        assert!(!format!("{frame:?}").contains("t0ken-not-a-secret"));
        let back: RegisterToken = serde_json::from_value(json!("t0ken-not-a-secret")).unwrap();
        assert_eq!(back.as_str(), "t0ken-not-a-secret");
    }

    /// The closed refusal reason set: all five variants serialize
    /// snake_case exactly; an unknown string is a decode error (no
    /// `#[serde(other)]` fallback).
    #[test]
    fn v1_refusal_reason_set_is_closed() {
        for (reason, wire) in [
            (RefusalReason::VersionMismatch, "version_mismatch"),
            (RefusalReason::BadToken, "bad_token"),
            (RefusalReason::DuplicateStatic, "duplicate_static"),
            (RefusalReason::IdConflict, "id_conflict"),
            (RefusalReason::Disabled, "disabled"),
        ] {
            assert_eq!(serde_json::to_value(reason).unwrap(), json!(wire));
            let back: RefusalReason = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(back, reason);
        }
        assert!(serde_json::from_value::<RefusalReason>(json!("nope")).is_err());
    }

    fn cmd(argv: &[&str]) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        }
    }

    /// Golden JSON shapes for every frame — byte-identical to the
    /// endpoint-side crate's goldens. This is the server half's half of the
    /// dual golden contract: if these change, kian-endpoint's goldens must
    /// change in the same commit (with a PROTOCOL_VERSION bump both sides).
    #[test]
    fn frames_golden_json_matches_endpoint_side_contract() {
        let id = Uuid::nil();

        let welcome = ServerFrame::Welcome {
            v: 1,
            heartbeat_ms: Some(100),
        };
        assert_eq!(
            serde_json::to_value(&welcome).unwrap(),
            json!({"type": "welcome", "v": 1, "heartbeat_ms": 100})
        );

        // v1 golden: heartbeat_ms stays `null` when absent (frozen v0 byte
        // shape — no skip_serializing_if).
        let welcome_none = ServerFrame::Welcome {
            v: 1,
            heartbeat_ms: None,
        };
        assert_eq!(
            serde_json::to_value(&welcome_none).unwrap(),
            json!({"type": "welcome", "v": 1, "heartbeat_ms": null})
        );

        let command = ServerFrame::Command {
            id,
            cmd: cmd(&["ls"]),
        };
        assert_eq!(
            serde_json::to_value(&command).unwrap(),
            json!({
                "type": "command",
                "id": "00000000-0000-0000-0000-000000000000",
                "cmd": {"argv": ["ls"], "env": {}, "cwd": null, "timeout_ms": null}
            })
        );

        assert_eq!(
            serde_json::to_value(&ServerFrame::Ping).unwrap(),
            json!({"type": "ping"})
        );
        assert_eq!(
            serde_json::to_value(&ServerFrame::Bye).unwrap(),
            json!({"type": "bye"})
        );

        let hello = EndpointFrame::Hello {
            v: 1,
            endpoint_id: "dev".into(),
            max_concurrent: 4,
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            json!({"type": "hello", "v": 1, "endpoint_id": "dev", "max_concurrent": 4})
        );

        let heartbeat = EndpointFrame::Heartbeat {
            endpoint_id: "dev".into(),
            ts_ms: 7,
            inflight: 2,
        };
        assert_eq!(
            serde_json::to_value(&heartbeat).unwrap(),
            json!({"type": "heartbeat", "endpoint_id": "dev", "ts_ms": 7, "inflight": 2})
        );

        let ok = EndpointFrame::command_ok(
            id,
            ExecResult {
                exit_code: Some(0),
                stdout: "out".into(),
                stderr: String::new(),
                timed_out: false,
                redactions: Vec::new(),
            },
        );
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            json!({
                "type": "result",
                "id": "00000000-0000-0000-0000-000000000000",
                "result": {"exit_code": 0, "stdout": "out", "stderr": "", "timed_out": false}
            })
        );

        let failed = EndpointFrame::command_failed(
            id,
            CommandError {
                code: ErrorCode::Timeout,
                message: "execution timed out".into(),
            },
        );
        assert_eq!(
            serde_json::to_value(&failed).unwrap(),
            json!({
                "type": "result",
                "id": "00000000-0000-0000-0000-000000000000",
                "error": {"code": "timeout", "message": "execution timed out"}
            })
        );
    }

    /// Round-trips through the line codec in both directions.
    #[test]
    fn encode_decode_line_round_trip() {
        let server_frames = vec![
            ServerFrame::Welcome {
                v: 1,
                heartbeat_ms: Some(250),
            },
            ServerFrame::Command {
                id: Uuid::new_v4(),
                cmd: cmd(&["echo", "hi"]),
            },
            ServerFrame::Ping,
            ServerFrame::Bye,
            ServerFrame::Rejected {
                reason: RefusalReason::BadToken,
                detail: None,
            },
            ServerFrame::Rejected {
                reason: RefusalReason::VersionMismatch,
                detail: Some("server speaks KiandProto 1; worker announced 0".into()),
            },
        ];
        for frame in &server_frames {
            assert_eq!(
                &decode_server_frame(&encode_server_frame(frame)).unwrap(),
                frame
            );
        }

        let endpoint_frames = vec![
            EndpointFrame::Hello {
                v: 1,
                endpoint_id: "e".into(),
                max_concurrent: 1,
            },
            EndpointFrame::Register {
                endpoint_id: "builder-1".into(),
                description: Some("linux builder".into()),
                token: RegisterToken::new("t0ken-not-a-secret".into()),
                version: PROTOCOL_VERSION,
                max_concurrent: 4,
            },
            EndpointFrame::Register {
                endpoint_id: "builder-1".into(),
                description: None,
                token: RegisterToken::new("t0ken-not-a-secret".into()),
                version: PROTOCOL_VERSION,
                max_concurrent: 4,
            },
            EndpointFrame::Heartbeat {
                endpoint_id: "e".into(),
                ts_ms: 42,
                inflight: 1,
            },
            EndpointFrame::Bye,
            EndpointFrame::command_ok(
                Uuid::new_v4(),
                ExecResult {
                    exit_code: None,
                    stdout: String::new(),
                    stderr: String::new(),
                    timed_out: true,
                    redactions: Vec::new(),
                },
            ),
            EndpointFrame::command_failed(
                Uuid::new_v4(),
                CommandError {
                    code: ErrorCode::Spawn,
                    message: "no such binary".into(),
                },
            ),
        ];
        for frame in &endpoint_frames {
            assert_eq!(
                &decode_endpoint_frame(&encode_endpoint_frame(frame)).unwrap(),
                frame
            );
        }
    }

    /// A welcome without a version field decodes with `v == 0` (the
    /// `#[serde(default)]` keeps older frames parseable — frozen v0 byte
    /// behavior). Since v1 the receiver must treat that as a version
    /// mismatch; the default is *not* the current version.
    #[test]
    fn welcome_version_defaults_and_result_parts_split() {
        let welcome: ServerFrame = serde_json::from_value(json!({"type": "welcome"})).unwrap();
        assert_eq!(
            welcome,
            ServerFrame::Welcome {
                v: 0,
                heartbeat_ms: None
            }
        );
        assert_ne!(
            welcome,
            ServerFrame::Welcome {
                v: PROTOCOL_VERSION,
                heartbeat_ms: None
            }
        );

        let id = Uuid::new_v4();
        let frame = EndpointFrame::command_ok(
            id,
            ExecResult {
                exit_code: Some(3),
                stdout: String::new(),
                stderr: "boom".into(),
                timed_out: false,
                redactions: Vec::new(),
            },
        );
        let (rid, result, error) = frame.result_parts().unwrap();
        assert_eq!(rid, id);
        assert_eq!(result.unwrap().exit_code, Some(3));
        assert!(error.is_none());
        assert!(
            EndpointFrame::Heartbeat {
                endpoint_id: "e".into(),
                ts_ms: 1,
                inflight: 0
            }
            .result_parts()
            .is_none()
        );
    }
}
