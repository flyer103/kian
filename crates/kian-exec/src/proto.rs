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
pub const PROTOCOL_VERSION: u32 = 0;

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
            v: 0,
            heartbeat_ms: Some(100),
        };
        assert_eq!(
            serde_json::to_value(&welcome).unwrap(),
            json!({"type": "welcome", "v": 0, "heartbeat_ms": 100})
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
            v: 0,
            endpoint_id: "dev".into(),
            max_concurrent: 4,
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            json!({"type": "hello", "v": 0, "endpoint_id": "dev", "max_concurrent": 4})
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
                v: 0,
                heartbeat_ms: Some(250),
            },
            ServerFrame::Command {
                id: Uuid::new_v4(),
                cmd: cmd(&["echo", "hi"]),
            },
            ServerFrame::Ping,
            ServerFrame::Bye,
        ];
        for frame in &server_frames {
            assert_eq!(
                &decode_server_frame(&encode_server_frame(frame)).unwrap(),
                frame
            );
        }

        let endpoint_frames = vec![
            EndpointFrame::Hello {
                v: 0,
                endpoint_id: "e".into(),
                max_concurrent: 1,
            },
            EndpointFrame::Heartbeat {
                endpoint_id: "e".into(),
                ts_ms: 42,
                inflight: 1,
            },
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

    /// A welcome without a version field decodes as v0 (`#[serde(default)]`
    /// keeps older frames parseable), and result parts split cleanly.
    #[test]
    fn welcome_version_defaults_and_result_parts_split() {
        let welcome: ServerFrame = serde_json::from_value(json!({"type": "welcome"})).unwrap();
        assert_eq!(
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
