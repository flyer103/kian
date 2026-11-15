//! KiandProto v0 wire types (DESIGN §4.3).
//!
//! Two frame directions over one bidirectional stream:
//! - [`ServerFrame`] — server → endpoint (welcome, command, ping, bye);
//! - [`EndpointFrame`] — endpoint → server (hello, heartbeat, result).
//!
//! Every message is serde-serialized JSON; each frame carries the protocol
//! `v` it was written for (or a handshake that pins it). v0 is provisional:
//! wire-format churn is expected when bundle lands `kian-exec`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use kian_core::fs::{FsCommand, FsResult};
use kian_core::seams::{ExecCommand, ExecResult};

/// Version of the KiandProto wire protocol implemented by this crate.
pub const PROTOCOL_VERSION: u32 = 3;

/// The closed register-refusal reason set (v1) — this crate's own
/// independent declaration of the server half's contract (the mirror
/// discipline; see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalReason {
    VersionMismatch,
    BadToken,
    DuplicateStatic,
    IdConflict,
    Disabled,
}

impl RefusalReason {
    /// The wire name (`version_mismatch`, `bad_token`, ...). Error text
    /// and log lines use this, never a Debug rendering.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::VersionMismatch => "version_mismatch",
            Self::BadToken => "bad_token",
            Self::DuplicateStatic => "duplicate_static",
            Self::IdConflict => "id_conflict",
            Self::Disabled => "disabled",
        }
    }
}

/// The shared worker token as it travels inside a register frame.
///
/// Redacting on purpose: `Debug`/`Display` print `<redacted>` so `{:?}`
/// of a whole frame — the common accidental leak path — can never print
/// the value. Serialization is the bare string.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterToken(String);

impl RegisterToken {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The raw value — the server compares constant-time; never log it.
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
    /// Handshake answer: server accepts the endpoint at version `v`.
    /// `heartbeat_ms`, when present, overrides the endpoint's configured
    /// heartbeat interval (server-directed liveness cadence).
    Welcome {
        #[serde(default)]
        v: u32,
        heartbeat_ms: Option<u64>,
    },
    /// Execute one command; answer with a [`EndpointFrame::CommandResult`]
    /// carrying the same `id`.
    Command { id: CommandId, cmd: ExecCommand },
    /// Execute one filesystem command (v2, ADR-023); answer with an
    /// [`EndpointFrame::FsCommandResult`] carrying the same `id`.
    #[serde(rename = "fs_command")]
    FsCommandFrame { id: CommandId, cmd: FsCommand },
    /// Liveness probe; the endpoint answers with an immediate heartbeat.
    Ping,
    /// Graceful close.
    Bye,
    /// Typed register refusal (v1): the server closes the connection
    /// right after this frame. `detail` interpolates only the
    /// `endpoint_id` and protocol versions — never a token.
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
    /// Handshake opener: endpoint announces its protocol version and
    /// concurrency cap. `max_concurrent` lets the server reason about
    /// in-flight pressure without any extra round trip.
    Hello {
        v: u32,
        endpoint_id: String,
        max_concurrent: u32,
    },
    /// Worker-dial registration handshake (v1): announce identity,
    /// optional description, the shared worker token, the protocol
    /// revision (`version` — the frozen spelling; `hello` says `v`) and
    /// the concurrency cap. Answered by `welcome` or a typed `rejected`.
    Register {
        endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        token: RegisterToken,
        version: u32,
        max_concurrent: u32,
        /// The worker's declared capabilities (v3, ADR-029). Optional;
        /// omitted when empty.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        capabilities: Vec<String>,
    },
    /// Liveness signal, emitted on the negotiated interval and on ping.
    /// `inflight` reports this connection's executing commands.
    Heartbeat {
        endpoint_id: String,
        ts_ms: u64,
        inflight: u32,
    },
    /// Result of one [`ServerFrame::Command`]. Exactly one of
    /// `result` / `error` is present (enforced by the constructors).
    #[serde(rename = "result")]
    CommandResult {
        id: CommandId,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<ExecResult>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<CommandError>,
    },
    /// Result of one [`ServerFrame::FsCommandFrame`] (v2, ADR-023).
    /// Exactly one of `result` / `error` is present.
    #[serde(rename = "fs_result")]
    FsCommandResult {
        id: CommandId,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<FsResult>,
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

    /// Build a successful filesystem result frame (v2).
    pub fn fs_ok(id: CommandId, result: FsResult) -> Self {
        Self::FsCommandResult {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build a failed filesystem result frame (v2).
    pub fn fs_failed(id: CommandId, error: CommandError) -> Self {
        Self::FsCommandResult {
            id,
            result: None,
            error: Some(error),
        }
    }

    /// The correlation id when this is a result frame (either family).
    pub fn command_id(&self) -> Option<CommandId> {
        match self {
            Self::CommandResult { id, .. } | Self::FsCommandResult { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// Split a filesystem result frame into its outcome parts (v2).
    pub fn fs_result_parts(&self) -> Option<(CommandId, Option<FsResult>, Option<CommandError>)> {
        match self {
            Self::FsCommandResult { id, result, error } => {
                Some((*id, result.clone(), error.clone()))
            }
            _ => None,
        }
    }

    /// Outcome of a result frame: `Ok` or the command failure.
    ///
    /// # Panics
    /// If called on a non-result frame or a malformed one (both impossible
    /// through the constructors).
    pub fn outcome(&self) -> Result<&ExecResult, &CommandError> {
        match self {
            Self::CommandResult {
                result: Some(result),
                error: None,
                ..
            } => Ok(result),
            Self::CommandResult {
                result: None,
                error: Some(error),
                ..
            } => Err(error),
            _ => panic!("command result frame must carry exactly one of result/error"),
        }
    }
}

impl From<kian_core::seams::ExecError> for CommandError {
    fn from(err: kian_core::seams::ExecError) -> Self {
        match err {
            kian_core::seams::ExecError::Spawn(message) => Self {
                code: ErrorCode::Spawn,
                message,
            },
            kian_core::seams::ExecError::Timeout => Self {
                code: ErrorCode::Timeout,
                message: "execution timed out".into(),
            },
            kian_core::seams::ExecError::Io(message) => Self {
                code: ErrorCode::Io,
                message,
            },
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

    /// v1 golden: the full register frame — description present, token as
    /// a bare string, revision spelled `version` (locked asymmetry).
    #[test]
    fn v1_golden_register_full() {
        let frame = EndpointFrame::Register {
            endpoint_id: "builder-1".into(),
            description: Some("linux builder".into()),
            token: RegisterToken::new("t0ken-not-a-secret".into()),
            version: 1,
            max_concurrent: 4,
            capabilities: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({
                "type": "register",
                "endpoint_id": "builder-1",
                "description": "linux builder",
                "token": "t0ken-not-a-secret",
                "version": 1,
                "max_concurrent": 4
            })
        );
    }

    /// v1 golden: register without a description omits the key entirely
    /// (there is no `"description":null` form).
    #[test]
    fn v1_golden_register_without_description() {
        let frame = EndpointFrame::Register {
            endpoint_id: "builder-1".into(),
            description: None,
            token: RegisterToken::new("t0ken-not-a-secret".into()),
            version: 1,
            max_concurrent: 4,
            capabilities: Vec::new(),
        };
        let value = serde_json::to_value(&frame).unwrap();
        assert_eq!(
            value,
            json!({
                "type": "register",
                "endpoint_id": "builder-1",
                "token": "t0ken-not-a-secret",
                "version": 1,
                "max_concurrent": 4
            })
        );
        assert!(value.get("description").is_none());
    }

    /// v3 golden: the endpoint half's register frame declares the same
    /// optional capability list (empty omitted, never `null`).
    #[test]
    fn v3_golden_register_with_capabilities() {
        let frame = EndpointFrame::Register {
            endpoint_id: "iphone".into(),
            description: Some("iPhone worker".into()),
            token: RegisterToken::new("t0ken-not-a-secret".into()),
            version: PROTOCOL_VERSION,
            max_concurrent: 1,
            capabilities: vec!["exec".into(), "apple-calendar".into()],
        };
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({
                "type": "register",
                "endpoint_id": "iphone",
                "description": "iPhone worker",
                "token": "t0ken-not-a-secret",
                "version": 3,
                "max_concurrent": 1,
                "capabilities": ["exec", "apple-calendar"]
            })
        );
        let empty = EndpointFrame::Register {
            endpoint_id: "builder-1".into(),
            description: None,
            token: RegisterToken::new("t0ken-not-a-secret".into()),
            version: PROTOCOL_VERSION,
            max_concurrent: 4,
            capabilities: Vec::new(),
        };
        assert!(
            serde_json::to_value(&empty)
                .unwrap()
                .get("capabilities")
                .is_none()
        );
    }

    /// v1 golden: the worker's bye is `{"type":"bye"}`.
    #[test]
    fn v1_golden_bye() {
        assert_eq!(
            serde_json::to_value(&EndpointFrame::Bye).unwrap(),
            json!({"type": "bye"})
        );
    }

    /// v1 decode goldens: welcome (both heartbeat forms) and every
    /// refusal reason parse into typed values — a mismatch is a typed
    /// error path, never a panic.
    #[test]
    fn v1_decode_goldens_welcome_and_every_rejection() {
        let welcome: ServerFrame =
            serde_json::from_value(json!({"type": "welcome", "v": 1, "heartbeat_ms": 10000}))
                .unwrap();
        assert_eq!(
            welcome,
            ServerFrame::Welcome {
                v: 1,
                heartbeat_ms: Some(10_000)
            }
        );

        let welcome_none: ServerFrame =
            serde_json::from_value(json!({"type": "welcome", "v": 1, "heartbeat_ms": null}))
                .unwrap();
        assert_eq!(
            welcome_none,
            ServerFrame::Welcome {
                v: 1,
                heartbeat_ms: None
            }
        );

        for (reason, wire) in [
            (RefusalReason::VersionMismatch, "version_mismatch"),
            (RefusalReason::BadToken, "bad_token"),
            (RefusalReason::DuplicateStatic, "duplicate_static"),
            (RefusalReason::IdConflict, "id_conflict"),
            (RefusalReason::Disabled, "disabled"),
        ] {
            let frame: ServerFrame = serde_json::from_value(json!({
                "type": "rejected",
                "reason": wire
            }))
            .unwrap();
            assert_eq!(
                frame,
                ServerFrame::Rejected {
                    reason,
                    detail: None
                }
            );
            // Round-trips back to the minimal bytes.
            assert_eq!(
                serde_json::to_value(&frame).unwrap(),
                json!({"type": "rejected", "reason": wire})
            );
        }

        let with_detail: ServerFrame = serde_json::from_value(json!({
            "type": "rejected",
            "reason": "version_mismatch",
            "detail": "server speaks KiandProto 1; worker announced 0"
        }))
        .unwrap();
        assert_eq!(
            with_detail,
            ServerFrame::Rejected {
                reason: RefusalReason::VersionMismatch,
                detail: Some("server speaks KiandProto 1; worker announced 0".into()),
            }
        );

        // The reason set is closed: an unknown string is a decode error.
        assert!(serde_json::from_value::<RefusalReason>(json!("nope")).is_err());
    }

    /// P3: the token newtype redacts in Debug/Display; the wire form is
    /// the bare string. `{:?}` of a whole register frame must not leak.
    #[test]
    fn v1_register_token_debug_is_redacted() {
        let token = RegisterToken::new("t0ken-not-a-secret".into());
        assert!(!format!("{token:?}").contains("t0ken-not-a-secret"));
        assert!(format!("{token:?}").contains("<redacted>"));
        assert!(!format!("{token}").contains("t0ken-not-a-secret"));
        assert_eq!(
            serde_json::to_value(&token).unwrap(),
            json!("t0ken-not-a-secret")
        );
        let frame = EndpointFrame::Register {
            endpoint_id: "builder-1".into(),
            description: None,
            token,
            version: 1,
            max_concurrent: 4,
            capabilities: Vec::new(),
        };
        assert!(!format!("{frame:?}").contains("t0ken-not-a-secret"));
    }

    fn cmd(argv: &[&str]) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        }
    }

    fn rt<T>(val: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let ser = serde_json::to_value(val).unwrap();
        let back: T = serde_json::from_value(ser).unwrap();
        assert_eq!(&back, val);
    }

    /// Golden JSON shapes for every frame variant — the wire contract of
    /// KiandProto v0. Churn is expected for bundle; until then these are
    /// frozen so the in-process mock and this crate cannot drift apart
    /// silently.
    #[test]
    fn frames_golden_json_and_round_trip() {
        let id = Uuid::nil();

        let welcome = ServerFrame::Welcome {
            v: 1,
            heartbeat_ms: Some(100),
        };
        assert_eq!(
            serde_json::to_value(&welcome).unwrap(),
            json!({"type": "welcome", "v": 1, "heartbeat_ms": 100})
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

        // Round-trips through the serde value tree.
        rt(&welcome);
        rt(&command);
        rt(&ServerFrame::Ping);
        rt(&ServerFrame::Bye);
        rt(&hello);
        rt(&heartbeat);
        rt(&ok);
        rt(&failed);

        // Exactly one of result/error on the wire.
        assert!(ok.outcome().is_ok());
        assert!(failed.outcome().is_err());
        assert_eq!(ok.command_id(), Some(id));
    }

    /// A welcome without a version field decodes with `v == 0`
    /// (`#[serde(default)]`, frozen v0 byte behavior). Since v1 the
    /// endpoint must treat that as a version mismatch — the default is
    /// *not* the current version.
    #[test]
    fn welcome_version_defaults_when_absent() {
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
    }

    /// The line codec round-trips both directions.
    #[test]
    fn encode_decode_line_round_trip() {
        let frames = vec![
            ServerFrame::Welcome {
                v: 1,
                heartbeat_ms: Some(250),
            },
            ServerFrame::Rejected {
                reason: RefusalReason::BadToken,
                detail: None,
            },
            ServerFrame::Rejected {
                reason: RefusalReason::VersionMismatch,
                detail: Some("server speaks KiandProto 1; worker announced 0".into()),
            },
            ServerFrame::Command {
                id: Uuid::new_v4(),
                cmd: cmd(&["echo", "hi"]),
            },
            ServerFrame::Ping,
            ServerFrame::Bye,
        ];
        for frame in &frames {
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
                description: None,
                token: RegisterToken::new("t0ken-not-a-secret".into()),
                version: PROTOCOL_VERSION,
                max_concurrent: 4,
                capabilities: Vec::new(),
            },
            EndpointFrame::Bye,
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
                kian_core::seams::ExecError::Spawn("no such binary".into()).into(),
            ),
        ];
        for frame in &endpoint_frames {
            assert_eq!(
                &decode_endpoint_frame(&encode_endpoint_frame(frame)).unwrap(),
                frame
            );
        }
    }
}
