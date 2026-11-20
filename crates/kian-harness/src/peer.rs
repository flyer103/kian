//! The stdio line peer shared by the adapters: spawn a child, write protocol lines, read
//! protocol lines with a bounded size, and kill the child when the session drops.
//!
//! The peer owns the process. A dropped bridge session kills the child (HARNESS-BRIDGE §4.1);
//! stderr is inherited for operator debugging and never parsed or persisted (§8).

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::event::ContractError;

/// The bounded line size (HARNESS-BRIDGE §8): a larger line is a protocol failure, never an
/// unbounded allocation.
pub const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;

/// One message from the peer.
#[derive(Debug)]
pub enum PeerLine {
    /// One stdout line (newline stripped).
    Line(String),
    /// stdout closed; the child exited with this code (`None` = killed by a signal).
    Exit(Option<i32>),
    /// Pipe I/O failed or a line exceeded [`MAX_LINE_BYTES`].
    Io(String),
}

/// A live child speaking one JSON object per line over stdio.
pub struct LinePeer {
    tx: mpsc::Sender<String>,
    rx: mpsc::Receiver<PeerLine>,
    kill: mpsc::Sender<()>,
}

impl LinePeer {
    /// Spawns `program args…` with extra environment pairs.
    pub async fn spawn(
        program: &str,
        args: &[String],
        env: &[(String, String)],
    ) -> Result<Self, String> {
        let mut command = Command::new(program);
        command
            .args(args)
            .envs(env.iter().map(|(k, v)| (k.clone(), v.clone())))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|e| format!("spawn {program}: {e}"))?;
        let stdin = child.stdin.take().ok_or("child stdin not piped")?;
        let stdout = child.stdout.take().ok_or("child stdout not piped")?;
        let (tx, ctrl_rx) = mpsc::channel(32);
        let (line_tx, line_rx) = mpsc::channel(64);
        let (kill, kill_rx) = mpsc::channel(1);
        tokio::spawn(pump(child, stdin, stdout, ctrl_rx, line_tx, kill_rx));
        Ok(Self {
            tx,
            rx: line_rx,
            kill,
        })
    }

    /// Writes one protocol line.
    pub async fn send(&self, line: String) -> Result<(), ContractError> {
        self.tx
            .send(line)
            .await
            .map_err(|_| ContractError::Io("child stdin closed".to_owned()))
    }

    /// The next peer message.
    pub async fn next(&mut self) -> PeerLine {
        match self.rx.recv().await {
            Some(line) => line,
            None => PeerLine::Exit(None),
        }
    }

    /// The next line, refusing anything but a line, within a deadline.
    pub async fn next_line_within(&mut self, limit: Duration) -> Result<String, ContractError> {
        match tokio::time::timeout(limit, self.next()).await {
            Err(_) => Err(ContractError::Drift(format!(
                "child did not answer within {limit:?}"
            ))),
            Ok(PeerLine::Line(line)) => Ok(line),
            Ok(PeerLine::Exit(code)) => Err(ContractError::Exited(format!("{code:?}"))),
            Ok(PeerLine::Io(e)) => Err(ContractError::Io(e)),
        }
    }
}

impl Drop for LinePeer {
    fn drop(&mut self) {
        // The pump kills the child; `try_send` because Drop cannot await.
        let _ = self.kill.try_send(());
    }
}

async fn pump(
    mut child: Child,
    mut stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    mut ctrl_rx: mpsc::Receiver<String>,
    line_tx: mpsc::Sender<PeerLine>,
    mut kill_rx: mpsc::Receiver<()>,
) {
    let mut reader = BufReader::new(stdout);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        tokio::select! {
            _ = kill_rx.recv() => {
                let _ = child.start_kill();
                break;
            }
            frame = ctrl_rx.recv() => match frame {
                Some(line) => {
                    if stdin.write_all(line.as_bytes()).await.is_err()
                        || stdin.write_all(b"\n").await.is_err()
                        || stdin.flush().await.is_err()
                    {
                        let _ = line_tx.send(PeerLine::Io("child stdin write failed".into())).await;
                        let _ = child.start_kill();
                        break;
                    }
                }
                None => {
                    let _ = child.start_kill();
                    break;
                }
            },
            read = reader.read_until(b'\n', &mut buf) => match read {
                Ok(0) => break,
                Ok(_) => {
                    if buf.len() > MAX_LINE_BYTES {
                        let _ = line_tx
                            .send(PeerLine::Io(format!("line exceeds {MAX_LINE_BYTES} bytes")))
                            .await;
                        let _ = child.start_kill();
                        break;
                    }
                    let text = String::from_utf8_lossy(&buf)
                        .trim_end_matches(['\n', '\r'])
                        .to_owned();
                    if line_tx.send(PeerLine::Line(text)).await.is_err() {
                        let _ = child.start_kill();
                        break;
                    }
                }
                Err(e) => {
                    let _ = line_tx.send(PeerLine::Io(e.to_string())).await;
                    let _ = child.start_kill();
                    break;
                }
            },
        }
    }
    let status = child.wait().await;
    let _ = line_tx
        .send(PeerLine::Exit(status.ok().and_then(|s| s.code())))
        .await;
}
