//! TCP transport for KiandProto v0: newline-delimited JSON frames over a
//! TCP stream (card C12, DESIGN §4.3).
//!
//! `attach` wires a `TcpStream` to the framed channel ends that
//! [`Endpoint::run_connection`] consumes; `connect` builds the server-side
//! peer over TCP. The concrete framing (JSON lines) is provisional and
//! expected to churn when Phase 2 lands the real `kian-exec` pair.

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;

use crate::endpoint::ServerPeer;
use crate::proto::{
    EndpointFrame, ServerFrame, decode_endpoint_frame, decode_server_frame, encode_endpoint_frame,
    encode_server_frame,
};

/// Wire a accepted TCP stream to the endpoint-side channel ends: a read
/// pump (lines → inbound frames, malformed lines logged and skipped) and
/// a write pump (outbound frames → lines). Both pumps exit when either
/// side closes, dropping the channels so `run_connection` observes the
/// disconnect.
pub fn attach(
    stream: TcpStream,
    inbound_tx: mpsc::Sender<ServerFrame>,
    mut outbound_rx: mpsc::Receiver<EndpointFrame>,
) {
    let (reader, mut writer) = stream.into_split();
    tokio::spawn(read_pump(reader, inbound_tx));
    tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let line = encode_endpoint_frame(&frame);
            if writer.write_all(line.as_bytes()).await.is_err()
                || writer.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
    });
}

async fn read_pump(reader: OwnedReadHalf, inbound_tx: mpsc::Sender<ServerFrame>) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        match decode_server_frame(&line) {
            Ok(frame) => {
                if inbound_tx.send(frame).await.is_err() {
                    break; // endpoint side went away
                }
            }
            Err(error) => {
                tracing::warn!(%error, "skipping malformed KiandProto line");
            }
        }
    }
}

/// Connect to an endpoint over TCP and return the server-side peer.
/// This is the reference client the Phase-2 `kian-exec` crate will grow
/// out of.
pub async fn connect(addr: SocketAddr) -> std::io::Result<ServerPeer> {
    let stream = TcpStream::connect(addr).await?;
    let (to_endpoint, mut from_peer) = mpsc::channel::<ServerFrame>(64);
    let (to_peer, from_endpoint) = mpsc::channel::<EndpointFrame>(64);

    let (reader, mut writer) = stream.into_split();
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match decode_endpoint_frame(&line) {
                Ok(frame) => {
                    if to_peer.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(error) => tracing::warn!(%error, "skipping malformed KiandProto line"),
            }
        }
    });
    tokio::spawn(async move {
        while let Some(frame) = from_peer.recv().await {
            let line = encode_server_frame(&frame);
            if writer.write_all(line.as_bytes()).await.is_err()
                || writer.write_all(b"\n").await.is_err()
            {
                break;
            }
        }
    });

    Ok(ServerPeer::new(to_endpoint, from_endpoint))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kian_core::seams::ExecCommand;

    use super::connect;
    use crate::endpoint::Endpoint;
    use crate::executor::LocalExecutor;

    fn cmd(argv: &[&str]) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms: None,
        }
    }

    /// The stream transport works end-to-end and survives a reconnect:
    /// connect → handshake → execute → disconnect → reconnect → execute.
    #[tokio::test]
    async fn tcp_transport_pair_execute_and_reconnect() {
        let endpoint = Arc::new(
            Endpoint::new("tcp-box", Arc::new(LocalExecutor))
                .with_heartbeat(Duration::from_secs(30)),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(endpoint.serve(listener));

        let mut peer = connect(addr).await.unwrap();
        let hello = peer.handshake(None).await.unwrap();
        assert_eq!(hello.endpoint_id, "tcp-box");
        let result = peer
            .execute(cmd(&["echo", "over-tcp"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.stdout.trim(), "over-tcp");

        drop(peer); // hard disconnect

        // Reconnect: the accept loop simply serves a fresh connection.
        let mut peer2 = connect(addr).await.unwrap();
        peer2.handshake(None).await.unwrap();
        let result2 = peer2
            .execute(cmd(&["echo", "again"]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result2.stdout.trim(), "again");
    }
}
