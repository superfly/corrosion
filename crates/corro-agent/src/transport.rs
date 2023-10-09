use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use metrics::{gauge, histogram, increment_counter};
use quinn::{
    ApplicationClose, Connection, ConnectionError, Endpoint, RecvStream, SendDatagramError,
    SendStream, WriteError,
};
use quinn_proto::ConnectionStats;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct Transport(Arc<TransportInner>);

#[derive(Debug)]
struct TransportInner {
    endpoint: Endpoint,
    conns: RwLock<HashMap<SocketAddr, Connection>>,
    rtt_tx: mpsc::Sender<(SocketAddr, Duration)>,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Datagram(#[from] SendDatagramError),
    #[error(transparent)]
    SendStreamWrite(#[from] WriteError),
}

impl Transport {
    pub fn new(endpoint: Endpoint, rtt_tx: mpsc::Sender<(SocketAddr, Duration)>) -> Self {
        Self(Arc::new(TransportInner {
            endpoint,
            conns: Default::default(),
            rtt_tx,
        }))
    }

    pub async fn send_datagram(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
        let conn = self.connect(addr).await?;
        debug!("connected to {addr}");
        match conn.send_datagram(data.clone()) {
            Ok(send) => {
                debug!("sent datagram to {addr}");
                return Ok(send);
            }
            Err(SendDatagramError::ConnectionLost(e)) => {
                debug!("retryable error attempting to send datagram: {e}");
            }
            Err(e) => {
                increment_counter!("corro.transport.send_datagram.errors", "addr" => addr.to_string(), "error" => e.to_string());
                return Err(e.into());
            }
        }

        let conn = self.connect(addr).await?;
        debug!("re-connected to {addr}");
        Ok(conn.send_datagram(data)?)
    }

    pub async fn send_uni(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
        let conn = self.connect(addr).await?;

        let mut stream = conn.open_uni().await?;

        stream.write_all(&data).await?;

        stream.finish().await?;

        Ok(())
    }

    pub async fn open_bi(
        &self,
        addr: SocketAddr,
    ) -> Result<(SendStream, RecvStream), TransportError> {
        let conn = self.connect(addr).await?;
        match conn.open_bi().await {
            Ok(send_recv) => return Ok(send_recv),
            Err(e @ ConnectionError::VersionMismatch) => {
                return Err(e.into());
            }
            Err(e) => {
                debug!("retryable error attempting to open bidirectional stream: {e}");
            }
        }

        // retry, it should reconnect!
        let conn = self.connect(addr).await?;
        Ok(conn.open_bi().await?)
    }

    async fn connect(&self, addr: SocketAddr) -> Result<Connection, TransportError> {
        let server_name = addr.ip().to_string();

        {
            let r = self.0.conns.read().await;
            if let Some(conn) = r.get(&addr).cloned() {
                if test_conn(&conn) {
                    if let Err(e) = self.0.rtt_tx.try_send((addr, conn.rtt())) {
                        debug!("could not send RTT for connection through sender: {e}");
                    }
                    return Ok(conn);
                }
            }
        }

        let conn = {
            let mut w = self.0.conns.write().await;
            if let Some(conn) = w.get(&addr).cloned() {
                if test_conn(&conn) {
                    return Ok(conn);
                }
            }

            let start = Instant::now();
            let conn = match self.0.endpoint.connect(addr, server_name.as_str())?.await {
                Ok(conn) => {
                    histogram!(
                        "corro.transport.connect.time.seconds",
                        start.elapsed().as_secs_f64()
                    );
                    conn
                }
                Err(e) => {
                    increment_counter!("corro.transport.connect.errors", "addr" => server_name, "error" => e.to_string());
                    return Err(e.into());
                }
            };

            if let Err(e) = self.0.rtt_tx.try_send((addr, conn.rtt())) {
                debug!("could not send RTT for connection through sender: {e}");
            }
            w.insert(addr, conn.clone());
            conn
        };

        Ok(conn)
    }

    pub fn emit_metrics(&self) {
        let read = self.0.conns.blocking_read();

        gauge!("corro.transport.connections", read.len() as f64);

        // make aggregate stats for all connections... so as to not overload a metrics server
        let stats = read
            .iter()
            .fold(ConnectionStats::default(), |mut acc, (addr, conn)| {
                let stats = conn.stats();

                gauge!("corro.transport.path.cwnd", stats.path.cwnd as f64, "addr" => addr.to_string());

                acc.path.congestion_events += stats.path.congestion_events;
                acc.path.lost_packets += stats.path.lost_packets;
                acc.path.lost_bytes += stats.path.lost_bytes;
                acc.path.sent_packets += stats.path.sent_packets;
                acc.path.sent_plpmtud_probes += stats.path.sent_plpmtud_probes;
                acc.path.lost_plpmtud_probes += stats.path.lost_plpmtud_probes;
                acc.path.black_holes_detected += stats.path.black_holes_detected;

                acc.frame_rx.acks += stats.frame_rx.acks;
                acc.frame_rx.crypto += stats.frame_rx.crypto;
                acc.frame_rx.connection_close += stats.frame_rx.connection_close;
                acc.frame_rx.data_blocked += stats.frame_rx.data_blocked;
                acc.frame_rx.datagram += stats.frame_rx.datagram;
                acc.frame_rx.handshake_done += stats.frame_rx.handshake_done;
                acc.frame_rx.max_data += stats.frame_rx.max_data;
                acc.frame_rx.max_stream_data += stats.frame_rx.max_stream_data;
                acc.frame_rx.max_streams_bidi += stats.frame_rx.max_streams_bidi;
                acc.frame_rx.max_streams_uni += stats.frame_rx.max_streams_uni;
                acc.frame_rx.new_connection_id += stats.frame_rx.new_connection_id;
                acc.frame_rx.new_token += stats.frame_rx.new_token;
                acc.frame_rx.path_challenge += stats.frame_rx.path_challenge;
                acc.frame_rx.path_response += stats.frame_rx.path_response;
                acc.frame_rx.ping += stats.frame_rx.ping;
                acc.frame_rx.reset_stream += stats.frame_rx.reset_stream;
                acc.frame_rx.retire_connection_id += stats.frame_rx.retire_connection_id;
                acc.frame_rx.stream_data_blocked += stats.frame_rx.stream_data_blocked;
                acc.frame_rx.streams_blocked_bidi += stats.frame_rx.streams_blocked_bidi;
                acc.frame_rx.streams_blocked_uni += stats.frame_rx.streams_blocked_uni;
                acc.frame_rx.stop_sending += stats.frame_rx.stop_sending;
                acc.frame_rx.stream += stats.frame_rx.stream;

                acc.frame_tx.acks += stats.frame_tx.acks;
                acc.frame_tx.crypto += stats.frame_tx.crypto;
                acc.frame_tx.connection_close += stats.frame_tx.connection_close;
                acc.frame_tx.data_blocked += stats.frame_tx.data_blocked;
                acc.frame_tx.datagram += stats.frame_tx.datagram;
                acc.frame_tx.handshake_done += stats.frame_tx.handshake_done;
                acc.frame_tx.max_data += stats.frame_tx.max_data;
                acc.frame_tx.max_stream_data += stats.frame_tx.max_stream_data;
                acc.frame_tx.max_streams_bidi += stats.frame_tx.max_streams_bidi;
                acc.frame_tx.max_streams_uni += stats.frame_tx.max_streams_uni;
                acc.frame_tx.new_connection_id += stats.frame_tx.new_connection_id;
                acc.frame_tx.new_token += stats.frame_tx.new_token;
                acc.frame_tx.path_challenge += stats.frame_tx.path_challenge;
                acc.frame_tx.path_response += stats.frame_tx.path_response;
                acc.frame_tx.ping += stats.frame_tx.ping;
                acc.frame_tx.reset_stream += stats.frame_tx.reset_stream;
                acc.frame_tx.retire_connection_id += stats.frame_tx.retire_connection_id;
                acc.frame_tx.stream_data_blocked += stats.frame_tx.stream_data_blocked;
                acc.frame_tx.streams_blocked_bidi += stats.frame_tx.streams_blocked_bidi;
                acc.frame_tx.streams_blocked_uni += stats.frame_tx.streams_blocked_uni;
                acc.frame_tx.stop_sending += stats.frame_tx.stop_sending;
                acc.frame_tx.stream += stats.frame_tx.stream;

                acc.udp_rx.bytes += stats.udp_rx.bytes;
                acc.udp_rx.datagrams += stats.udp_rx.datagrams;
                acc.udp_rx.transmits += stats.udp_rx.transmits;

                acc.udp_tx.bytes += stats.udp_tx.bytes;
                acc.udp_tx.datagrams += stats.udp_tx.datagrams;
                acc.udp_tx.transmits += stats.udp_tx.transmits;

                acc
            });

        gauge!(
            "corro.transport.path.congestion_events",
            stats.path.congestion_events as f64
        );
        gauge!(
            "corro.transport.path.lost_packets",
            stats.path.lost_packets as f64
        );
        gauge!(
            "corro.transport.path.lost_bytes",
            stats.path.lost_bytes as f64
        );
        gauge!(
            "corro.transport.path.sent_packets",
            stats.path.sent_packets as f64
        );
        gauge!(
            "corro.transport.path.sent_plpmtud_probes",
            stats.path.sent_plpmtud_probes as f64
        );
        gauge!(
            "corro.transport.path.lost_plpmtud_probes",
            stats.path.lost_plpmtud_probes as f64
        );
        gauge!(
            "corro.transport.path.black_holes_detected",
            stats.path.black_holes_detected as f64
        );

        gauge!("corro.transport.frame_rx", stats.frame_rx.acks as f64, "type" => "acks");
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.crypto as f64,
            "type" => "crypto"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.connection_close as f64,
            "type" => "connection_close"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.data_blocked as f64,
            "type" => "data_blocked"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.datagram as f64,
            "type" => "datagram"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.handshake_done as f64,
            "type" => "handshake_done"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.max_data as f64,
            "type" => "max_data"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.max_stream_data as f64,
            "type" => "max_stream_data"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.max_streams_bidi as f64,
            "type" => "max_streams_bidi"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.max_streams_uni as f64,
            "type" => "max_streams_uni"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.new_connection_id as f64,
            "type" => "new_connection_id"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.new_token as f64,
            "type" => "new_token"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.path_challenge as f64,
            "type" => "path_challenge"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.path_response as f64,
            "type" => "path_response"
        );
        gauge!("corro.transport.frame_rx", stats.frame_rx.ping as f64, "type" => "ping");
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.reset_stream as f64,
            "type" => "reset_stream"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.retire_connection_id as f64,
            "type" => "retire_connection_id"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.stream_data_blocked as f64,
            "type" => "stream_data_blocked"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.streams_blocked_bidi as f64,
            "type" => "streams_blocked_bidi"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.streams_blocked_uni as f64,
            "type" => "streams_blocked_uni"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.stop_sending as f64,
            "type" => "stop_sending"
        );
        gauge!(
            "corro.transport.frame_rx",
            stats.frame_rx.stream as f64,
            "type" => "stream"
        );

        gauge!("corro.transport.frame_tx", stats.frame_tx.acks as f64, "type" => "acks");
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.crypto as f64,
            "type" => "crypto"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.connection_close as f64,
            "type" => "connection_close"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.data_blocked as f64,
            "type" => "data_blocked"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.datagram as f64,
            "type" => "datagram"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.handshake_done as f64,
            "type" => "handshake_done"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.max_data as f64,
            "type" => "max_data"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.max_stream_data as f64,
            "type" => "max_stream_data"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.max_streams_bidi as f64,
            "type" => "max_streams_bidi"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.max_streams_uni as f64,
            "type" => "max_streams_uni"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.new_connection_id as f64,
            "type" => "new_connection_id"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.new_token as f64,
            "type" => "new_token"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.path_challenge as f64,
            "type" => "path_challenge"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.path_response as f64,
            "type" => "path_response"
        );
        gauge!("corro.transport.frame_tx", stats.frame_tx.ping as f64, "type" => "ping");
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.reset_stream as f64,
            "type" => "reset_stream"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.retire_connection_id as f64,
            "type" => "retire_connection_id"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.stream_data_blocked as f64,
            "type" => "stream_data_blocked"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.streams_blocked_bidi as f64,
            "type" => "streams_blocked_bidi"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.streams_blocked_uni as f64,
            "type" => "streams_blocked_uni"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.stop_sending as f64,
            "type" => "stop_sending"
        );
        gauge!(
            "corro.transport.frame_tx",
            stats.frame_tx.stream as f64,
            "type" => "stream"
        );

        gauge!("corro.transport.udp_rx.bytes", stats.udp_rx.bytes as f64);
        gauge!(
            "corro.transport.udp_rx.datagrams",
            stats.udp_rx.datagrams as f64
        );
        gauge!(
            "corro.transport.udp_rx.transmits",
            stats.udp_rx.transmits as f64
        );

        gauge!("corro.transport.udp_tx.bytes", stats.udp_tx.bytes as f64);
        gauge!(
            "corro.transport.udp_tx.datagrams",
            stats.udp_tx.datagrams as f64
        );
        gauge!(
            "corro.transport.udp_tx.transmits",
            stats.udp_tx.transmits as f64
        );
    }
}

const NO_ERROR: quinn::VarInt = quinn::VarInt::from_u32(0);

fn test_conn(conn: &Connection) -> bool {
    match conn.close_reason() {
        None => true,
        Some(
            ConnectionError::TimedOut
            | ConnectionError::Reset
            | ConnectionError::LocallyClosed
            | ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: NO_ERROR,
                ..
            }),
        ) => {
            // don't log, pretty normal stuff
            false
        }
        Some(e) => {
            warn!("cached connection was closed abnormally, reconnecting: {e}");
            false
        }
    }
}
