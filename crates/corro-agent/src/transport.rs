use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use corro_types::config::GossipConfig;
use metrics::{counter, gauge, histogram};
use quinn::{
    ApplicationClose, Connection, ConnectionError, Endpoint, RecvStream, SendDatagramError,
    SendStream, WriteError,
};
use quinn_proto::ConnectionStats;
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    time::error::Elapsed,
};
use tracing::{debug, debug_span, info, warn, Instrument};

use crate::api::peer::gossip_client_endpoint;

#[derive(Debug, Clone)]
pub struct Transport(Arc<TransportInner>);

#[derive(Debug)]
struct TransportInner {
    endpoints: Vec<Endpoint>,
    conns: RwLock<HashMap<SocketAddr, Arc<Mutex<Option<Connection>>>>>,
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
    #[error(transparent)]
    TimedOut(#[from] Elapsed),
    #[error(transparent)]
    Stopped(#[from] quinn::StoppedError),
}

impl Transport {
    pub async fn new(
        config: &GossipConfig,
        rtt_tx: mpsc::Sender<(SocketAddr, Duration)>,
    ) -> eyre::Result<Self> {
        let mut endpoints = vec![];
        let endpoints_count = if config.client_addr.port() == 0 {
            // zero port means we'll use whatever is available,
            // corrosion can use multiple sockets and reduce the risk of filling kernel buffers
            8
        } else {
            // non-zero client addr port means we can only use 1
            1
        };
        for i in 0..endpoints_count {
            let ep = gossip_client_endpoint(config).await?;
            info!(
                "Transport ({i}) for outgoing connections bound to socket {}",
                ep.local_addr().unwrap()
            );
            endpoints.push(ep);
        }
        Ok(Self(Arc::new(TransportInner {
            endpoints,
            conns: Default::default(),
            rtt_tx,
        })))
    }

    #[tracing::instrument(skip(self, data), fields(buf_size = data.len()), level = "debug", err)]
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
                counter!("corro.transport.send_datagram.errors", "addr" => addr.to_string(), "error" => e.to_string()).increment(1);
                if matches!(e, SendDatagramError::TooLarge) {
                    warn!(%addr, "attempted to send a larger-than-PMTU datagram. len: {}, pmtu: {:?}", data.len(), conn.max_datagram_size());
                }
                return Err(e.into());
            }
        }

        let conn = self.connect(addr).await?;
        debug!("re-connected to {addr}");
        Ok(conn.send_datagram(data)?)
    }

    #[tracing::instrument(skip(self, data), fields(buf_size = data.len()), level = "debug", err)]
    pub async fn send_uni(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
        let conn = self.connect(addr).await?;

        let mut stream = match conn
            .open_uni()
            .instrument(debug_span!("quic_open_uni"))
            .await
        {
            Ok(stream) => stream,
            Err(e @ ConnectionError::VersionMismatch) => {
                return Err(e.into());
            }
            Err(e) => {
                debug!("retryable error attempting to open unidirectional stream: {e}");
                let conn = self.connect(addr).await?;
                conn.open_uni()
                    .instrument(debug_span!("quic_open_uni"))
                    .await?
            }
        };

        stream
            .write_chunk(data)
            .instrument(debug_span!("quic_write_chunk"))
            .await?;

        stream
            .finish()
            .expect("unreachable, the stream does not leave this method");

        stream
            .stopped()
            .instrument(debug_span!("quic_stopped"))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug", err)]
    pub async fn open_bi(
        &self,
        addr: SocketAddr,
    ) -> Result<(SendStream, RecvStream), TransportError> {
        let conn = self.connect(addr).await?;
        match conn.open_bi().instrument(debug_span!("quic_open_bi")).await {
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
        Ok(conn
            .open_bi()
            .instrument(debug_span!("quic_open_bi"))
            .await?)
    }

    async fn measured_connect(
        &self,
        addr: SocketAddr,
        server_name: String,
    ) -> Result<Connection, TransportError> {
        let start = Instant::now();

        let mut hasher = seahash::SeaHasher::new();
        addr.hash(&mut hasher);
        let endpoint_idx = (hasher.finish() % self.0.endpoints.len() as u64) as usize;

        async {
            match tokio::time::timeout(Duration::from_secs(5), self
                .0
                .endpoints[endpoint_idx]
                .connect(addr, &server_name)?)
                .await
            {
                Ok(Ok(conn)) => {
                    histogram!("corro.transport.connect.time.seconds").record(start.elapsed().as_secs_f64());
                    tracing::Span::current().record("rtt", conn.rtt().as_secs_f64());
                    Ok(conn)
                },
                Ok(Err(e)) => {
                    counter!("corro.transport.connect.errors", "addr" => server_name, "error" => e.to_string()).increment(1);
                    Err(e.into())
                }
                Err(e) => {
                    counter!("corro.transport.connect.errors", "addr" => server_name, "error" => "timed out").increment(1);
                    Err(e.into())
                }
            }
        }.instrument(debug_span!("quic_connect", %addr, rtt = tracing::field::Empty)).await
    }

    // this shouldn't block for long...
    async fn get_lock(&self, addr: SocketAddr) -> Arc<Mutex<Option<Connection>>> {
        {
            let r = self.0.conns.read().await;
            if let Some(lock) = r.get(&addr) {
                return lock.clone();
            }
        }

        let mut w = self.0.conns.write().await;
        w.entry(addr).or_default().clone()
    }

    #[tracing::instrument(skip(self), fields(tid = ?std::thread::current().id()), level = "debug", err)]
    async fn connect(&self, addr: SocketAddr) -> Result<Connection, TransportError> {
        let conn_lock = self.get_lock(addr).await;

        let mut lock = conn_lock.lock().await;

        if let Some(conn) = lock.as_ref() {
            if test_conn(conn) {
                if let Err(e) = self.0.rtt_tx.try_send((addr, conn.rtt())) {
                    debug!("could not send RTT for connection through sender: {e}");
                }
                return Ok(conn.clone());
            }
        }

        // clear it, if there was one it didn't pass the test.
        *lock = None;

        let conn = self.measured_connect(addr, addr.ip().to_string()).await?;
        *lock = Some(conn.clone());
        Ok(conn)
    }

    pub fn emit_metrics(&self) {
        let conns = {
            let read = self.0.conns.blocking_read();
            read.iter()
                .filter_map(|(addr, conn)| {
                    conn.blocking_lock()
                        .as_ref()
                        .map(|conn| (*addr, conn.stats()))
                })
                .collect::<Vec<_>>()
        };

        gauge!("corro.transport.connections").set(conns.len() as f64);

        // make aggregate stats for all connections... so as to not overload a metrics server
        let stats = conns
            .iter()
            .fold(ConnectionStats::default(), |mut acc, (addr, stats)| {
                gauge!("corro.transport.path.cwnd", "addr" => addr.to_string())
                    .set(stats.path.cwnd as f64);
                gauge!("corro.transport.path.congestion_events", "addr" => addr.to_string())
                    .set(stats.path.congestion_events as f64);
                gauge!("corro.transport.path.black_holes_detected", "addr" => addr.to_string())
                    .set(stats.path.black_holes_detected as f64);

                acc.path.lost_packets += stats.path.lost_packets;
                acc.path.lost_bytes += stats.path.lost_bytes;
                acc.path.sent_packets += stats.path.sent_packets;
                acc.path.sent_plpmtud_probes += stats.path.sent_plpmtud_probes;
                acc.path.lost_plpmtud_probes += stats.path.lost_plpmtud_probes;

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
                acc.udp_rx.ios += stats.udp_rx.ios;

                acc.udp_tx.bytes += stats.udp_tx.bytes;
                acc.udp_tx.datagrams += stats.udp_tx.datagrams;
                acc.udp_tx.ios += stats.udp_tx.ios;

                acc
            });
        gauge!("corro.transport.path.lost_packets").set(stats.path.lost_packets as f64);
        gauge!("corro.transport.path.lost_bytes").set(stats.path.lost_bytes as f64);
        gauge!("corro.transport.path.sent_packets").set(stats.path.sent_packets as f64);
        gauge!("corro.transport.path.sent_plpmtud_probes")
            .set(stats.path.sent_plpmtud_probes as f64);
        gauge!("corro.transport.path.lost_plpmtud_probes")
            .set(stats.path.lost_plpmtud_probes as f64);

        gauge!("corro.transport.frame_rx", "type" => "acks").set(stats.frame_rx.acks as f64);
        gauge!("corro.transport.frame_rx", "type" => "crypto").set(stats.frame_rx.crypto as f64);
        gauge!("corro.transport.frame_rx", "type" => "connection_close")
            .set(stats.frame_rx.connection_close as f64);
        gauge!("corro.transport.frame_rx", "type" => "data_blocked")
            .set(stats.frame_rx.data_blocked as f64);
        gauge!("corro.transport.frame_rx", "type" => "datagram")
            .set(stats.frame_rx.datagram as f64);
        gauge!("corro.transport.frame_rx", "type" => "handshake_done")
            .set(stats.frame_rx.handshake_done as f64);
        gauge!("corro.transport.frame_rx", "type" => "max_data")
            .set(stats.frame_rx.max_data as f64);
        gauge!("corro.transport.frame_rx", "type" => "max_stream_data")
            .set(stats.frame_rx.max_stream_data as f64);
        gauge!("corro.transport.frame_rx", "type" => "max_streams_bidi")
            .set(stats.frame_rx.max_streams_bidi as f64);
        gauge!("corro.transport.frame_rx", "type" => "max_streams_uni")
            .set(stats.frame_rx.max_streams_uni as f64);
        gauge!("corro.transport.frame_rx", "type" => "new_connection_id")
            .set(stats.frame_rx.new_connection_id as f64);
        gauge!("corro.transport.frame_rx", "type" => "new_token")
            .set(stats.frame_rx.new_token as f64);
        gauge!("corro.transport.frame_rx", "type" => "path_challenge")
            .set(stats.frame_rx.path_challenge as f64);
        gauge!("corro.transport.frame_rx", "type" => "path_response")
            .set(stats.frame_rx.path_response as f64);
        gauge!("corro.transport.frame_rx", "type" => "ping").set(stats.frame_rx.ping as f64);
        gauge!("corro.transport.frame_rx", "type" => "reset_stream")
            .set(stats.frame_rx.reset_stream as f64);
        gauge!("corro.transport.frame_rx", "type" => "retire_connection_id")
            .set(stats.frame_rx.retire_connection_id as f64);
        gauge!("corro.transport.frame_rx", "type" => "stream_data_blocked")
            .set(stats.frame_rx.stream_data_blocked as f64);
        gauge!("corro.transport.frame_rx", "type" => "streams_blocked_bidi")
            .set(stats.frame_rx.streams_blocked_bidi as f64);
        gauge!("corro.transport.frame_rx", "type" => "streams_blocked_uni")
            .set(stats.frame_rx.streams_blocked_uni as f64);
        gauge!("corro.transport.frame_rx", "type" => "stop_sending")
            .set(stats.frame_rx.stop_sending as f64);
        gauge!("corro.transport.frame_rx", "type" => "stream").set(stats.frame_rx.stream as f64);

        gauge!("corro.transport.frame_tx", "type" => "acks").set(stats.frame_tx.acks as f64);
        gauge!("corro.transport.frame_tx", "type" => "crypto").set(stats.frame_tx.crypto as f64);
        gauge!("corro.transport.frame_tx", "type" => "connection_close")
            .set(stats.frame_tx.connection_close as f64);
        gauge!("corro.transport.frame_tx", "type" => "data_blocked")
            .set(stats.frame_tx.data_blocked as f64);
        gauge!("corro.transport.frame_tx", "type" => "datagram")
            .set(stats.frame_tx.datagram as f64);
        gauge!("corro.transport.frame_tx", "type" => "handshake_done")
            .set(stats.frame_tx.handshake_done as f64);
        gauge!("corro.transport.frame_tx", "type" => "max_data")
            .set(stats.frame_tx.max_data as f64);
        gauge!("corro.transport.frame_tx", "type" => "max_stream_data")
            .set(stats.frame_tx.max_stream_data as f64);
        gauge!("corro.transport.frame_tx", "type" => "max_streams_bidi")
            .set(stats.frame_tx.max_streams_bidi as f64);
        gauge!("corro.transport.frame_tx", "type" => "max_streams_uni")
            .set(stats.frame_tx.max_streams_uni as f64);
        gauge!("corro.transport.frame_tx", "type" => "new_connection_id")
            .set(stats.frame_tx.new_connection_id as f64);
        gauge!("corro.transport.frame_tx", "type" => "new_token")
            .set(stats.frame_tx.new_token as f64);
        gauge!("corro.transport.frame_tx", "type" => "path_challenge")
            .set(stats.frame_tx.path_challenge as f64);
        gauge!("corro.transport.frame_tx", "type" => "path_response")
            .set(stats.frame_tx.path_response as f64);
        gauge!("corro.transport.frame_tx", "type" => "ping").set(stats.frame_tx.ping as f64);
        gauge!("corro.transport.frame_tx", "type" => "reset_stream")
            .set(stats.frame_tx.reset_stream as f64);
        gauge!("corro.transport.frame_tx", "type" => "retire_connection_id")
            .set(stats.frame_tx.retire_connection_id as f64);
        gauge!("corro.transport.frame_tx", "type" => "stream_data_blocked")
            .set(stats.frame_tx.stream_data_blocked as f64);
        gauge!("corro.transport.frame_tx", "type" => "streams_blocked_bidi")
            .set(stats.frame_tx.streams_blocked_bidi as f64);
        gauge!("corro.transport.frame_tx", "type" => "streams_blocked_uni")
            .set(stats.frame_tx.streams_blocked_uni as f64);
        gauge!("corro.transport.frame_tx", "type" => "stop_sending")
            .set(stats.frame_tx.stop_sending as f64);
        gauge!("corro.transport.frame_tx", "type" => "stream").set(stats.frame_tx.stream as f64);

        gauge!("corro.transport.udp_rx.bytes").set(stats.udp_rx.bytes as f64);
        gauge!("corro.transport.udp_rx.datagrams").set(stats.udp_rx.datagrams as f64);
        gauge!("corro.transport.udp_rx.transmits").set(stats.udp_rx.ios as f64);

        gauge!("corro.transport.udp_tx.bytes").set(stats.udp_tx.bytes as f64);
        gauge!("corro.transport.udp_tx.datagrams").set(stats.udp_tx.datagrams as f64);
        gauge!("corro.transport.udp_tx.transmits").set(stats.udp_tx.ios as f64);
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
