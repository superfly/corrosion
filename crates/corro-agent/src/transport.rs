use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use quinn::{
    ApplicationClose, Connection, ConnectionError, Endpoint, RecvStream, SendDatagramError,
    SendStream,
};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, trace, warn};

#[derive(Debug, Clone)]
pub struct Transport(Arc<TransportInner>);

#[derive(Debug)]
struct TransportInner {
    endpoint: Endpoint,
    conns: RwLock<HashMap<SocketAddr, Connection>>,
    rtt_tx: mpsc::Sender<(SocketAddr, Duration)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    Datagram(#[from] SendDatagramError),
}

impl Transport {
    pub fn new(endpoint: Endpoint, rtt_tx: mpsc::Sender<(SocketAddr, Duration)>) -> Self {
        Self(Arc::new(TransportInner {
            endpoint,
            conns: Default::default(),
            rtt_tx,
        }))
    }

    pub async fn send_datagram(&self, addr: SocketAddr, data: Bytes) -> Result<(), ConnectError> {
        let conn = self.connect(addr).await?;
        debug!("connected to {addr}");
        match conn.send_datagram(data.clone()) {
            Ok(send) => {
                trace!("sent datagram to {addr}");
                return Ok(send);
            }
            Err(e @ SendDatagramError::ConnectionLost(ConnectionError::VersionMismatch)) => {
                return Err(e.into());
            }
            Err(SendDatagramError::ConnectionLost(e)) => {
                debug!("retryable error attempting to open unidirectional stream: {e}");
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        let conn = self.connect(addr).await?;
        debug!("re-connected to {addr}");
        Ok(conn.send_datagram(data)?)
    }

    pub async fn open_uni(&self, addr: SocketAddr) -> Result<SendStream, ConnectError> {
        let conn = self.connect(addr).await?;
        match conn.open_uni().await {
            Ok(send) => return Ok(send),
            Err(e @ ConnectionError::VersionMismatch) => {
                return Err(e.into());
            }
            Err(e) => {
                debug!("retryable error attempting to open unidirectional stream: {e}");
            }
        }

        let conn = self.connect(addr).await?;
        Ok(conn.open_uni().await?)
    }

    pub async fn open_bi(
        &self,
        addr: SocketAddr,
    ) -> Result<(SendStream, RecvStream), ConnectError> {
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

    async fn connect(&self, addr: SocketAddr) -> Result<Connection, ConnectError> {
        let server_name = addr.ip().to_string();

        {
            let r = self.0.conns.read().await;
            if let Some(conn) = r.get(&addr).cloned() {
                if test_conn(&conn) {
                    if let Err(e) = self.0.rtt_tx.try_send((addr, conn.rtt())) {
                        warn!("could not send RTT for connection through sender: {e}");
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

            let conn = self.0.endpoint.connect(addr, server_name.as_str())?.await?;
            if let Err(e) = self.0.rtt_tx.try_send((addr, conn.rtt())) {
                warn!("could not send RTT for connection through sender: {e}");
            }
            w.insert(addr, conn.clone());
            conn
        };

        Ok(conn)
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
