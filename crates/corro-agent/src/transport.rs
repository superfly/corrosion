use std::{borrow::Cow, collections::HashMap, net::SocketAddr, sync::Arc};

use compact_str::CompactString;
use quinn::{Connection, Endpoint};
use tokio::sync::RwLock;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct Transport {
    endpoint: Endpoint,
    conns: Arc<RwLock<HashMap<SocketAddr, HashMap<CompactString, Connection>>>>,
    default_server_name: CompactString,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
}

impl Transport {
    pub fn new(endpoint: Endpoint, default_server_name: CompactString) -> Self {
        Self {
            endpoint,
            conns: Default::default(),
            default_server_name,
        }
    }

    pub async fn connect(
        &self,
        socket: SocketAddr,
        server_name: &str,
    ) -> Result<Connection, ConnectError> {
        let server_name: Cow<'_, str> = if server_name.is_empty() {
            Cow::Owned(socket.ip().to_string())
        } else {
            Cow::Borrowed(server_name)
        };

        {
            let r = self.conns.read().await;
            if let Some(conn) = r
                .get(&socket)
                .and_then(|map| map.get(server_name.as_ref()))
                .cloned()
            {
                if let Some(reason) = conn.close_reason() {
                    warn!("cached connection was closed: {reason}");
                } else {
                    return Ok(conn);
                }
            }
        }

        let conn = {
            let mut w = self.conns.write().await;
            if let Some(conn) = w
                .get(&socket)
                .and_then(|map| map.get(server_name.as_ref()))
                .cloned()
            {
                return Ok(conn);
            }

            let conn = self.endpoint.connect(socket, server_name.as_ref())?.await?;
            w.entry(socket)
                .or_default()
                .insert(server_name.into(), conn.clone());
            conn
        };

        Ok(conn)
    }
}
