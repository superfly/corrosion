use std::{net::SocketAddr, sync::Arc};

use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;

use crate::{
    actor::ActorId, broadcast::BroadcastInput, pubsub::Subscribers, sqlite::SqlitePool, Bookie,
};

use super::members::Members;

#[derive(Clone)]
pub struct Agent(pub Arc<AgentInner>);

pub struct AgentInner {
    pub actor_id: ActorId,
    pub ro_pool: SqlitePool,
    pub rw_pool: SqlitePool,
    pub gossip_addr: SocketAddr,
    pub api_addr: Option<SocketAddr>,
    pub members: RwLock<Members>,
    pub clock: Arc<uhlc::HLC>,
    pub bookie: Bookie,
    pub consul: Option<consul::Client>,
    pub subscribers: Subscribers,
    pub tx_bcast: Sender<BroadcastInput>,
}

impl Agent {
    /// Return a borrowed [SqlitePool]
    pub fn read_only_pool(&self) -> &SqlitePool {
        &self.0.ro_pool
    }

    pub fn read_write_pool(&self) -> &SqlitePool {
        &self.0.rw_pool
    }

    pub fn actor_id(&self) -> ActorId {
        self.0.actor_id
    }

    pub fn clock(&self) -> &Arc<uhlc::HLC> {
        &self.0.clock
    }

    pub fn consul(&self) -> Option<&consul::Client> {
        self.0.consul.as_ref()
    }

    pub fn gossip_addr(&self) -> SocketAddr {
        self.0.gossip_addr
    }
    pub fn api_addr(&self) -> Option<SocketAddr> {
        self.0.api_addr
    }
    pub fn subscribers(&self) -> &Subscribers {
        &self.0.subscribers
    }

    pub fn tx_bcast(&self) -> &Sender<BroadcastInput> {
        &self.0.tx_bcast
    }

    pub fn bookie(&self) -> &Bookie {
        &self.0.bookie
    }
}
