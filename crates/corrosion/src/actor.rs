use std::{net::SocketAddr, ops::Deref};

use foca::Identity;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize, Readable, Writable)]
#[serde(transparent)]
pub struct ActorId(pub Uuid);

impl Deref for ActorId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct Actor {
    id: ActorId,
    addr: SocketAddr,

    // An extra field to allow fast rejoin
    bump: u16,
}

impl Actor {
    pub fn new(id: ActorId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            bump: rand::random(),
        }
    }

    pub fn id(&self) -> ActorId {
        self.id
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl From<SocketAddr> for Actor {
    fn from(value: SocketAddr) -> Self {
        Self::new(ActorId(Uuid::nil()), value)
    }
}

impl Identity for Actor {
    // Since a client outside the cluster will not be aware of our
    // `bump` field, we implement the optional trait method
    // `has_same_prefix` to allow anyone that knows our `addr`
    // to join our cluster.
    fn has_same_prefix(&self, other: &Self) -> bool {
        // this happens if we're announcing ourselves to another node
        // we don't yet have any info about them, except their gossip addr
        if other.id.is_nil() || self.id.is_nil() {
            self.addr.eq(&other.addr)
        } else {
            self.id.eq(&other.id)
        }
    }

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            id: self.id,
            addr: self.addr,
            bump: self.bump.wrapping_add(1),
        })
    }
}
