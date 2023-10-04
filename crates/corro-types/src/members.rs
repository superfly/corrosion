use std::{collections::BTreeMap, net::SocketAddr, ops::Range, time::Duration};

use circular_buffer::CircularBuffer;
use tracing::{debug, trace};

use crate::{
    actor::{Actor, ActorId},
    broadcast::Timestamp,
};

#[derive(Clone, Debug)]
pub struct MemberState {
    pub addr: SocketAddr,
    pub ts: Timestamp,

    pub ring: Option<u8>,
}

impl MemberState {
    pub fn new(addr: SocketAddr, ts: Timestamp) -> Self {
        Self {
            addr,
            ts,
            ring: None,
        }
    }

    pub fn is_ring0(&self) -> bool {
        self.ring == Some(0)
    }
}

#[derive(Debug, Default)]
pub struct Rtt {
    pub buf: CircularBuffer<20, u64>,
}

#[derive(Default)]
pub struct Members {
    pub states: BTreeMap<ActorId, MemberState>,
    pub by_addr: BTreeMap<SocketAddr, ActorId>,
    pub rtts: BTreeMap<SocketAddr, Rtt>,
}

impl Members {
    pub fn get(&self, id: &ActorId) -> Option<&MemberState> {
        self.states.get(id)
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn add_member(&mut self, actor: &Actor) -> bool {
        let actor_id = actor.id();
        let member = self
            .states
            .entry(actor_id)
            .or_insert_with(|| MemberState::new(actor.addr(), actor.ts()));

        trace!("member: {member:?}");

        if actor.ts() < member.ts {
            debug!("older timestamp, ignoring");
            return false;
        }

        let newer = actor.ts() > member.ts;

        if newer {
            member.addr = actor.addr();
            member.ts = actor.ts();

            self.by_addr.insert(actor.addr(), actor.id());
            self.recalculate_rings(actor.addr());
        }

        newer
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn remove_member(&mut self, actor: &Actor) -> bool {
        let effectively_down = if let Some(member) = self.states.get(&actor.id()) {
            member.ts == actor.ts()
        } else {
            // Shouldn't happen
            false
        };

        if effectively_down {
            self.by_addr.remove(&actor.addr());
            self.states.remove(&actor.id());
        }

        effectively_down
    }

    pub fn add_rtt(&mut self, addr: SocketAddr, rtt: Duration) {
        self.rtts
            .entry(addr)
            .or_default()
            .buf
            .push_front(rtt.subsec_millis() as u64 + (rtt.as_secs() * 1000));

        self.recalculate_rings(addr)
    }

    fn recalculate_rings(&mut self, addr: SocketAddr) {
        if let Some(actor_id) = self.by_addr.get(&addr) {
            if let Some(avg) = self.rtts.get(&addr).and_then(|rtt| {
                (!rtt.buf.is_empty()).then(|| {
                    (rtt.buf.as_slices().0.iter().sum::<u64>()
                        + rtt.buf.as_slices().1.iter().sum::<u64>())
                        / rtt.buf.len() as u64
                })
            }) {
                if let Some(state) = self.states.get_mut(actor_id) {
                    for (ring, n) in BUCKETS.iter().enumerate() {
                        if n.contains(&avg) {
                            state.ring = Some(ring as u8);
                            break;
                        }
                    }
                }
            }
        }
    }

    pub fn ring0(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.states
            .values()
            .filter_map(|v| v.ring.and_then(|ring| (ring == 0).then_some(v.addr)))
    }
}

const BUCKETS: [Range<u64>; 6] = [0..5, 5..15, 15..50, 50..100, 100..200, 200..300];

#[derive(Clone)]
pub enum MemberEvent {
    Up(Actor),
    Down(Actor),
}

impl MemberEvent {
    pub fn actor(&self) -> &Actor {
        match self {
            MemberEvent::Up(actor) => actor,
            MemberEvent::Down(actor) => actor,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            MemberEvent::Up(_) => "up",
            MemberEvent::Down(_) => "down",
        }
    }
}
