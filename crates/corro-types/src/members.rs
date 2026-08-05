use std::{collections::BTreeMap, net::SocketAddr, ops::Range, time::Duration};

use circular_buffer::CircularBuffer;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::{
    actor::{Actor, ActorId, ClusterId, MemberId},
    broadcast::Timestamp,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberState {
    pub addr: SocketAddr,
    pub ts: Timestamp,
    pub cluster_id: ClusterId,

    pub ring: Option<u8>,
    pub last_sync_ts: Option<Timestamp>,
    pub member_id: Option<MemberId>,
}

impl MemberState {
    pub fn new(
        addr: SocketAddr,
        ts: Timestamp,
        cluster_id: ClusterId,
        member_id: Option<MemberId>,
    ) -> Self {
        Self {
            addr,
            ts,
            cluster_id,
            ring: None,
            last_sync_ts: None,
            member_id,
        }
    }

    pub fn is_ring0(&self) -> bool {
        self.ring == Some(0)
    }

    pub fn to_actor(&self, id: ActorId) -> Actor {
        Actor::new(id, self.addr, self.ts, self.cluster_id, self.member_id)
    }
}

const RING_BUCKETS: [Range<u64>; 6] = [0..6, 6..15, 15..50, 50..100, 100..200, 200..300];

/// Number of recent RTT samples retained per member and used to compute the
/// median RTT for ring assignment.
const RTT_WINDOW: usize = 20;

/// Hysteresis dead-band as a percentage of the shared boundary value, applied
/// only to moves between *adjacent* ring buckets. Prevents ring flapping when
/// the average RTT sits right on a bucket boundary. Non-adjacent jumps are not
/// affected and switch immediately.
const RING_HYSTERESIS_PCT: u64 = 20;

/// Index of the `RING_BUCKETS` range containing `avg`. Values at or above the
/// last bucket (>= 300ms) are clamped into the top bucket so a ring is always
/// assigned.
fn bucket_for(avg: u64) -> u8 {
    RING_BUCKETS
        .iter()
        .position(|r| r.contains(&avg))
        .unwrap_or(RING_BUCKETS.len() - 1) as u8
}

/// Compute the ring for `avg`, applying hysteresis only when new ring bucket
/// is adjacent to prevent small rtt jumps from moving rings.
fn ring_with_hysteresis(current: Option<u8>, avg: u64) -> u8 {
    let target = bucket_for(avg);
    let Some(current) = current else {
        return target;
    };
    if target == current || target.abs_diff(current) != 1 {
        return target;
    }
    if target > current {
        // Moving up: exceed the end-of-current boundary by the margin.
        let boundary = RING_BUCKETS[current as usize].end;
        let margin = boundary * RING_HYSTERESIS_PCT / 100;
        if avg >= boundary + margin {
            target
        } else {
            current
        }
    } else {
        // Moving down: drop below the start-of-current boundary by the margin.
        let boundary = RING_BUCKETS[current as usize].start;
        let margin = boundary * RING_HYSTERESIS_PCT / 100;
        if avg + margin <= boundary {
            target
        } else {
            current
        }
    }
}

fn median_rtt(rtt: &Rtt) -> Option<u64> {
    let len = rtt.buf.len();
    if len == 0 {
        return None;
    }

    let mut scratch = [0u64; RTT_WINDOW];
    let (head, tail) = rtt.buf.as_slices();
    scratch[..head.len()].copy_from_slice(head);
    scratch[head.len()..len].copy_from_slice(tail);

    let values = &mut scratch[..len];
    values.sort_unstable();

    let mid = len / 2;
    Some(if len % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2
    } else {
        values[mid]
    })
}

#[derive(Debug, Default, Clone)]
pub struct Rtt {
    pub buf: CircularBuffer<RTT_WINDOW, u64>,
}

#[derive(Default)]
pub struct Members {
    pub member_id: Option<MemberId>,
    pub states: BTreeMap<ActorId, MemberState>,
    pub by_addr: BTreeMap<SocketAddr, ActorId>,
    pub rtts: BTreeMap<SocketAddr, Rtt>,
}

#[derive(Debug)]
pub enum MemberAddedResult {
    NewMember(MemberState),
    Removed,
    Updated(MemberState),
    Ignored,
}

impl Members {
    pub fn new(member_id: Option<MemberId>) -> Self {
        Members {
            member_id,
            ..Default::default()
        }
    }

    pub fn get(&self, id: &ActorId) -> Option<&MemberState> {
        self.states.get(id)
    }

    pub fn update_sync_ts(&mut self, actor_id: &ActorId, ts: Timestamp) {
        if let Some(state) = self.states.get_mut(actor_id) {
            state.last_sync_ts = Some(ts);
        }
    }

    /// Median RTT in milliseconds for this member (same statistic as
    /// [`Self::recalculate_rings`]), or `None` if there are no samples yet.
    pub fn avg_rtt_ms(&self, actor_id: &ActorId) -> Option<u64> {
        let addr = self.states.get(actor_id)?.addr;
        median_rtt(self.rtts.get(&addr)?)
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn add_member(&mut self, actor: &Actor) -> MemberAddedResult {
        let actor_id = actor.id();
        let mut ret = MemberAddedResult::Ignored;

        if actor.member_id() != self.member_id {
            info!(
                "Removing member, {actor_id:?} has member_id {:?} but and our member_id is {:?}",
                actor.member_id(),
                self.member_id
            );
            let removed = self.states.remove(&actor_id).is_some();
            self.by_addr.remove(&actor.addr());
            return if removed {
                MemberAddedResult::Removed
            } else {
                MemberAddedResult::Ignored
            };
        }

        let is_new = !self.states.contains_key(&actor_id);
        let member = self.states.entry(actor_id).or_insert_with(|| {
            MemberState::new(
                actor.addr(),
                actor.ts(),
                actor.cluster_id(),
                actor.member_id(),
            )
        });

        if is_new {
            ret = MemberAddedResult::NewMember(member.clone());
        }

        trace!("member: {member:?}");

        // The received timestamp is older than the previously known
        // one.  If we just added the member this shouldn't ever
        // trigger (because the timestamps would be the same).
        if actor.ts().to_duration() < member.ts.to_duration() {
            debug!("older timestamp, ignoring");
            return MemberAddedResult::Ignored;
        }

        // If the new timestamp is newer than what we had on file we
        // update the member, then set the return to "Update".
        // Because a newly inserted member would always have the same
        // timestamp this code doesn't run if we just inserted.
        if actor.ts().to_duration() > member.ts.to_duration() {
            member.addr = actor.addr();
            member.ts = actor.ts();
            member.cluster_id = actor.cluster_id();
            member.member_id = actor.member_id();
            ret = MemberAddedResult::Updated(member.clone());
        }

        // If we just inserted, add the actor to the by_addr set and
        // recalculate the RTT rings.
        if matches!(ret, MemberAddedResult::NewMember(_)) {
            self.by_addr.insert(actor.addr(), actor.id());
            self.recalculate_rings(actor.addr());
        }

        ret
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

    /// For a given member, calculate the median RTT and update `self.ring` with
    /// the index of the corresponding bucket in `RING_BUCKETS`, applying
    /// hysteresis on moves between adjacent buckets (see [`ring_with_hysteresis`])
    /// to avoid flapping when the RTT sits near a bucket boundary.
    fn recalculate_rings(&mut self, addr: SocketAddr) {
        if let Some(actor_id) = self.by_addr.get(&addr) {
            if let Some(rtt) = self.rtts.get(&addr) {
                let median = median_rtt(rtt);

                if let Some(median) = median {
                    if let Some(state) = self.states.get_mut(actor_id) {
                        let new_ring = ring_with_hysteresis(state.ring, median);
                        if state.ring != Some(new_ring) {
                            info!(
                                "actor: {actor_id}, rtt: {:?}{:?}, old ring: {:?}, new ring: {new_ring}, median: {median}",
                                rtt.buf.as_slices().0,
                                rtt.buf.as_slices().1,
                                state.ring,
                            );
                        }
                        state.ring = Some(new_ring);
                    }
                }
            }
        }
    }

    /// Get member addresses where the ring index is `0` (meaning a
    /// very small RTT)
    pub fn ring0(&self, cluster_id: ClusterId) -> impl Iterator<Item = SocketAddr> + '_ {
        self.states.values().filter_map(move |v| {
            v.ring
                .and_then(|ring| (v.cluster_id == cluster_id && ring == 0).then_some(v.addr))
        })
    }
}
