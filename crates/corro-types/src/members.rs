use std::{
    cmp::Ordering,
    collections::{btree_map::Entry, BTreeMap},
    net::SocketAddr,
    ops::Range,
    time::Duration,
};

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
/// max RTT for ring assignment.
const RTT_WINDOW: usize = 20;

/// How far past a bucket boundary the RTT must sit before an adjacent ring
/// step follows it. Stops 1ms wobble on a boundary from flipping rings.
const RING_HYSTERESIS_MS: u64 = 2;

/// Index of the `RING_BUCKETS` range containing `rtt_ms`. Values at or above
/// the last bucket (>= 300ms) are clamped into the top bucket so a ring is
/// always assigned.
fn bucket_for(rtt_ms: u64) -> u8 {
    RING_BUCKETS
        .iter()
        .position(|r| r.contains(&rtt_ms))
        .unwrap_or(RING_BUCKETS.len() - 1) as u8
}

/// Compute the ring for `rtt_ms`, applying hysteresis on adjacent steps.
fn ring_with_hysteresis(current: Option<u8>, rtt_ms: u64) -> u8 {
    let target = bucket_for(rtt_ms);
    let Some(current) = current else {
        return target;
    };
    if target == current {
        return target;
    }
    if target.abs_diff(current) > 1 {
        return target;
    }

    if target > current {
        let boundary = RING_BUCKETS[current as usize].end;
        if rtt_ms >= boundary + RING_HYSTERESIS_MS {
            current + 1
        } else {
            current
        }
    } else {
        let boundary = RING_BUCKETS[current as usize].start;
        if rtt_ms + RING_HYSTERESIS_MS <= boundary {
            current - 1
        } else {
            current
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Rtt {
    pub buf: CircularBuffer<RTT_WINDOW, u64>,
}

impl Rtt {
    /// Max sample in the window, in milliseconds.
    pub fn max_ms(&self) -> Option<u64> {
        self.buf.iter().copied().max()
    }
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

    /// Max RTT in milliseconds for this member (same statistic as
    /// [`Self::recalculate_rings`]), or `None` if there are no samples yet.
    pub fn min_rtt_ms(&self, actor_id: &ActorId) -> Option<u64> {
        let addr = self.states.get(actor_id)?.addr;
        self.rtts.get(&addr)?.max_ms()
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn add_member(&mut self, actor: &Actor) -> MemberAddedResult {
        let actor_id = actor.id();

        if actor.member_id() != self.member_id {
            info!(
                "Removing member, {actor_id:?} has member_id {:?} but and our member_id is {:?}",
                actor.member_id(),
                self.member_id
            );
            let removed = self.states.remove(&actor_id);
            if let Some(member) = &removed {
                if self.by_addr.get(&member.addr) == Some(&actor_id) {
                    self.by_addr.remove(&member.addr);
                }
            }
            return if removed.is_some() {
                MemberAddedResult::Removed
            } else {
                MemberAddedResult::Ignored
            };
        }

        match self.states.entry(actor_id) {
            Entry::Vacant(e) => {
                let member = MemberState::new(
                    actor.addr(),
                    actor.ts(),
                    actor.cluster_id(),
                    actor.member_id(),
                );
                trace!("member: {member:?}");
                e.insert(member.clone());
                self.by_addr.insert(actor.addr(), actor_id);
                self.recalculate_rings(actor.addr());
                MemberAddedResult::NewMember(member)
            }
            Entry::Occupied(mut e) => {
                let member = e.get_mut();
                trace!("member: {member:?}");

                match actor.ts().to_duration().cmp(&member.ts.to_duration()) {
                    Ordering::Less | Ordering::Equal => MemberAddedResult::Ignored,
                    Ordering::Greater => {
                        let addr_changed = member.addr != actor.addr();
                        if addr_changed {
                            if self.by_addr.get(&member.addr) == Some(&actor_id) {
                                self.by_addr.remove(&member.addr);
                            }
                            self.by_addr.insert(actor.addr(), actor_id);
                            if let Some(rtt) = self.rtts.remove(&member.addr) {
                                self.rtts.insert(actor.addr(), rtt);
                            }
                            member.addr = actor.addr();
                        }
                        member.ts = actor.ts();
                        member.cluster_id = actor.cluster_id();
                        member.member_id = actor.member_id();
                        let updated = member.clone();
                        if addr_changed {
                            self.recalculate_rings(actor.addr());
                        }
                        MemberAddedResult::Updated(updated)
                    }
                }
            }
        }
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

    /// For a given member, calculate the max RTT and update `self.ring`
    /// with the index of the corresponding bucket in `RING_BUCKETS`, applying
    /// hysteresis on moves between adjacent buckets (see [`ring_with_hysteresis`])
    /// to avoid flapping when the RTT sits near a bucket boundary.
    fn recalculate_rings(&mut self, addr: SocketAddr) {
        if let Some(actor_id) = self.by_addr.get(&addr) {
            if let Some(rtt) = self.rtts.get(&addr) {
                let max = rtt.max_ms();

                let (b1, b2) = rtt.buf.as_slices();
                if let Some(max) = max {
                    if let Some(state) = self.states.get_mut(actor_id) {
                        let new_ring = ring_with_hysteresis(state.ring, max);
                        if state.ring != Some(new_ring) {
                            debug!(
                                "actor: {actor_id}, old ring: {:?}, new ring: {new_ring}, max: {max}, buf: {:?} {:?}",
                                state.ring, b1, b2
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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use uuid::Uuid;

    use super::*;

    #[test]
    fn hysteresis_holds_either_side_of_a_boundary() {
        // The band around a boundary spans it by RING_HYSTERESIS_MS in each
        // direction: 4..=8 around the 6ms boundary, 13..=17 around the 15ms one.
        assert_eq!(ring_with_hysteresis(Some(0), 7), 0);
        assert_eq!(ring_with_hysteresis(Some(0), 8), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 5), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 4), 0);

        assert_eq!(ring_with_hysteresis(Some(1), 16), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 17), 2);
        assert_eq!(ring_with_hysteresis(Some(2), 14), 2);
        assert_eq!(ring_with_hysteresis(Some(2), 13), 1);
    }

    #[test]
    fn ring_survives_the_jitter_seen_in_production() {
        // A peer wobbling 1ms on a boundary must keep a ring that holds at
        // every value it visits.
        for boundary in [6u64, 15, 50, 100, 200] {
            let held: Vec<u8> = (0..RING_BUCKETS.len() as u8)
                .filter(|ring| {
                    (boundary - 1..=boundary + 1)
                        .all(|rtt| ring_with_hysteresis(Some(*ring), rtt) == *ring)
                })
                .collect();
            assert!(
                !held.is_empty(),
                "a peer wobbling 1ms around {boundary}ms has no ring it can settle on"
            );
        }
    }

    #[test]
    fn every_rtt_settles_on_exactly_one_ring() {
        // No RTT may leave the ring oscillating, and no RTT may settle more than
        // one ring away from the bucket it actually falls in.
        for rtt in 0..300u64 {
            let settled: Vec<u8> = (0..RING_BUCKETS.len() as u8)
                .filter(|ring| ring_with_hysteresis(Some(*ring), rtt) == *ring)
                .collect();
            assert!(!settled.is_empty(), "rtt {rtt} never settles");
            for ring in settled {
                assert!(
                    ring.abs_diff(bucket_for(rtt)) <= 1,
                    "rtt {rtt} settles on ring {ring}, too far from {}",
                    bucket_for(rtt)
                );
            }
        }
    }

    #[test]
    fn first_assignment_has_no_hysteresis() {
        assert_eq!(ring_with_hysteresis(None, 10), 1);
        assert_eq!(ring_with_hysteresis(None, 20), 2);
    }

    fn test_actor(
        id: ActorId,
        addr: SocketAddr,
        ts_secs: u64,
        member_id: Option<MemberId>,
    ) -> Actor {
        Actor::new(
            id,
            addr,
            Timestamp::from(ts_secs << 32),
            ClusterId(1),
            member_id,
        )
    }

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn update_to_new_address_rekeys_by_addr() {
        let id = ActorId(Uuid::new_v4());
        let mut members = Members::new(Some(MemberId(1)));

        assert!(matches!(
            members.add_member(&test_actor(id, test_addr(1), 1, Some(MemberId(1)))),
            MemberAddedResult::NewMember(_)
        ));

        assert!(matches!(
            members.add_member(&test_actor(id, test_addr(2), 2, Some(MemberId(1)))),
            MemberAddedResult::Updated(_)
        ));

        assert_eq!(members.get(&id).unwrap().addr, test_addr(2));
        assert!(
            !members.by_addr.contains_key(&test_addr(1)),
            "stale by_addr entry leaks after an address change"
        );
        assert_eq!(members.by_addr.get(&test_addr(2)), Some(&id));
    }

    #[test]
    fn rtt_samples_follow_the_member_to_its_new_address() {
        let id = ActorId(Uuid::new_v4());
        let mut members = Members::new(Some(MemberId(1)));

        members.add_member(&test_actor(id, test_addr(1), 1, Some(MemberId(1))));
        members.add_rtt(test_addr(1), Duration::from_millis(2));
        assert_eq!(members.get(&id).unwrap().ring, Some(0));

        members.add_member(&test_actor(id, test_addr(2), 2, Some(MemberId(1))));
        members.add_rtt(test_addr(2), Duration::from_millis(100));

        assert_eq!(
            members.get(&id).unwrap().ring,
            Some(4),
            "ring must be recalculable from RTT samples taken at the new address"
        );
    }

    #[test]
    fn mismatched_member_id_removes_the_index_entry_for_the_stored_address() {
        let a = ActorId(Uuid::new_v4());
        let b = ActorId(Uuid::new_v4());
        let mut members = Members::new(Some(MemberId(1)));

        members.add_member(&test_actor(a, test_addr(1), 1, Some(MemberId(1))));
        members.add_member(&test_actor(b, test_addr(2), 1, Some(MemberId(1))));

        // A renews with a different member id and reports B's address.
        assert!(matches!(
            members.add_member(&test_actor(a, test_addr(2), 2, Some(MemberId(9)))),
            MemberAddedResult::Removed
        ));

        assert!(
            !members.by_addr.contains_key(&test_addr(1)),
            "the removed member's index entry must be dropped from its stored address"
        );
        assert_eq!(
            members.by_addr.get(&test_addr(2)),
            Some(&b),
            "another member's index entry must not be clobbered"
        );
        assert!(members.get(&b).is_some());
    }
}
