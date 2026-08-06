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
/// minimum RTT for ring assignment.
const RTT_WINDOW: usize = 20;

/// Hysteresis dead-band as a percentage of the shared boundary value, applied
/// when stepping to an adjacent ring. Prevents ring flapping when the RTT sits
/// right on a bucket boundary.
const RING_HYSTERESIS_PCT: u64 = 25;

/// Floor on the hysteresis margin in ms. Percent-only margins are tiny on the
/// low ring edges (20% of 15ms ≈ 3ms), which is where Near↔Mid flaps in practice.
const RING_HYSTERESIS_MIN_MS: u64 = 8;

/// Index of the `RING_BUCKETS` range containing `rtt_ms`. Values at or above
/// the last bucket (>= 300ms) are clamped into the top bucket so a ring is
/// always assigned.
fn bucket_for(rtt_ms: u64) -> u8 {
    RING_BUCKETS
        .iter()
        .position(|r| r.contains(&rtt_ms))
        .unwrap_or(RING_BUCKETS.len() - 1) as u8
}

fn hysteresis_margin(boundary: u64) -> u64 {
    let m = (boundary * RING_HYSTERESIS_PCT / 100).max(RING_HYSTERESIS_MIN_MS);
    // Never exceed the boundary itself — otherwise a ring could become
    // impossible to leave downward (e.g. start=6 with margin=8).
    m.min(boundary)
}

/// Compute the ring for `rtt_ms`, applying hysteresis on adjacent steps.
///
/// Multi-bucket jumps move at most one ring per update so a single noisy
/// sample cannot skip hysteresis (e.g. ring 2 → 0).
fn ring_with_hysteresis(current: Option<u8>, rtt_ms: u64) -> u8 {
    let target = bucket_for(rtt_ms);
    let Some(current) = current else {
        return target;
    };
    if target == current {
        return target;
    }

    // Step at most one ring toward the target; hysteresis applies to that step.
    if target > current {
        let boundary = RING_BUCKETS[current as usize].end;
        let margin = hysteresis_margin(boundary);
        if rtt_ms >= boundary + margin {
            current + 1
        } else {
            current
        }
    } else {
        let boundary = RING_BUCKETS[current as usize].start;
        let margin = hysteresis_margin(boundary);
        if rtt_ms + margin <= boundary {
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
    /// Smallest sample in the window, in milliseconds.
    ///
    /// Samples are reads of quinn's smoothed RTT, not independent
    /// measurements: one delayed ACK inflates the estimator and it then decays
    /// back over roughly a full window, so most of the buffer can be tail from
    /// a single stall. The minimum recovers the propagation floor, which is
    /// what the rings are meant to describe.
    pub fn min_ms(&self) -> Option<u64> {
        self.buf.iter().min().copied()
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

    /// Minimum RTT in milliseconds for this member (same statistic as
    /// [`Self::recalculate_rings`]), or `None` if there are no samples yet.
    pub fn min_rtt_ms(&self, actor_id: &ActorId) -> Option<u64> {
        let addr = self.states.get(actor_id)?.addr;
        self.rtts.get(&addr)?.min_ms()
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

    /// For a given member, calculate the minimum RTT and update `self.ring`
    /// with the index of the corresponding bucket in `RING_BUCKETS`, applying
    /// hysteresis on moves between adjacent buckets (see [`ring_with_hysteresis`])
    /// to avoid flapping when the RTT sits near a bucket boundary.
    fn recalculate_rings(&mut self, addr: SocketAddr) {
        if let Some(actor_id) = self.by_addr.get(&addr) {
            if let Some(rtt) = self.rtts.get(&addr) {
                let min = rtt.min_ms();

                let (b1, b2) = rtt.buf.as_slices();
                if let Some(min) = min {
                    if let Some(state) = self.states.get_mut(actor_id) {
                        let new_ring = ring_with_hysteresis(state.ring, min);
                        if state.ring != Some(new_ring) {
                            debug!(
                                "actor: {actor_id}, old ring: {:?}, new ring: {new_ring}, min: {min}, buf: {:?} {:?}",
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
    use super::*;

    #[test]
    fn hysteresis_holds_near_mid_boundary() {
        // Ring 1 is 6..15, ring 2 is 15..50. Margin at 15ms is max(25%*15, 8) = 8.
        // Stay in ring 1 until avg >= 23; stay in ring 2 until avg <= 7.
        assert_eq!(ring_with_hysteresis(Some(1), 18), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 22), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 23), 2);

        assert_eq!(ring_with_hysteresis(Some(2), 12), 2);
        assert_eq!(ring_with_hysteresis(Some(2), 8), 2);
        assert_eq!(ring_with_hysteresis(Some(2), 7), 1);
    }

    #[test]
    fn hysteresis_steps_one_ring_at_a_time() {
        // A sudden drop from mid-ring RTT must not skip straight to ring 0.
        assert_eq!(ring_with_hysteresis(Some(2), 3), 1);
        // Leaving ring 1 downward still needs to clear its start bound (avg 0).
        assert_eq!(ring_with_hysteresis(Some(1), 3), 1);
        assert_eq!(ring_with_hysteresis(Some(1), 0), 0);
    }

    #[test]
    fn first_assignment_has_no_hysteresis() {
        assert_eq!(ring_with_hysteresis(None, 10), 1);
        assert_eq!(ring_with_hysteresis(None, 20), 2);
    }
}
