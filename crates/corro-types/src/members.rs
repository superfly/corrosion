use std::{collections::BTreeMap, net::SocketAddr};

use tracing::trace;

use crate::actor::{Actor, ActorId};

#[derive(Clone, Debug)]
pub struct MemberState {
    pub addr: SocketAddr,

    counter: u8,
}

impl MemberState {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, counter: 0 }
    }
}

#[derive(Default)]
pub struct Members {
    pub states: BTreeMap<ActorId, MemberState>,
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
            .or_insert_with(|| MemberState::new(actor.addr()));

        member.addr = actor.addr();

        trace!("member: {member:?}");

        member.counter += 1;
        member.counter == 1
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn remove_member(&mut self, actor: &Actor) -> bool {
        let effectively_down = if let Some(member) = self.states.get_mut(&actor.id()) {
            member.counter -= 1;
            member.counter == 0
        } else {
            // Shouldn't happen
            false
        };

        if effectively_down {
            self.states.remove(&actor.id());
        }

        effectively_down
    }
}

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
