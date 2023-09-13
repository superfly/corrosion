use std::{collections::BTreeMap, net::SocketAddr};

use jsonpath_rust::JsonPathInst;
use tracing::trace;

use crate::{
    actor::{Actor, ActorId},
    config::NodeMeta,
};

#[derive(Clone, Debug)]
pub struct MemberState {
    pub addr: SocketAddr,

    pub priority_broadcast: bool,

    counter: u8,
}

impl MemberState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            priority_broadcast: false,
            counter: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MembersError {
    #[error("could not parse priority filter: {0}")]
    PriorityFilterParse(String),
}

#[derive(Default)]
pub struct Members {
    pub states: BTreeMap<ActorId, MemberState>,
    pub meta: BTreeMap<ActorId, NodeMeta>,
    pub priority_filter: Option<JsonPathInst>,
}

impl Members {
    pub fn new(priority_filter: Option<&str>) -> Result<Self, MembersError> {
        let priority_filter = if let Some(filter) = priority_filter {
            Some(filter.parse().map_err(MembersError::PriorityFilterParse)?)
        } else {
            None
        };
        Ok(Self {
            priority_filter,
            ..Default::default()
        })
    }

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
        let new_entry = member.counter == 1;

        if new_entry {
            self.set_priority(&actor_id);
        }

        new_entry
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

    pub fn set_meta(&mut self, actor_id: &ActorId, meta: NodeMeta) {
        self.meta.insert(*actor_id, meta);
        self.set_priority(actor_id);
    }

    fn set_priority(&mut self, actor_id: &ActorId) {
        if let Some(filter) = &self.priority_filter {
            if let Some(meta) = self.meta.get(actor_id) {
                if let Some(state) = self.states.get_mut(actor_id) {
                    let v = serde_json::Value::from_iter(
                        meta.iter().map(|(k, v)| (k.clone(), v.clone())),
                    );
                    state.priority_broadcast = !filter.find_slice(&v).is_empty();
                }
            }
        }
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
