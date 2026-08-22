use super::{RestartDirective, RestartReason, SupervisedActor};
use crate::{
    agent::{handlers, util},
    transport::Transport,
};
use bytes::Bytes;
use corro_types::{
    actor::Actor as CorroActor,
    agent::{Agent, Bookie},
    channel::CorroReceiver,
};
use foca::OwnedNotification;
use std::future::Future;
use tripwire::Tripwire;

pub(in crate::agent) struct SyncActor {
    agent: Agent,
    bookie: Bookie,
    transport: Transport,
}

impl SyncActor {
    pub(in crate::agent) fn new(agent: Agent, bookie: Bookie, transport: Transport) -> Self {
        Self {
            agent,
            bookie,
            transport,
        }
    }
}

impl SupervisedActor for SyncActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "sync"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let agent = self.agent.clone();
        let bookie = self.bookie.clone();
        let transport = self.transport.clone();

        async move {
            util::sync_loop(agent, bookie, transport, tripwire).await;
            Ok(())
        }
    }
}

fn notifications_restart_directive(reason: RestartReason) -> RestartDirective {
    match reason {
        RestartReason::Completed | RestartReason::Failed | RestartReason::Panicked => {
            RestartDirective::Escalate
        }
    }
}

pub(in crate::agent) struct NotificationsActor {
    agent: Agent,
    receiver: CorroReceiver<OwnedNotification<CorroActor>>,
}

impl NotificationsActor {
    pub(in crate::agent) fn new(
        agent: Agent,
        receiver: CorroReceiver<OwnedNotification<CorroActor>>,
    ) -> Self {
        Self { agent, receiver }
    }
}

impl SupervisedActor for NotificationsActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "notifications"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let agent = self.agent.clone();
        let receiver = &mut self.receiver;

        async move {
            handlers::handle_notifications(agent, receiver, tripwire).await;
            Ok(())
        }
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        notifications_restart_directive(reason)
    }
}

fn gossip_to_send_restart_directive(reason: RestartReason) -> RestartDirective {
    match reason {
        RestartReason::Completed => RestartDirective::Escalate,
        RestartReason::Failed | RestartReason::Panicked => RestartDirective::Restart,
    }
}

pub(in crate::agent) struct GossipToSendActor {
    transport: Transport,
    receiver: CorroReceiver<(CorroActor, Bytes)>,
}

impl GossipToSendActor {
    pub(in crate::agent) fn new(
        transport: Transport,
        receiver: CorroReceiver<(CorroActor, Bytes)>,
    ) -> Self {
        Self {
            transport,
            receiver,
        }
    }
}

impl SupervisedActor for GossipToSendActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "gossip-to-send"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let transport = self.transport.clone();
        let receiver = &mut self.receiver;

        async move {
            handlers::handle_gossip_to_send(transport, receiver, tripwire).await;
            Ok(())
        }
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        gossip_to_send_restart_directive(reason)
    }
}

impl SupervisedActor for handlers::ChangesActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "changes"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        handlers::ChangesActor::run(self, tripwire).await;
        Ok(())
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed => RestartDirective::Restart,
            RestartReason::Panicked => RestartDirective::Escalate,
        }
    }

    fn on_escalate(&mut self, reason: RestartReason) {
        debug_assert_eq!(reason, RestartReason::Panicked);
        self.mark_unhealthy("changes actor panicked");
    }
}

impl SupervisedActor for util::ApplyBufferedActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "apply-buffered"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        util::ApplyBufferedActor::run(self, tripwire).await;
        Ok(())
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed => RestartDirective::Restart,
            RestartReason::Panicked => RestartDirective::Escalate,
        }
    }

    fn on_escalate(&mut self, reason: RestartReason) {
        debug_assert_eq!(reason, RestartReason::Panicked);
        self.mark_unhealthy("apply-buffered actor panicked");
    }
}

impl SupervisedActor for util::ClearBufferedMetaActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "clear-buffered-meta"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        util::ClearBufferedMetaActor::run(self, tripwire).await;
        Ok(())
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed | RestartReason::Panicked => RestartDirective::Restart,
        }
    }
}

impl SupervisedActor for handlers::DbMaintenanceActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "db-maintenance"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        handlers::DbMaintenanceActor::run(self, tripwire).await;
        Ok(())
    }
}

impl SupervisedActor for crate::plumtree::PlumtreeActor {
    type Error = eyre::Report;

    fn name(&self) -> &'static str {
        "plumtree"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        crate::plumtree::PlumtreeActor::run(self, tripwire)
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed | RestartReason::Panicked => RestartDirective::Restart,
        }
    }
}

impl SupervisedActor for crate::broadcast::GossipBroadcastActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "gossip-broadcast"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        crate::broadcast::GossipBroadcastActor::run(self, tripwire).await;

        Ok(())
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed | RestartReason::Panicked => RestartDirective::Restart,
        }
    }
}

pub(in crate::agent) struct AgentMetricsActor {
    agent: Agent,
    transport: Transport,
}

impl AgentMetricsActor {
    pub(in crate::agent) fn new(agent: Agent, transport: Transport) -> Self {
        Self { agent, transport }
    }
}

impl SupervisedActor for AgentMetricsActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "agent-metrics"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        crate::agent::metrics::metrics_loop(self.agent.clone(), self.transport.clone(), tripwire)
            .await;

        Ok(())
    }
}

pub(in crate::agent) struct QueryMetricsActor;

impl QueryMetricsActor {
    pub(in crate::agent) fn new() -> Self {
        Self
    }
}

impl SupervisedActor for QueryMetricsActor {
    type Error = std::convert::Infallible;

    fn name(&self) -> &'static str {
        "query-metrics"
    }

    async fn run(&mut self, tripwire: Tripwire) -> Result<(), Self::Error> {
        corro_types::sqlite::query_metrics_loop(tripwire).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_escalate_on_every_non_shutdown_exit() {
        for reason in [
            RestartReason::Completed,
            RestartReason::Failed,
            RestartReason::Panicked,
        ] {
            assert_eq!(
                notifications_restart_directive(reason),
                RestartDirective::Escalate
            );
        }
    }

    #[test]
    fn gossip_sender_escalates_on_channel_close() {
        assert_eq!(
            gossip_to_send_restart_directive(RestartReason::Completed),
            RestartDirective::Escalate
        );
        assert_eq!(
            gossip_to_send_restart_directive(RestartReason::Failed),
            RestartDirective::Restart
        );
        assert_eq!(
            gossip_to_send_restart_directive(RestartReason::Panicked),
            RestartDirective::Restart
        );
    }
}
