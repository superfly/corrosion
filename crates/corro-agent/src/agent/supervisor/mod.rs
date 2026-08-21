use futures::FutureExt;
use metrics::counter;
use spawn::spawn_counted;
use std::{
    any::Any,
    fmt::Display,
    future::Future,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use tripwire::Tripwire;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestartReason {
    Completed,
    Failed,
    Panicked,
}

impl RestartReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Panicked => "panicked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestartDirective {
    Restart,
    Stop,
    Escalate,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RestartPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
    reset_after: Duration,
}

impl RestartPolicy {
    pub(super) fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        reset_after: Duration,
    ) -> Self {
        assert!(
            initial_backoff <= max_backoff,
            "initial restart backoff must not exceed maximum backoff"
        );

        Self {
            initial_backoff,
            max_backoff,
            reset_after,
        }
    }
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(250),
            Duration::from_secs(30),
            Duration::from_secs(60),
        )
    }
}

pub(super) trait SupervisedActor: Send + 'static {
    type Error: Display + Send + Sync + 'static;

    fn name(&self) -> &'static str;

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn restart_directive(&mut self, _reason: RestartReason) -> RestartDirective {
        RestartDirective::Restart
    }

    fn on_restart(&mut self, _reason: RestartReason) {}

    fn on_escalate(&mut self, _reason: RestartReason) {}
}

pub(super) fn spawn_supervised<A>(
    mut actor: A,
    tripwire: Tripwire,
    policy: RestartPolicy,
) -> JoinHandle<()>
where
    A: SupervisedActor,
{
    let name = actor.name();

    spawn_counted(async move {
        let mut restart_backoff = policy.initial_backoff;

        loop {
            if tripwire.is_shutting_down() {
                break;
            }

            let started_at = Instant::now();

            let outcome = AssertUnwindSafe(actor.run(tripwire.clone()))
                .catch_unwind()
                .await;

            if tripwire.is_shutting_down() {
                info!(actor = name, "actor stopped during shutdown");
                break;
            }

            let reason = match outcome {
                Ok(Ok(())) => {
                    warn!(actor = name, "actor exited unexpectedly");
                    RestartReason::Completed
                }
                Ok(Err(e)) => {
                    error!(actor = name, error = %e, "actor failed");
                    RestartReason::Failed
                }
                Err(payload) => {
                    error!(
                        actor = name,
                        panic = panic_message(payload.as_ref()),
                        "actor panicked"
                    );
                    RestartReason::Panicked
                }
            };

            match actor.restart_directive(reason) {
                RestartDirective::Restart => {}

                RestartDirective::Stop => {
                    counter!(
                        "corro.runtime.actor.stop.total",
                        "actor" => name,
                        "reason" => reason.as_str(),
                    )
                    .increment(1);

                    warn!(
                        actor = name,
                        reason = reason.as_str(),
                        "actor stopped without restart"
                    );
                    break;
                }

                RestartDirective::Escalate => {
                    actor.on_escalate(reason);

                    counter!(
                        "corro.runtime.actor.escalate.total",
                        "actor" => name,
                        "reason" => reason.as_str(),
                    )
                    .increment(1);

                    error!(
                        actor = name,
                        reason = reason.as_str(),
                        "actor failure escalated to node shutdown"
                    );
                    break;
                }
            }

            actor.on_restart(reason);

            counter!(
                "corro.runtime.actor.restart.total",
                "actor" => name,
                "reason" => reason.as_str(),
            )
            .increment(1);

            let (restart_delay, next_backoff) =
                restart_schedule(restart_backoff, policy, started_at.elapsed());

            warn!(
                actor = name,
                reason = reason.as_str(),
                ?restart_delay,
                "restarting actor"
            );

            let mut restart_tripwire = tripwire.clone();

            tokio::select! {
                _ = tokio::time::sleep(restart_delay) => {}
                _ = &mut restart_tripwire => {
                    break;
                }
            }

            restart_backoff = next_backoff;
        }

        info!(actor = name, "actor supervisor stopped");
    })
}

fn restart_schedule(
    current_backoff: Duration,
    policy: RestartPolicy,
    run_duration: Duration,
) -> (Duration, Duration) {
    let delay = if run_duration >= policy.reset_after {
        policy.initial_backoff
    } else {
        current_backoff
    };

    let next_backoff = delay.saturating_mul(2).min(policy.max_backoff);

    (delay, next_backoff)
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    }
}

mod actors;

pub(super) use actors::{
    AgentMetricsActor, GossipToSendActor, NotificationsActor, QueryMetricsActor, SyncActor,
};

#[cfg(test)]
mod tests;
