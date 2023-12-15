//! Start the root agent tasks
//!
//! 1. foo
//! 2. bar
//! 3. spawn `super::other_module`

use std::{sync::Arc, time::Duration};

use crate::{
    agent::{handlers, metrics, setup, uni, util, AgentOptions},
    api::public::pubsub::{
        process_sub_channel, MatcherBroadcastCache, SharedMatcherBroadcastCache,
    },
    broadcast::runtime_loop,
};
use corro_types::{
    actor::Actor,
    agent::Agent,
    broadcast::BroadcastV1,
    config::Config,
    pubsub::{Matcher, SubsManager},
};

use futures::FutureExt;
use spawn::spawn_counted;
use tokio::sync::{mpsc::channel, RwLock as TokioRwLock};
use tracing::{debug, error, info};
use tripwire::{PreemptibleFutureExt, Tripwire};

/// Start a new agent with an existing configuration
///
/// First initialise `AgentOptions` state via `setup()`, then spawn a
/// new task that runs the main agent state machine
pub async fn start_with_config(conf: Config, tripwire: Tripwire) -> eyre::Result<Agent> {
    let (agent, opts) = setup(conf, tripwire.clone()).await?;

    tokio::spawn({
        let agent = agent.clone();
        async move {
            match run(agent, opts).await {
                Ok(_) => info!("corrosion agent run is done"),
                Err(e) => error!("running corrosion agent failed: {e}"),
            }
        }
    });

    Ok(agent)
}

async fn run(agent: Agent, opts: AgentOptions) -> eyre::Result<()> {
    let AgentOptions {
        actor_id,
        gossip_server_endpoint,
        transport,
        api_listener,
        mut tripwire,
        rx_bcast,
        rx_apply,
        rx_empty,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        subs_manager,
        rtt_rx,
    } = opts;

    let subs_bcast_cache = setup_spawn_subscriptions(&agent, &subs_manager, &tripwire).await;

    //// Start PG server to accept query requests from PG clients
    // TODO: pull this out into a separate function
    if let Some(pg_conf) = agent.config().api.pg.clone() {
        info!("Starting PostgreSQL wire-compatible server");
        let pg_server = corro_pg::start(agent.clone(), pg_conf, tripwire.clone()).await?;
        info!(
            "Started PostgreSQL wire-compatible server, listening at {}",
            pg_server.local_addr
        );
    }

    //// TODO: turn channels into a named and counted resource
    let (to_send_tx, to_send_rx) = channel(10240);
    let (notifications_tx, notifications_rx) = channel(10240);
    let (bcast_msg_tx, bcast_rx) = channel::<BroadcastV1>(10240);
    let (process_uni_tx, mut process_uni_rx) = channel(10240);

    //// DOCME: why here and why now?
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    //// Start the main SWIM runtime loop
    runtime_loop(
        Actor::new(
            actor_id,
            agent.external_addr().unwrap_or_else(|| agent.gossip_addr()),
            agent.clock().new_timestamp().into(),
        ),
        agent.clone(),
        transport.clone(),
        rx_foca,
        rx_bcast,
        to_send_tx,
        notifications_tx,
        tripwire.clone(),
    );

    //// Start async uni-broadcast message decoder
    bcast::spawn_message_decoder(&agent, process_uni_rx, bcast_msg_tx);

    // TODO: start big_boi::spawn_lots_of_things

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    Ok(())
}

/// Initialise subscription state and tasks
///
/// 1. Get subscriptions state directory from config
/// 2. Load existing subscriptions and restore them in SubsManager
/// 3. Spawn subscription processor task
async fn setup_spawn_subscriptions(
    agent: &Agent,
    subs_manager: &SubsManager,
    tripwire: &Tripwire,
) -> eyre::Result<SharedMatcherBroadcastCache> {
    let mut subs_bcast_cache = MatcherBroadcastCache::default();
    let mut to_cleanup = vec![];

    let subs_path = agent.config().db.subscriptions_path();

    if let Ok(mut dir) = tokio::fs::read_dir(&subs_path).await {
        while let Ok(Some(entry)) = dir.next_entry().await {
            let path_str = entry.path().display().to_string();
            if let Some(sub_id_str) = path_str.strip_prefix(subs_path.as_str()) {
                if let Ok(sub_id) = sub_id_str.trim_matches('/').parse() {
                    let (_, created) = match subs_manager.restore(
                        sub_id,
                        &subs_path,
                        &agent.schema().read(),
                        agent.pool(),
                        tripwire.clone(),
                    ) {
                        Ok(res) => res,
                        Err(e) => {
                            error!(%sub_id, "could not restore subscription: {e}");
                            to_cleanup.push(sub_id);
                            continue;
                        }
                    };

                    info!(%sub_id, "Restored subscription");

                    let (sub_tx, _) = tokio::sync::broadcast::channel(10240);

                    tokio::spawn(process_sub_channel(
                        subs_manager.clone(),
                        sub_id,
                        sub_tx.clone(),
                        created.evt_rx,
                    ));

                    subs_bcast_cache.insert(sub_id, sub_tx);
                }
            }
        }
    }

    for id in to_cleanup {
        info!(sub_id = %id, "Cleaning up unclean subscription");
        Matcher::cleanup(id, Matcher::sub_path(subs_path.as_path(), id))?;
    }

    Ok(Arc::new(TokioRwLock::new(subs_bcast_cache)))
}
