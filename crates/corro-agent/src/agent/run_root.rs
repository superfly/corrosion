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

    // Get our gossip address and make sure it's valid
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    // Setup subscription handlers
    let subs_bcast_cache = setup_spawn_subscriptions(&agent, &subs_manager, &tripwire).await?;

    //// Start PG server to accept query requests from PG clients
    // TODO: pull this out into a separate function?
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
    let (process_uni_tx, process_uni_rx) = channel(10240);

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

    //// Update member connection RTTs
    handlers::spawn_rtt_handler(&agent, rtt_rx);

    //// Start async uni-broadcast message decoder
    uni::spawn_unipayload_message_decoder(&agent, process_uni_rx, bcast_msg_tx);

    //// Start an incoming (corrosion) connection handler.  This
    //// future tree spawns additional message type sub-handlers
    handlers::spawn_gossipserver_handler(&agent, &tripwire, gossip_server_endpoint, process_uni_tx);

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    handlers::spawn_backoff_handler(&agent, gossip_addr);

    tokio::spawn(util::clear_overwritten_versions(agent.clone()));

    // Load existing cluster members into the SWIM runtime
    util::initialise_foca(&agent).await;

    // Setup client http API
    util::setup_http_api_handler(
        &agent,
        &tripwire,
        subs_bcast_cache,
        &subs_manager,
        api_listener,
    )
    .await?;

    spawn_counted(handlers::handle_changes(
        agent.clone(),
        rx_changes,
        tripwire.clone(),
    ));

    spawn_counted(util::write_empties_loop(
        agent.clone(),
        rx_empty,
        tripwire.clone(),
    ));

    tokio::spawn(util::clear_buffered_meta_loop(agent.clone(), rx_clear_buf));

    spawn_counted(
        util::sync_loop(agent.clone(), transport.clone(), rx_apply, tripwire.clone())
            .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    tokio::spawn(metrics::metrics_loop(agent.clone(), transport.clone()));
    tokio::spawn(handlers::handle_gossip_to_send(
        transport.clone(),
        to_send_rx,
    ));
    tokio::spawn(handlers::handle_notifications(
        agent.clone(),
        notifications_rx,
    ));
    tokio::spawn(handlers::handle_broadcasts(agent.clone(), bcast_rx));

    let mut db_cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 15));

    loop {
        tokio::select! {
            biased;
            _ = db_cleanup_interval.tick() => {
                tokio::spawn(handlers::handle_db_cleanup(agent.pool().clone()).preemptible(tripwire.clone()));
            },
            _ = &mut tripwire => {
                debug!("tripped corrosion");
                break;
            }
        }
    }

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
