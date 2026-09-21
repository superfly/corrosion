//! Start the root agent tasks

use std::time::Instant;

use super::supervisor::{
    spawn_supervised, AgentMetricsActor, GossipToSendActor, NotificationsActor, QueryMetricsActor,
    RestartPolicy, SyncActor,
};

use crate::agent::util::execute_schema_from_paths;
use crate::{
    agent::{handlers, reaper::spawn_reaper, setup, util, AgentOptions},
    broadcast::{runtime_loop, BroadcastOpts, GossipBroadcastActor},
    transport::Transport,
};

use corro_types::{
    agent::{Agent, ApplyTrigger, Bookie},
    base::CrsqlSeq,
    channel::bounded,
    config::{BroadcastMethod, Config, PerfConfig},
};

use tokio::task::JoinHandle;
use tracing::{error, info};
use tripwire::Tripwire;

/// Start a new agent with an existing configuration
///
/// First initialise `AgentOptions` state via `setup()`, then spawn a
/// new task that runs the main agent state machine
pub async fn start_with_config(
    conf: Config,
    tripwire: Tripwire,
) -> eyre::Result<(Agent, Bookie, Transport, Vec<JoinHandle<()>>)> {
    let (agent, opts) = setup(conf.clone(), tripwire.clone()).await?;
    let transport = opts.transport.clone();

    let (bookie, handles) = run(agent.clone(), opts, conf.perf).await?;

    Ok((agent, bookie, transport, handles))
}

async fn run(
    agent: Agent,
    opts: AgentOptions,
    pconf: PerfConfig,
) -> eyre::Result<(Bookie, Vec<JoinHandle<()>>)> {
    let AgentOptions {
        gossip_server_endpoint,
        transport,
        api_listeners,
        mut tripwire,
        rx_bcast,
        rx_apply,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        rx_plumtree,
        rx_plumtree_updates,
        subs_manager,
        subs_bcast_cache,
        updates_bcast_cache,
        rtt_rx,
    } = opts;

    // Get our gossip address and make sure it's valid
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    //// Start PG server to accept query requests from PG clients
    // TODO: pull this out into a separate function?
    if let Some(pg_confs) = agent.config().api.pg.clone() {
        info!("Starting PostgreSQL wire-compatible server");
        for pg_conf in pg_confs {
            let pg_server = corro_pg::start(agent.clone(), pg_conf, tripwire.clone()).await?;
            info!(
                "Started PostgreSQL wire-compatible server, listening at {}",
                pg_server.local_addr
            );
        }
    }

    let (to_send_tx, to_send_rx) = bounded(pconf.to_send_channel_len, "to_send");
    let (notifications_tx, notifications_rx) =
        bounded(pconf.notifications_channel_len, "notifications");

    let loaded_member_states = util::load_member_states(&agent).await;
    let member_states: Vec<_> = loaded_member_states
        .iter()
        .map(|(address, member, _)| (*address, member.clone()))
        .collect();

    //// Start the main SWIM runtime loop
    let foca_config = runtime_loop(
        // here the agent already has the current cluster_id, we don't need to pass one
        agent.actor(None, agent.config().gossip.member_id),
        agent.clone(),
        rx_foca,
        to_send_tx,
        notifications_tx,
        tripwire.clone(),
        member_states,
    );

    //// Update member connection RTTs
    handlers::spawn_rtt_sampler(&transport, tripwire.clone());
    handlers::spawn_rtt_handler(&agent, rtt_rx, tripwire.clone());

    handlers::spawn_swim_announcer(&agent, gossip_addr, tripwire.clone());

    // Load existing cluster members into the SWIM runtime
    util::initialise_foca(&agent, loaded_member_states).await;

    match agent.broadcast_method() {
        BroadcastMethod::Gossip => spawn_supervised(
            &agent,
            GossipBroadcastActor::new(
                agent.clone(),
                rx_bcast,
                transport.clone(),
                foca_config,
                BroadcastOpts::default(),
            ),
            tripwire.clone(),
            RestartPolicy::default(),
        ),
        BroadcastMethod::Plumtree => spawn_supervised(
            &agent,
            crate::plumtree::PlumtreeActor::new(
                agent.clone(),
                transport.clone(),
                rx_plumtree,
                rx_plumtree_updates,
                agent.tx_changes().clone(),
            ),
            tripwire.clone(),
            RestartPolicy::default(),
        ),
    };

    // Load schema from paths
    if let Err(e) = execute_schema_from_paths(&agent).await {
        error!("could not execute schema: {e}");
    }

    let mut handles = vec![];
    // Setup client http API
    let mut http_handles = util::setup_http_api_handler(
        &agent,
        &mut tripwire,
        subs_bcast_cache,
        updates_bcast_cache,
        &subs_manager,
        api_listeners,
    )
    .await?;
    handles.append(&mut http_handles);

    spawn_supervised(
        &agent,
        util::ClearBufferedMetaActor::new(agent.clone(), rx_clear_buf),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    spawn_supervised(
        &agent,
        AgentMetricsActor::new(agent.clone(), transport.clone()),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    spawn_supervised(
        &agent,
        QueryMetricsActor::new(),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    spawn_supervised(
        &agent,
        GossipToSendActor::new(transport.clone(), to_send_rx),
        tripwire.clone(),
        RestartPolicy::default(),
    );
    spawn_supervised(
        &agent,
        NotificationsActor::new(agent.clone(), notifications_rx),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    spawn_supervised(
        &agent,
        handlers::DbMaintenanceActor::new(&agent),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    let bookie = agent.bookie().clone();

    // Bookie was fully loaded by setup(). Walk it to schedule apply for any
    // fully-buffered (gap-free) partials that were never applied before shutdown.
    let start = Instant::now();
    {
        let guard = bookie.owned_guard();
        for (&actor_id, booked) in bookie.iter(&guard) {
            let bookedr = booked.read();
            for (version, partial) in bookedr.partials.iter() {
                let gaps_count = partial.seqs.gaps(&(CrsqlSeq(0)..=partial.last_seq)).count();
                if gaps_count == 0 {
                    info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
                    let tx_apply = agent.tx_apply().clone();
                    let version = *version;
                    tokio::spawn(async move {
                        if let Err(e) = tx_apply
                            .send(ApplyTrigger::Version(actor_id, version))
                            .await
                        {
                            error!("could not schedule buffered changes application: {e}");
                        }
                    });
                }
            }
        }
    }
    info!("Checked bookie partials in {:?}", start.elapsed());

    spawn_supervised(
        &agent,
        SyncActor::new(agent.clone(), bookie.clone(), transport.clone()),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    spawn_supervised(
        &agent,
        util::ApplyBufferedActor::new(agent.clone(), bookie.clone(), rx_apply),
        tripwire.clone(),
        RestartPolicy::default(),
    );

    if let Err(e) = spawn_reaper(&agent, tripwire.clone()) {
        error!("could not spawn reaper: {e}");
    }

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    //// Start an incoming (corrosion) connection handler.  This
    //// future tree spawns additional message type sub-handlers
    handlers::spawn_gossipserver_handler(&agent, &bookie, &tripwire, gossip_server_endpoint);

    let changes_handle = spawn_supervised(
        &agent,
        handlers::ChangesActor::new(agent.clone(), bookie.clone(), rx_changes),
        tripwire.clone(),
        RestartPolicy::default(),
    );
    handles.push(changes_handle);

    Ok((bookie, handles))
}
