//! Start the root agent tasks

use std::sync::Arc;

use crate::{
    agent::{
        handlers::{self, spawn_handle_db_cleanup},
        metrics, setup, uni, util, AgentOptions,
    },
    api::public::pubsub::{
        process_sub_channel, MatcherBroadcastCache, SharedMatcherBroadcastCache,
    },
    broadcast::runtime_loop,
};
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, BookedVersions, Bookie},
    base::CrsqlSeq,
    broadcast::BroadcastV1,
    config::Config,
    pubsub::{Matcher, SubsManager},
};

use futures::{FutureExt, StreamExt, TryStreamExt};
use spawn::spawn_counted;
use tokio::{
    sync::{mpsc::channel, RwLock as TokioRwLock},
    task::block_in_place,
};
use tracing::{error, info};
use tripwire::Tripwire;

/// Start a new agent with an existing configuration
///
/// First initialise `AgentOptions` state via `setup()`, then spawn a
/// new task that runs the main agent state machine
pub async fn start_with_config(conf: Config, tripwire: Tripwire) -> eyre::Result<(Agent, Bookie)> {
    let (agent, opts) = setup(conf, tripwire.clone()).await?;

    let bookie = run(agent.clone(), opts).await?;

    Ok((agent, bookie))
}

async fn run(agent: Agent, opts: AgentOptions) -> eyre::Result<Bookie> {
    let AgentOptions {
        actor_id,
        gossip_server_endpoint,
        transport,
        api_listener,
        tripwire,
        lock_registry,
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
    let (bcast_msg_tx, bcast_msg_rx) = channel::<BroadcastV1>(10240);
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

    handlers::spawn_swim_announcer(&agent, gossip_addr);

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

    spawn_counted(util::write_empties_loop(
        agent.clone(),
        rx_empty,
        tripwire.clone(),
    ));

    tokio::spawn(util::clear_buffered_meta_loop(agent.clone(), rx_clear_buf));

    tokio::spawn(metrics::metrics_loop(agent.clone(), transport.clone()));
    tokio::spawn(handlers::handle_gossip_to_send(
        transport.clone(),
        to_send_rx,
    ));
    tokio::spawn(handlers::handle_notifications(
        agent.clone(),
        notifications_rx,
    ));
    tokio::spawn(handlers::handle_broadcasts(agent.clone(), bcast_msg_rx));

    spawn_handle_db_cleanup(agent.pool().clone());

    let bookie = Bookie::new_with_registry(Default::default(), lock_registry);
    {
        let mut w = bookie.write("init").await;
        w.insert(agent.actor_id(), agent.booked().clone());
    }
    {
        let conn = agent.pool().read().await?;
        let actor_ids: Vec<ActorId> = conn
            .prepare("SELECT DISTINCT actor_id FROM __corro_bookkeeping")?
            .query_map([], |row| row.get(0))
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        let pool = agent.pool();

        let mut buf = futures::stream::iter(
            actor_ids
                .into_iter()
                // don't re-process the current actor!
                .filter(|other_actor_id| *other_actor_id != agent.actor_id())
                .map(|actor_id| {
                    let pool = pool.clone();
                    async move {
                        tokio::spawn(async move {
                            let conn = pool.read().await?;

                            block_in_place(|| BookedVersions::from_conn(&conn, actor_id))
                                .map(|bv| (actor_id, bv))
                                .map_err(eyre::Report::from)
                        })
                        .await?
                    }
                }),
        )
        .buffer_unordered(8);

        while let Some((actor_id, bv)) = TryStreamExt::try_next(&mut buf).await? {
            for (version, partial) in bv.partials.iter() {
                let gaps_count = partial.seqs.gaps(&(CrsqlSeq(0)..=partial.last_seq)).count();

                if gaps_count == 0 {
                    info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
                    let _ = agent.tx_apply().send((actor_id, *version)).await;
                }
            }

            bookie
                .write("replace_actor")
                .await
                .replace_actor(actor_id, bv);
        }
    }

    spawn_counted(
        util::sync_loop(
            agent.clone(),
            bookie.clone(),
            transport.clone(),
            rx_apply,
            tripwire.clone(),
        )
        .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    //// Start an incoming (corrosion) connection handler.  This
    //// future tree spawns additional message type sub-handlers
    handlers::spawn_gossipserver_handler(
        &agent,
        &bookie,
        &tripwire,
        gossip_server_endpoint,
        process_uni_tx,
    );

    //// Start async uni-broadcast message decoder
    uni::spawn_unipayload_message_decoder(&agent, &bookie, process_uni_rx, bcast_msg_tx);

    spawn_counted(handlers::handle_changes(
        agent.clone(),
        bookie.clone(),
        rx_changes,
        tripwire.clone(),
    ));

    tokio::spawn(util::clear_overwritten_versions(
        agent.clone(),
        bookie.clone(),
    ));

    Ok(bookie)
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
