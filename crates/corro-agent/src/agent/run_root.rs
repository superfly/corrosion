//! Start the root agent tasks

use std::{ops::RangeInclusive, time::Instant};

use crate::{
    agent::{
        handlers::{self, spawn_handle_db_cleanup},
        metrics, setup, util, AgentOptions,
    },
    broadcast::runtime_loop,
};
use corro_types::{
    actor::ActorId,
    agent::{Agent, BookedVersions, Bookie},
    base::{CrsqlSeq, Version},
    channel::bounded,
    config::{Config, PerfConfig},
};

use futures::{FutureExt, StreamExt, TryStreamExt};
use rangemap::RangeInclusiveSet;
use spawn::spawn_counted;
use tracing::{error, info};
use tripwire::Tripwire;

/// Start a new agent with an existing configuration
///
/// First initialise `AgentOptions` state via `setup()`, then spawn a
/// new task that runs the main agent state machine
pub async fn start_with_config(conf: Config, tripwire: Tripwire) -> eyre::Result<(Agent, Bookie)> {
    let (agent, opts) = setup(conf.clone(), tripwire.clone()).await?;

    let bookie = run(agent.clone(), opts, conf.perf).await?;

    Ok((agent, bookie))
}

async fn run(agent: Agent, opts: AgentOptions, pconf: PerfConfig) -> eyre::Result<Bookie> {
    let AgentOptions {
        gossip_server_endpoint,
        transport,
        api_listeners,
        tripwire,
        lock_registry,
        rx_bcast,
        rx_apply,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        subs_manager,
        subs_bcast_cache,
        rtt_rx,
    } = opts;

    // Get our gossip address and make sure it's valid
    let gossip_addr = gossip_server_endpoint.local_addr()?;

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

    let (to_send_tx, to_send_rx) = bounded(pconf.to_send_channel_len, "to_send");
    let (notifications_tx, notifications_rx) =
        bounded(pconf.notifications_channel_len, "notifications");

    //// Start the main SWIM runtime loop
    runtime_loop(
        // here the agent already has the current cluster_id, we don't need to pass one
        agent.actor(None),
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
        api_listeners,
    )
    .await?;

    // spawn_counted(util::write_empties_loop(
    //     agent.clone(),
    //     rx_empty,
    //     tripwire.clone(),
    // ));

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

    spawn_handle_db_cleanup(agent.pool().clone());

    let bookie = Bookie::new_with_registry(Default::default(), lock_registry);
    {
        let mut w = bookie.write("init").await;
        w.insert(agent.actor_id(), agent.booked().clone());
    }

    let start = Instant::now();
    {
        let conn = agent.pool().read().await?;
        let actor_ids: Vec<ActorId> = conn
            .prepare("SELECT site_id FROM crsql_site_id WHERE ordinal > 0")?
            .query_map([], |row| row.get(0))
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        // not strictly required, but we don't need to keep it open
        drop(conn);

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
                            let mut conn = pool.write_normal().await?;

                            tokio::task::block_in_place(|| {
                                BookedVersions::from_conn(&mut conn, actor_id)
                                    .map(|bv| (actor_id, bv))
                                    .map_err(eyre::Report::from)
                            })
                        })
                        .await?
                    }
                }),
        )
        .buffer_unordered(4);

        while let Some((actor_id, bv)) = TryStreamExt::try_next(&mut buf).await? {
            for (version, partial) in bv.partials.iter() {
                let gaps_count = partial.seqs.gaps(&(CrsqlSeq(0)..=partial.last_seq)).count();

                if gaps_count == 0 {
                    info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
                    let tx_apply = agent.tx_apply().clone();
                    let version = *version;
                    tokio::spawn(async move {
                        if let Err(e) = tx_apply.send((actor_id, version)).await {
                            error!("could not schedule buffered changes application: {e}");
                        }
                    });
                }
            }

            // {
            //     let mut conn = pool.write_normal().await?;
            //     rusqlite::vtab::series::load_module(&conn)?;

            //     let mut snap = bv.snapshot();

            //     let tx = conn.transaction()?;
            //     for range in bv.needed().iter() {
            //         let versions = tx
            //             .prepare_cached(
            //                 "
            //             SELECT distinct start_version, coalesce(end_version, start_version)
            //                 from __corro_bookkeeping, generate_series(?,?,1)
            //                 where actor_id = ? and
            //                 value between start_version and coalesce(end_version, start_version)
            //         ",
            //             )?
            //             .query_map(
            //                 rusqlite::params![range.start(), range.end(), actor_id],
            //                 |row| Ok(row.get(0)?..=row.get(1)?),
            //             )?
            //             .collect::<rusqlite::Result<RangeInclusiveSet<Version>>>()?;

            //         snap = snap.insert_db(&tx, versions)?;
            //     }

            //     tx.commit()?;

            //     bv.apply_snapshot(snap);
            // }

            bookie
                .write("replace_actor")
                .await
                .replace_actor(actor_id, bv);
        }
    }

    info!("Bookkeeping fully loaded in {:?}", start.elapsed());

    // tokio::spawn({
    //     let pool = agent.pool().clone();
    //     let bookie = bookie.clone();
    //     async move {
    //         let actor_ids: Vec<_> = {
    //             let r = bookie.read("cleanup gaps").await;
    //             r.keys().copied().collect()
    //         };

    //         for actor_id in actor_ids {
    //             let mut conn = pool.write_low().await?;
    //             let booked = {
    //                 bookie
    //                     .read("cleanup gaps per actor")
    //                     .await
    //                     .get(&actor_id)
    //                     .cloned()
    //                     .unwrap()
    //             };
    //             let mut w = booked.write("cleanup gaps booked write").await;

    //             let mut snap = w.snapshot();

    //             let tx = conn.transaction()?;
    //             for range in w.needed().iter() {
    //                 let versions = tx
    //                     .prepare_cached(
    //                         "
    //                     SELECT distinct start_version, coalesce(end_version)
    //                         from __corro_bookkeeping, generate_series(?,?,1)
    //                         where actor_id = ? and
    //                         value between start_version and coalesce(end_version, start_version)
    //                 ",
    //                     )?
    //                     .query_map(
    //                         rusqlite::params![range.start(), range.end(), actor_id],
    //                         |row| Ok(row.get(0)?..=row.get(1)?),
    //                     )?
    //                     .collect::<rusqlite::Result<RangeInclusiveSet<Version>>>()?;

    //                 snap = snap.insert_db(&tx, versions)?;
    //             }

    //             tx.commit()?;

    //             w.apply_snapshot(snap);
    //         }

    //         Ok::<_, eyre::Report>(())
    //     }
    // });

    spawn_counted(
        util::sync_loop(
            agent.clone(),
            bookie.clone(),
            transport.clone(),
            tripwire.clone(),
        )
        .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    spawn_counted(util::apply_fully_buffered_changes_loop(
        agent.clone(),
        bookie.clone(),
        rx_apply,
    ));

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    //// Start an incoming (corrosion) connection handler.  This
    //// future tree spawns additional message type sub-handlers
    handlers::spawn_gossipserver_handler(&agent, &bookie, &tripwire, gossip_server_endpoint);

    spawn_counted(handlers::handle_changes(
        agent.clone(),
        bookie.clone(),
        rx_changes,
        tripwire.clone(),
    ));

    if let Some(secs) = agent.config().db.clear_overwritten_secs {
        tokio::spawn(util::clear_overwritten_versions_loop(
            agent.clone(),
            bookie.clone(),
            secs,
        ));
    }

    Ok(bookie)
}
