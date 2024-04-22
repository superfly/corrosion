//! Agent utility functions that don't have an obvious place in
//! another module
//!
//! Any set of functions that can create a coherent module (i.e. have
//! a similar API facade, handle the same kinds of data, etc) should
//! be pulled out of this file in future.

use crate::{
    agent::{handlers, CountedExecutor, MAX_SYNC_BACKOFF, TO_CLEAR_COUNT},
    api::public::{
        api_v1_db_schema, api_v1_queries, api_v1_table_stats, api_v1_transactions,
        pubsub::{api_v1_sub_by_id, api_v1_subs},
    },
    transport::Transport,
};
use corro_types::{
    actor::{Actor, ActorId},
    agent::{
        Agent, Bookie, ChangeError, CurrentVersion, KnownDbVersion, PartialVersion, SplitPool,
    },
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{ChangeSource, ChangeV1, Changeset, ChangesetParts, FocaCmd, FocaInput},
    channel::CorroReceiver,
    config::AuthzConfig,
    pubsub::SubsManager,
};
use std::{
    cmp,
    collections::{BTreeMap, BTreeSet, HashSet},
    convert::Infallible,
    net::SocketAddr,
    ops::RangeInclusive,
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};

use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    headers::{authorization::Bearer, Authorization},
    routing::{get, post},
    BoxError, Extension, Router, TypedHeader,
};
use foca::Member;
use futures::FutureExt;
use hyper::{server::conn::AddrIncoming, StatusCode};
use metrics::{counter, histogram};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{
    named_params, params, params_from_iter, Connection, OptionalExtension, ToSql, Transaction,
};
use spawn::spawn_counted;
use tokio::{net::TcpListener, sync::mpsc::Sender, task::block_in_place, time::sleep};
use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace, warn};
use tripwire::{PreemptibleFutureExt, Tripwire};

use super::BcastCache;

pub async fn initialise_foca(agent: &Agent) {
    let states = load_member_states(agent).await;
    if !states.is_empty() {
        let mut foca_states = BTreeMap::<SocketAddr, Member<Actor>>::new();

        {
            // block to drop the members write lock
            let mut members = agent.members().write();
            for (address, foca_state) in states {
                members.by_addr.insert(address, foca_state.id().id());
                if matches!(foca_state.state(), foca::State::Suspect) {
                    continue;
                }

                // Add to the foca_states if the member doesn't yet
                // exist in the map, or if we are replacing an older
                // timestamp
                match foca_states.get(&address) {
                    Some(state) if state.id().ts() < foca_state.id().ts() => {
                        foca_states.insert(address, foca_state);
                    }
                    None => {
                        foca_states.insert(address, foca_state);
                    }
                    _ => {}
                }
            }
        }

        if let Err(e) = agent
            .tx_foca()
            .send(FocaInput::ApplyMany(foca_states.into_values().collect()))
            .await
        {
            error!("Failed to queue initial foca state: {e:?}, cluster membership states will be broken!");
        }

        let agent = agent.clone();
        tokio::task::spawn(async move {
            // Add some random scatter to the task sleep so that
            // restarted nodes don't all rejoin at once
            let scatter = rand::random::<u64>() % 15;
            tokio::time::sleep(Duration::from_secs(25 + scatter)).await;

            async fn apply_rejoin(agent: &Agent) -> eyre::Result<()> {
                let (cb_tx, cb_rx) = tokio::sync::oneshot::channel();
                agent
                    .tx_foca()
                    .send(FocaInput::Cmd(FocaCmd::Rejoin(cb_tx)))
                    .await?;
                cb_rx.await??;
                Ok(())
            }

            if let Err(e) = apply_rejoin(&agent).await {
                error!("failed to execute cluster rejoin: {e:?}");
            }
        });
    } else {
        warn!("No existing cluster member state to load!  This seems sus");
    }
}

pub async fn clear_overwritten_versions_loop(agent: Agent, bookie: Bookie, sleep_in_secs: u64) {
    let pool = agent.pool();
    let sleep_duration = Duration::from_secs(sleep_in_secs);

    loop {
        sleep(sleep_duration).await;
        info!("Starting compaction...");

        if clear_overwritten_versions(&agent, &bookie, pool, None)
            .await
            .is_err()
        {
            continue;
        }
    }
}

/// Prune the database
pub async fn clear_overwritten_versions(
    _agent: &Agent,
    _bookie: &Bookie,
    _pool: &SplitPool,
    _feedback: Option<Sender<String>>,
) -> Result<(), String> {
    // let start = Instant::now();

    // let bookie_clone = {
    //     bookie
    //         .read("gather_booked_for_compaction")
    //         .await
    //         .iter()
    //         .map(|(actor_id, booked)| (*actor_id, booked.clone()))
    //         .collect::<HashMap<ActorId, _>>()
    // };

    // let mut inserted = 0;
    // let mut deleted = 0;

    // let mut db_elapsed = Duration::new(0, 0);

    // if let Some(ref tx) = feedback {
    //     tx.send(format!(
    //         "Compacting changes for {} actors",
    //         bookie_clone.len()
    //     ))
    //     .await
    //     .map_err(|e| format!("{e}"))?;
    // }

    // for (actor_id, booked) in bookie_clone {
    //     if let Some(ref tx) = feedback {
    //         tx.send(format!("Starting change compaction for {actor_id}"))
    //             .await
    //             .map_err(|e| format!("{e}"))?;
    //     }

    //     // pull the current db version -> version map at the present time
    //     // these are only updated _after_ a transaction has been committed, via a write lock
    //     // so it should be representative of the current state.
    //     let mut versions = {
    //         match timeout(
    //             Duration::from_secs(1),
    //             booked.read(format!(
    //                 "clear_overwritten_versions:{}",
    //                 actor_id.as_simple()
    //             )),
    //         )
    //         .await
    //         {
    //             Ok(booked) => booked.current_versions(),
    //             Err(_) => {
    //                 info!(%actor_id, "timed out acquiring read lock on bookkeeping, skipping for now");

    //                 if let Some(ref tx) = feedback {
    //                     tx.send("timed out acquiring read lock on bookkeeping".into())
    //                         .await
    //                         .map_err(|e| format!("{e}"))?;
    //                 }

    //                 return Err("Timed out acquiring read lock on bookkeeping".into());
    //             }
    //         }
    //     };

    //     if versions.is_empty() {
    //         if let Some(ref tx) = feedback {
    //             tx.send("No versions to compact".into())
    //                 .await
    //                 .map_err(|e| format!("{e}"))?;
    //         }
    //         continue;
    //     }

    //     // we're using a read connection here, starting a read-only transaction
    //     // this should be representative of the state of current versions from the actor
    //     let cleared_versions = match pool.read().await {
    //         Ok(mut conn) => {
    //             let start = Instant::now();
    //             let res = block_in_place(|| {
    //                 let tx = conn.transaction()?;
    //                 find_cleared_db_versions(&tx, &actor_id)
    //             });
    //             db_elapsed += start.elapsed();
    //             match res {
    //                 Ok(cleared) => {
    //                     debug!(
    //                         actor_id = %actor_id,
    //                         "Aggregated {} DB versions to clear in {:?}",
    //                         cleared.len(),
    //                         start.elapsed()
    //                     );

    //                     if let Some(ref tx) = feedback {
    //                         tx.send(format!("Aggregated {} DB versions to clear", cleared.len()))
    //                             .await
    //                             .map_err(|e| format!("{e}"))?;
    //                     }

    //                     cleared
    //                 }
    //                 Err(e) => {
    //                     error!("could not get cleared versions: {e}");

    //                     if let Some(ref tx) = feedback {
    //                         tx.send(format!("failed to get cleared versions: {e}"))
    //                             .await
    //                             .map_err(|e| format!("{e}"))?;
    //                     }

    //                     return Err("failed to cleared versions".into());
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             error!("could not get read connection: {e}");
    //             if let Some(ref tx) = feedback {
    //                 tx.send(format!("failed to get read connection: {e}"))
    //                     .await
    //                     .map_err(|e| format!("{e}"))?;
    //             }
    //             return Err("could not get read connection".into());
    //         }
    //     };

    //     if !cleared_versions.is_empty() {
    //         let mut to_clear = Vec::new();

    //         for db_v in cleared_versions {
    //             if let Some(v) = versions.remove(&db_v) {
    //                 to_clear.push((db_v, v))
    //             }
    //         }

    //         if !to_clear.is_empty() {
    //             // use a write lock here so we can mutate the bookkept state
    //             let mut bookedw = booked
    //                 .write(format!("clearing:{}", actor_id.as_simple()))
    //                 .await;

    //             for (_db_v, v) in to_clear.iter() {
    //                 // only remove when confirming that version is still considered "current"
    //                 if bookedw.contains_current(v) {
    //                     // set it as cleared right away
    //                     bookedw.insert(*v, KnownDbVersion::Cleared);
    //                     deleted += 1;
    //                 }
    //             }

    //             // find any affected cleared ranges
    //             for range in to_clear
    //                 .iter()
    //                 .filter_map(|(_, v)| bookedw.cleared.get(v))
    //                 .dedup()
    //             {
    //                 // schedule for clearing in the background task
    //                 if let Err(e) = agent.tx_empty().send((actor_id, range.clone())).await {
    //                     error!("could not schedule version to be cleared: {e}");
    //                     if let Some(ref tx) = feedback {
    //                         tx.send(format!("failed to get queue compaction set: {e}"))
    //                             .await
    //                             .map_err(|e| format!("{e}"))?;
    //                     }
    //                 } else {
    //                     inserted += 1;
    //                 }

    //                 tokio::task::yield_now().await;
    //             }

    //             if let Some(ref tx) = feedback {
    //                 tx.send(format!("Queued {inserted} empty versions to compact"))
    //                     .await
    //                     .map_err(|e| format!("{e}"))?;
    //             }
    //         }
    //     }

    //     if let Some(ref tx) = feedback {
    //         tx.send(format!("Finshed compacting changes for {actor_id}"))
    //             .await
    //             .map_err(|e| format!("{e}"))?;
    //     }

    //     tokio::time::sleep(Duration::from_secs(1)).await;
    // }

    // info!(
    //         "Compaction done, cleared {} DB bookkeeping table rows (wall time: {:?}, db time: {db_elapsed:?})",
    //         deleted - inserted,
    //         start.elapsed()
    // );

    Ok(())
}

/// Load the existing known member state and addresses
pub async fn load_member_states(agent: &Agent) -> Vec<(SocketAddr, Member<Actor>)> {
    match agent.pool().read().await {
        Ok(conn) => block_in_place(|| {
            match conn.prepare("SELECT address,foca_state FROM __corro_members") {
                Ok(mut prepped) => {
                    match prepped
                    .query_map([], |row| Ok((
                            row.get::<_, String>(0)?.parse().map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?,
                            row.get::<_, String>(1)?
                        ))
                    )
                    .and_then(|rows| rows.collect::<rusqlite::Result<Vec<(SocketAddr, String)>>>())
                {
                    Ok(members) => {
                        members.into_iter().filter_map(|(address, state)| match serde_json::from_str::<foca::Member<Actor>>(state.as_str()) {
                            Ok(fs) => Some((address, fs)),
                            Err(e) => {
                                error!("could not deserialize foca member state: {e} (json: {state})");
                                None
                            }
                        }).collect::<Vec<(SocketAddr, Member<Actor>)>>()
                    }
                    Err(e) => {
                        error!("could not query for foca member states: {e}");
                        vec![]
                    },
                }
                }
                Err(e) => {
                    error!("could not prepare query for foca member states: {e}");
                    vec![]
                }
            }
        }),
        Err(e) => {
            error!("could not acquire conn for foca member states: {e}");
            vec![]
        }
    }
}

pub async fn setup_http_api_handler(
    agent: &Agent,
    tripwire: &Tripwire,
    subs_bcast_cache: BcastCache,
    subs_manager: &SubsManager,
    (api_listener, extra_listeners): (TcpListener, Vec<TcpListener>),
) -> eyre::Result<()> {
    let api = Router::new()
        // transactions
        .route(
            "/v1/transactions",
            post(api_v1_transactions).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(128)),
            ),
        )
        // queries
        .route(
            "/v1/queries",
            post(api_v1_queries).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(128)),
            ),
        )
        .route(
            "/v1/subscriptions",
            post(api_v1_subs).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(128)),
            ),
        )
        .route(
            "/v1/subscriptions/:id",
            get(api_v1_sub_by_id).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(128)),
            ),
        )
        .route(
            "/v1/migrations",
            post(api_v1_db_schema).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(4)),
            ),
        )
        .route(
            "/v1/table_stats",
            post(api_v1_table_stats).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(4)),
            ),
        )
        .layer(axum::middleware::from_fn(require_authz))
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(Arc::new(AtomicI64::new(0))))
                .layer(Extension(agent.clone()))
                .layer(Extension(subs_bcast_cache))
                .layer(Extension(subs_manager.clone()))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http());

    spawn_server_on_bind(api_listener, api.clone(), &tripwire)?;

    for extra_addr in extra_listeners {
        spawn_server_on_bind(extra_addr, api.clone(), &tripwire)?;
    }

    Ok(())
}

fn spawn_server_on_bind(
    api_listener: TcpListener,
    api: Router,
    tripwire: &Tripwire,
) -> eyre::Result<()> {
    let api_addr = api_listener.local_addr()?;
    info!("Starting API listener on tcp/{api_addr}");
    let mut incoming = AddrIncoming::from_listener(api_listener)?;

    incoming.set_nodelay(true);
    spawn_counted(
        axum::Server::builder(incoming)
            .executor(CountedExecutor)
            .serve(
                api.clone()
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(
                tripwire
                    .clone()
                    .inspect(move |_| info!("corrosion api http tripped {api_addr}")),
            )
            .inspect(|_| info!("corrosion api is done")),
    );

    Ok(())
}

async fn require_authz<B>(
    Extension(agent): Extension<Agent>,
    maybe_authz_header: Option<TypedHeader<Authorization<Bearer>>>,
    request: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let passed = if let Some(ref authz) = agent.config().api.authorization {
        match authz {
            AuthzConfig::BearerToken(token) => maybe_authz_header
                .map(|h| h.token() == token)
                .unwrap_or(false),
        }
    } else {
        true
    };

    if !passed {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

// DOCME: provide some context for this function
// TODO: move to a more appropriate module?
#[tracing::instrument(skip_all)]
pub fn find_cleared_db_versions(
    tx: &Transaction,
    actor_id: &ActorId,
) -> rusqlite::Result<BTreeSet<CrsqlDbVersion>> {
    let clock_site_id: Option<u64> = match tx
        .prepare_cached("SELECT ordinal FROM crsql_site_id WHERE site_id = ?")?
        .query_row([actor_id], |row| row.get(0))
        .optional()?
    {
        Some(ordinal) => Some(ordinal),
        None => {
            warn!(actor_id = %actor_id, "could not find crsql ordinal for actor");
            return Ok(Default::default());
        }
    };

    let tables = tx
        .prepare_cached(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<Result<BTreeSet<String>, _>>()?;

    if tables.is_empty() {
        // means there's no schema trakced by cr-sqlite or corrosion.
        return Ok(BTreeSet::new());
    }

    let mut params: Vec<&dyn ToSql> = vec![actor_id];
    let to_clear_query = format!(
        "SELECT DISTINCT db_version FROM __corro_bookkeeping WHERE actor_id = ? AND db_version IS NOT NULL
            EXCEPT SELECT db_version FROM ({});",
        tables
            .iter()
            .map(|table| {
                params.push(&clock_site_id);
                format!("SELECT DISTINCT db_version FROM {table} WHERE site_id = ?")
            })
            .collect::<Vec<_>>()
            .join(" UNION ")
    );

    let cleared_db_versions: BTreeSet<CrsqlDbVersion> = tx
        .prepare_cached(&to_clear_query)?
        .query_map(params_from_iter(params.into_iter()), |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

    Ok(cleared_db_versions)
}

/// Periodically initiate a sync with many other nodes.  Before we do
/// though, apply buffered/ partial changesets to avoid having to sync
/// things we should already know about.
///
/// Actual sync logic is handled by
/// [`handle_sync`](crate::agent::handlers::handle_sync).
pub async fn sync_loop(
    agent: Agent,
    bookie: Bookie,
    transport: Transport,
    mut rx_apply: CorroReceiver<(ActorId, Version)>,
    mut tripwire: Tripwire,
) {
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(Duration::from_secs(1), MAX_SYNC_BACKOFF)
        .iter();
    let next_sync_at = tokio::time::sleep(sync_backoff.next().unwrap());
    tokio::pin!(next_sync_at);

    loop {
        enum Branch {
            Tick,
            BackgroundApply { actor_id: ActorId, version: Version },
        }

        let branch = tokio::select! {
            biased;

            maybe_item = rx_apply.recv() => match maybe_item {
                Some((actor_id, version)) => Branch::BackgroundApply{actor_id, version},
                None => {
                    debug!("background applies queue is closed, breaking out of loop");
                    break;
                }
            },

            _ = &mut next_sync_at => {
                Branch::Tick
            },
            _ = &mut tripwire => {
                break;
            }
        };

        match branch {
            Branch::Tick => {
                // ignoring here, there is trying and logging going on inside
                match tokio::time::timeout(
                    Duration::from_secs(300),
                    handlers::handle_sync(&agent, &bookie, &transport),
                )
                .preemptible(&mut tripwire)
                .await
                {
                    tripwire::Outcome::Preempted(_) => {
                        warn!("aborted sync by tripwire");
                        break;
                    }
                    tripwire::Outcome::Completed(res) => match res {
                        Ok(Err(e)) => {
                            error!("could not sync: {e}");
                            // keep syncing until we successfully sync
                        }
                        Err(_e) => {
                            warn!("timed out waiting for sync to complete!");
                        }
                        Ok(Ok(_)) => {}
                    },
                }
                next_sync_at
                    .as_mut()
                    .reset(tokio::time::Instant::now() + sync_backoff.next().unwrap());
            }
            Branch::BackgroundApply { actor_id, version } => {
                debug!(%actor_id, %version, "picked up background apply of buffered changes");
                match process_fully_buffered_changes(&agent, &bookie, actor_id, version).await {
                    Ok(false) => {
                        warn!(%actor_id, %version, "did not apply buffered changes");
                    }
                    Ok(true) => {
                        debug!(%actor_id, %version, "succesfully applied buffered changes");
                    }
                    Err(e) => {
                        error!(%actor_id, %version, "could not apply fully buffered changes: {e}");
                    }
                }
            }
        }
    }
}

/// Compact the database by finding cleared versions
pub async fn clear_buffered_meta_loop(
    agent: Agent,
    mut rx_partials: CorroReceiver<(ActorId, RangeInclusive<Version>)>,
) {
    while let Some((actor_id, versions)) = rx_partials.recv().await {
        let pool = agent.pool().clone();
        let self_actor_id = agent.actor_id();
        tokio::spawn(async move {
            loop {
                let res = {
                    let mut conn = pool.write_low().await?;

                    block_in_place(|| {
                        let tx = conn.immediate_transaction()?;

                        // TODO: delete buffered changes from deleted sequences only (maybe, it's kind of hard and may not be necessary)

                        // sub query required due to DELETE and LIMIT interaction
                        let seq_count = tx
                            .prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE (site_id, version, start_seq) IN (SELECT site_id, version, start_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND version >= ? AND version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        // sub query required due to DELETE and LIMIT interaction
                        let buf_count = tx
                            .prepare_cached("DELETE FROM __corro_buffered_changes WHERE (site_id, db_version, version, seq) IN (SELECT site_id, db_version, version, seq FROM __corro_buffered_changes WHERE site_id = ? AND version >= ? AND version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        tx.commit()?;

                        Ok::<_, rusqlite::Error>((buf_count, seq_count))
                    })
                };

                match res {
                    Ok((buf_count, seq_count)) => {
                        if buf_count + seq_count > 0 {
                            info!(%actor_id, %self_actor_id, "cleared {} buffered meta rows for versions {versions:?}", buf_count + seq_count);
                        }
                        if buf_count < TO_CLEAR_COUNT && seq_count < TO_CLEAR_COUNT {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(%actor_id, "could not clear buffered meta for versions {versions:?}: {e}");
                    }
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            Ok::<_, eyre::Report>(())
        });
    }
}

#[tracing::instrument(skip_all, err)]
pub fn process_single_version(
    agent: &Agent,
    tx: &Transaction,
    last_db_version: Option<CrsqlDbVersion>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let (known, changeset) = if changeset.is_complete() {
        let (known, changeset) = process_complete_version(
            tx,
            actor_id,
            last_db_version,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
        )?;

        if check_buffered_meta_to_clear(tx, actor_id, changeset.versions())? {
            if let Err(e) = agent
                .tx_clear_buf()
                .try_send((actor_id, changeset.versions()))
            {
                error!("could not schedule buffered meta clear: {e}");
            }
        }

        (known, changeset)
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = process_incomplete_version(tx, actor_id, &parts)?;

        (known, parts.into())
    };

    Ok((known, changeset))
}

pub fn store_empty_changeset(
    conn: &Connection,
    actor_id: ActorId,
    versions: RangeInclusive<Version>,
) -> Result<usize, ChangeError> {
    // first, delete "current" versions, they're now gone!
    let deleted: Vec<RangeInclusive<Version>> = conn
        .prepare_cached(
            "
        DELETE FROM __corro_bookkeeping
            WHERE
                actor_id = :actor_id AND
                start_version >= COALESCE((
                    SELECT start_version
                        FROM __corro_bookkeeping
                        WHERE
                            actor_id = :actor_id AND
                            start_version < :start
                        ORDER BY start_version DESC
                        LIMIT 1
                ), 1)
                AND
                (
                    -- start_version is between start and end of range AND no end_version
                    ( start_version BETWEEN :start AND :end AND end_version IS NULL ) OR

                    -- start_version and end_version are within the range
                    ( start_version >= :start AND end_version <= :end ) OR

                    -- range being inserted is partially contained within another
                    ( start_version <= :end AND end_version >= :end ) OR

                    -- start_version = end + 1 (to collapse ranges)
                    ( start_version = :end + 1 AND end_version IS NOT NULL ) OR

                    -- end_version = start - 1 (to collapse ranges)
                    ( end_version = :start - 1 )
                )
            RETURNING start_version, end_version",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_map(
            named_params![
                ":actor_id": actor_id,
                ":start": versions.start(),
                ":end": versions.end(),
            ],
            |row| {
                let start = row.get(0)?;
                Ok(start..=row.get::<_, Option<Version>>(1)?.unwrap_or(start))
            },
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    // println!("deleted: {deleted:?}");

    if !deleted.is_empty() {
        debug!(
            "deleted {} still-live versions from database's bookkeeping",
            deleted.len()
        );
    }

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert(versions);

    // we should never have deleted non-contiguous ranges, abort!
    if new_ranges.len() > 1 {
        warn!("deleted non-contiguous ranges! {new_ranges:?}");
        return Err(ChangeError::NonContiguousDelete);
    }

    let mut inserted = 0;

    // println!("inserting: {new_ranges:?}");

    for range in new_ranges {
        // insert cleared versions
        inserted += conn
        .prepare_cached(
            "
                INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, NULL, NULL, NULL);
            ",
        ).map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .execute(params![actor_id, range.start(), range.end()]).map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;
    }

    Ok(inserted)
}

#[tracing::instrument(skip(agent, bookie), err)]
pub async fn process_fully_buffered_changes(
    agent: &Agent,
    bookie: &Bookie,
    actor_id: ActorId,
    version: Version,
) -> Result<bool, ChangeError> {
    let mut conn = agent.pool().write_normal().await?;
    debug!(%actor_id, %version, "acquired write (normal) connection to process fully buffered changes");

    let booked = {
        bookie
            .write(format!(
                "process_fully_buffered(ensure):{}",
                actor_id.as_simple()
            ))
            .await
            .ensure(actor_id)
    };

    let mut bookedw = booked
        .write(format!(
            "process_fully_buffered(booked writer):{}",
            actor_id.as_simple()
        ))
        .await;
    debug!(%actor_id, %version, "acquired Booked write lock to process fully buffered changes");

    let inserted = block_in_place(|| {
        let (last_seq, ts) = {
            match bookedw.partials.get(&version) {
                Some(PartialVersion { seqs, last_seq, ts }) => {
                    if seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).count() != 0 {
                        error!(%actor_id, %version, "found sequence gaps: {:?}, aborting!", seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).collect::<RangeInclusiveSet<CrsqlSeq>>());
                        // TODO: return an error here
                        return Ok(false);
                    }
                    (*last_seq, *ts)
                }
                None => {
                    warn!(%actor_id, %version, "version not found in cache, returning");
                    return Ok(false);
                }
            }
        };

        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: Some(version),
            })?;

        info!(%actor_id, %version, "Processing buffered changes to crsql_changes (actor: {actor_id}, version: {version}, last_seq: {last_seq})");

        let max_db_version: Option<Option<CrsqlDbVersion>> = tx.prepare_cached("SELECT MAX(db_version) FROM __corro_buffered_changes WHERE site_id = ? AND version = ?").map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?.query_row(params![actor_id.as_bytes(), version], |row| row.get(0)).optional().map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;

        let start = Instant::now();

        if let Some(max_db_version) = max_db_version.flatten() {
            // insert all buffered changes into crsql_changes directly from the buffered changes table
            let count = tx
            .prepare_cached(
                r#"
                INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                    SELECT                 "table", pk, cid, val, col_version, ? as db_version, site_id, cl, seq
                        FROM __corro_buffered_changes
                            WHERE site_id = ?
                              AND version = ?
                            ORDER BY db_version ASC, seq ASC
                            "#,
            ).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
            .execute(params![max_db_version, actor_id.as_bytes(), version]).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;
            info!(%actor_id, %version, "Inserted {count} rows from buffered into crsql_changes in {:?}", start.elapsed());
        } else {
            info!(%actor_id, %version, "No buffered rows, skipped insertion into crsql_changes");
        }

        if let Err(e) = agent.tx_clear_buf().try_send((actor_id, version..=version)) {
            error!("could not schedule buffered data clear: {e}");
        }

        let rows_impacted: i64 = tx
            .prepare_cached("SELECT crsql_rows_impacted()")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: Some(version),
            })?
            .query_row((), |row| row.get(0))
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: Some(version),
            })?;

        debug!(%actor_id, %version, "rows impacted by buffered changes insertion: {rows_impacted}");

        if rows_impacted > 0 {
            let db_version: CrsqlDbVersion = tx
                .query_row("SELECT crsql_next_db_version()", [], |row| row.get(0))
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;
            debug!("db version: {db_version}");

            tx.prepare_cached(
                "
                INSERT OR IGNORE INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (
                        :actor_id,
                        :version,
                        :db_version,
                        :last_seq,
                        :ts
                    );",
            ).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
            .execute(named_params! {
                ":actor_id": actor_id,
                ":version": version,
                ":db_version": db_version,
                ":last_seq": last_seq,
                ":ts": ts
            }).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;

            debug!(%actor_id, %version, "inserted bookkeeping row after buffered insert");
        } else {
            store_empty_changeset(&tx, actor_id, version..=version)?;

            debug!(%actor_id, %version, "inserted CLEARED bookkeeping row after buffered insert");
        };

        let needed_changes =
            bookedw
                .insert_db(&tx, [version..=version].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: Some(version),
        })?;

        bookedw.apply_needed_changes(needed_changes);

        Ok::<_, ChangeError>(true)
    })?;

    Ok(inserted)
}

#[tracing::instrument(skip(agent, bookie, changes), err)]
pub async fn process_multiple_changes(
    agent: Agent,
    bookie: Bookie,
    changes: Vec<(ChangeV1, ChangeSource, Instant)>,
) -> Result<(), ChangeError> {
    let start = Instant::now();
    counter!("corro.agent.changes.processing.started").increment(changes.len() as u64);
    debug!(self_actor_id = %agent.actor_id(), "processing multiple changes, len: {}", changes.iter().map(|(change, _, _)| cmp::max(change.len(), 1)).sum::<usize>());

    let mut seen = HashSet::new();
    let mut unknown_changes: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (change, src, queued_at) in changes {
        histogram!("corro.agent.changes.queued.seconds").record(queued_at.elapsed());
        let versions = change.versions();
        let seqs = change.seqs();
        if !seen.insert((change.actor_id, versions, seqs.cloned())) {
            continue;
        }
        if bookie
            .write(format!(
                "process_multiple_changes(ensure):{}",
                change.actor_id.as_simple()
            ))
            .await
            .ensure(change.actor_id)
            .read(format!(
                "process_multiple_changes(contains?):{}",
                change.actor_id.as_simple()
            ))
            .await
            .contains_all(change.versions(), change.seqs())
        {
            continue;
        }

        unknown_changes
            .entry(change.actor_id)
            .or_default()
            .push((change, src));
    }

    let mut conn = agent.pool().write_normal().await?;

    let changesets = block_in_place(|| {
        let start = Instant::now();
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        let mut knowns: BTreeMap<ActorId, Vec<_>> = BTreeMap::new();
        let mut changesets = vec![];

        let mut last_db_version = None;

        // let mut writers: BTreeMap<ActorId, _> = Default::default();

        for (actor_id, changes) in unknown_changes {
            let booked = {
                bookie
                    .blocking_write(format!(
                        "process_multiple_changes(for_actor_blocking):{}",
                        actor_id.as_simple()
                    ))
                    .ensure(actor_id)
            };
            let booked_write = booked.blocking_write(format!(
                "process_multiple_changes(booked writer, unknown changes):{}",
                actor_id.as_simple()
            ));

            let mut seen = RangeInclusiveMap::new();

            for (change, src) in changes {
                trace!("handling a single changeset: {change:?}");
                let seqs = change.seqs();
                if booked_write.contains_all(change.versions(), change.seqs()) {
                    trace!("previously unknown versions are now deemed known, aborting inserts");
                    continue;
                }

                let versions = change.versions();

                // check if we've seen this version here...
                if versions.clone().all(|version| match seqs {
                    Some(check_seqs) => match seen.get(&version) {
                        Some(known) => match known {
                            KnownDbVersion::Partial(PartialVersion { seqs, .. }) => {
                                check_seqs.clone().all(|seq| seqs.contains(&seq))
                            }
                            KnownDbVersion::Current { .. } | KnownDbVersion::Cleared => true,
                        },
                        None => false,
                    },
                    None => seen.contains_key(&version),
                }) {
                    continue;
                }

                // optimizing this, insert later!
                let known = if change.is_complete() && change.is_empty() {
                    KnownDbVersion::Cleared
                } else {
                    if let Some(seqs) = change.seqs() {
                        if seqs.end() < seqs.start() {
                            warn!(%actor_id, versions = ?change.versions(), "received an invalid change, seqs start is greater than seqs end: {seqs:?}");
                            continue;
                        }
                    }

                    let (known, versions) = match process_single_version(
                        &agent,
                        &tx,
                        last_db_version,
                        change,
                    ) {
                        Ok((known, changeset)) => {
                            let versions = changeset.versions();
                            if let KnownDbVersion::Current(CurrentVersion { db_version, .. }) =
                                &known
                            {
                                last_db_version = Some(*db_version);
                                changesets.push((actor_id, changeset, *db_version, src));
                            }
                            (known, versions)
                        }
                        Err(e) => {
                            error!(%actor_id, ?versions, "could not process single change: {e}");
                            continue;
                        }
                    };
                    debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "got known to insert: {known:?}");
                    known
                };

                seen.insert(versions.clone(), known.clone());
                knowns.entry(actor_id).or_default().push((versions, known));
            }
            // if knowns.contains_key(&actor_id) {
            //     writers.insert(actor_id, booked_write);
            // }
        }

        let mut count = 0;
        let mut needed_changes = BTreeMap::new();

        for (actor_id, knowns) in knowns.iter_mut() {
            debug!(%actor_id, self_actor_id = %agent.actor_id(), "processing {} knowns", knowns.len());

            let mut all_versions = RangeInclusiveSet::new();

            for (versions, known) in knowns.iter() {
                match known {
                    KnownDbVersion::Partial { .. } => {}
                    KnownDbVersion::Current(CurrentVersion {
                        db_version,
                        last_seq,
                        ts,
                    }) => {
                        count += 1;
                        let version = versions.start();
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), %version, "inserting bookkeeping row db_version: {db_version}, ts: {ts:?}");
                        tx.prepare_cached("
                            INSERT OR IGNORE INTO __corro_bookkeeping ( actor_id,  start_version,  db_version,  last_seq,  ts)
                                                    VALUES  (:actor_id, :start_version, :db_version, :last_seq, :ts);").map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?
                            .execute(named_params!{
                                ":actor_id": actor_id,
                                ":start_version": *version,
                                ":db_version": *db_version,
                                ":last_seq": *last_seq,
                                ":ts": *ts
                            }).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?;
                    }
                    KnownDbVersion::Cleared => {
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserting CLEARED bookkeeping");
                        store_empty_changeset(&tx, *actor_id, versions.clone())?;
                    }
                }

                all_versions.insert(versions.clone());

                debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserted bookkeeping row");
            }

            let booked = {
                bookie
                    .blocking_write(format!(
                        "process_multiple_changes(for_actor_blocking):{actor_id}",
                    ))
                    .ensure(*actor_id)
            };
            let mut booked_write = booked.blocking_write(format!(
                "process_multiple_changes(booked writer, during knowns):{actor_id}",
            ));

            needed_changes.insert(
                *actor_id,
                booked_write
                    .insert_db(&tx, all_versions)
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: Some(*actor_id),
                        version: None,
                    })?,
            );
        }

        debug!("inserted {count} new changesets");

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?;

        for (_, changeset, _, _) in changesets.iter() {
            if let Some(ts) = changeset.ts() {
                let dur = (agent.clock().new_timestamp().get_time() - ts.0).to_duration();
                histogram!("corro.agent.changes.commit.lag.seconds").record(dur);
            }
        }

        debug!("committed {count} changes in {:?}", start.elapsed());

        for (actor_id, knowns) in knowns {
            let booked = {
                bookie
                    .blocking_write(format!(
                        "process_multiple_changes(for_actor_blocking):{actor_id}",
                    ))
                    .ensure(actor_id)
            };
            let mut booked_write = booked.blocking_write(format!(
                "process_multiple_changes(booked writer, before apply needed):{actor_id}",
            ));

            if let Some(needed_changes) = needed_changes.remove(&actor_id) {
                booked_write.apply_needed_changes(needed_changes);
            }

            for (versions, known) in knowns {
                let version = *versions.start();
                if let KnownDbVersion::Partial(partial) = known {
                    let PartialVersion { seqs, last_seq, .. } =
                        booked_write.insert_partial(version, partial);

                    let full_seqs_range = CrsqlSeq(0)..=last_seq;
                    let gaps_count = seqs.gaps(&full_seqs_range).count();
                    if gaps_count == 0 {
                        // if we have no gaps, then we can schedule applying all these changes.
                        debug!(%actor_id, %version, "we now have all versions, notifying for background jobber to insert buffered changes! seqs: {seqs:?}, expected full seqs: {full_seqs_range:?}");
                        let tx_apply = agent.tx_apply().clone();
                        tokio::spawn(async move {
                            if let Err(e) = tx_apply.send((actor_id, version)).await {
                                error!("could not send trigger for applying fully buffered changes later: {e}");
                            }
                        });
                    } else {
                        debug!(%actor_id, %version, "still have {gaps_count} gaps in partially buffered seqs");
                    }
                }
            }
        }

        Ok::<_, ChangeError>(changesets)
    })?;

    let mut change_chunk_size = 0;

    for (_actor_id, changeset, db_version, _src) in changesets {
        change_chunk_size += changeset.changes().len();
        agent
            .subs_manager()
            .match_changes(changeset.changes(), db_version);
    }

    histogram!("corro.agent.changes.processing.time.seconds").record(start.elapsed());
    histogram!("corro.agent.changes.processing.chunk_size").record(change_chunk_size as f64);

    Ok(())
}

#[tracing::instrument(skip(tx, parts), err)]
pub fn process_incomplete_version(
    tx: &Transaction,
    actor_id: ActorId,
    parts: &ChangesetParts,
) -> rusqlite::Result<KnownDbVersion> {
    let ChangesetParts {
        version,
        changes,
        seqs,
        last_seq,
        ts,
    } = parts;

    let mut changes_per_table = BTreeMap::new();

    debug!(%actor_id, %version, "incomplete change, seqs: {seqs:?}, last_seq: {last_seq:?}, len: {}", changes.len());
    let mut inserted = 0;
    for change in changes.iter() {
        trace!("buffering change! {change:?}");

        // insert change, do nothing on conflict
        let new_insertion = tx.prepare_cached(
            r#"
                INSERT INTO __corro_buffered_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, version)
                VALUES
                    (:table, :pk, :cid, :val, :col_version, :db_version, :site_id, :cl, :seq, :version)
                ON CONFLICT (site_id, db_version, version, seq)
                    DO NOTHING
            "#,
        )?
        .execute(named_params!{
            ":table": change.table.as_str(),
            ":pk": change.pk,
            ":cid": change.cid.as_str(),
            ":val": &change.val,
            ":col_version": change.col_version,
            ":db_version": change.db_version,
            ":site_id": &change.site_id,
            ":cl": change.cl,
            ":seq": change.seq,
            ":version": version,
        })?;

        inserted += new_insertion;

        if let Some(counter) = changes_per_table.get_mut(&change.table) {
            *counter += 1;
        } else {
            changes_per_table.insert(change.table.clone(), 1);
        }
    }

    debug!(%actor_id, %version, "buffered {inserted} changes");

    let deleted: Vec<RangeInclusive<CrsqlSeq>> = tx
        .prepare_cached(
            "
            DELETE FROM __corro_seq_bookkeeping
                WHERE site_id = :actor_id AND version = :version AND
                (
                    -- start_seq and end_seq are within the range
                    ( start_seq >= :start AND end_seq <= :end ) OR

                    -- range being inserted is partially contained within another
                    ( start_seq <= :end AND end_seq >= :end ) OR

                    -- start_seq = end + 1 (to collapse ranges)
                    ( start_seq = :end + 1) OR

                    -- end_seq = start - 1 (to collapse ranges)
                    ( end_seq = :start - 1 )
                )
                RETURNING start_seq, end_seq
        ",
        )?
        .query_map(
            named_params![
                ":actor_id": actor_id,
                ":version": version,
                ":start": seqs.start(),
                ":end": seqs.end(),
            ],
            |row| Ok(row.get(0)?..=row.get(1)?),
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert(seqs.clone());

    // we should never have deleted non-contiguous seq ranges, abort!
    if new_ranges.len() > 1 {
        warn!("deleted non-contiguous ranges! {new_ranges:?}");
        // this serves as a failsafe
        return Err(rusqlite::Error::StatementChangedRows(new_ranges.len()));
    }

    // insert new seq ranges, there should only be one...
    for range in new_ranges.clone() {
        tx
        .prepare_cached(
            "
                INSERT INTO __corro_seq_bookkeeping (site_id, version, start_seq, end_seq, last_seq, ts)
                    VALUES (?, ?, ?, ?, ?, ?);
            ",
        )?
        .execute(params![actor_id, version, range.start(), range.end(), last_seq, ts])?;
    }

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", "table" => table_name.to_string(), "source" => "remote").increment(count);
    }

    Ok(KnownDbVersion::Partial(PartialVersion {
        seqs: new_ranges,
        last_seq: *last_seq,
        ts: *ts,
    }))
}

#[tracing::instrument(skip(tx, last_db_version, parts), err)]
pub fn process_complete_version(
    tx: &Transaction,
    actor_id: ActorId,
    last_db_version: Option<CrsqlDbVersion>,
    versions: RangeInclusive<Version>,
    parts: ChangesetParts,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangesetParts {
        version,
        changes,
        seqs,
        last_seq,
        ts,
    } = parts;

    let len = changes.len();

    let max_db_version = changes
        .iter()
        .map(|c| c.db_version)
        .max()
        .unwrap_or(CrsqlDbVersion(0));

    debug!(%actor_id, %version, "complete change, applying right away! seqs: {seqs:?}, last_seq: {last_seq}, changes len: {len}, max db version: {max_db_version}");

    debug_assert!(len <= (seqs.end().0 - seqs.start().0 + 1) as usize);

    let mut impactful_changeset = vec![];

    let mut last_rows_impacted = 0;

    let mut changes_per_table = BTreeMap::new();

    // we need to manually increment the next db version for each changeset
    tx
        .prepare_cached("SELECT CASE WHEN COALESCE(?, crsql_db_version()) >= ? THEN crsql_next_db_version(crsql_next_db_version() + 1) END")?
        .query_row(params![last_db_version, max_db_version], |_row| Ok(()))?;

    for change in changes {
        trace!("inserting change! {change:?}");

        tx.prepare_cached(
            r#"
                INSERT INTO crsql_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                VALUES
                    (?,       ?,  ?,   ?,   ?,           ?,          ?,       ?,  ?)
            "#,
        )?
        .execute(params![
            change.table.as_str(),
            change.pk,
            change.cid.as_str(),
            &change.val,
            change.col_version,
            change.db_version,
            &change.site_id,
            change.cl,
            // increment the seq by the start_seq or else we'll have multiple change rows with the same seq
            change.seq,
        ])?;
        let rows_impacted: i64 = tx
            .prepare_cached("SELECT crsql_rows_impacted()")?
            .query_row((), |row| row.get(0))?;

        if rows_impacted > last_rows_impacted {
            trace!("inserted the change into crsql_changes");
            impactful_changeset.push(change);
            if let Some(c) = impactful_changeset.last() {
                if let Some(counter) = changes_per_table.get_mut(&c.table) {
                    *counter += 1;
                } else {
                    changes_per_table.insert(c.table.clone(), 1);
                }
            }
        }
        last_rows_impacted = rows_impacted;
    }

    let (known_version, new_changeset) = if impactful_changeset.is_empty() {
        (KnownDbVersion::Cleared, Changeset::Empty { versions })
    } else {
        // TODO: find a way to avoid this...
        let db_version: CrsqlDbVersion = tx
            .prepare_cached("SELECT crsql_next_db_version()")?
            .query_row([], |row| row.get(0))?;
        (
            KnownDbVersion::Current(CurrentVersion {
                db_version,
                last_seq,
                ts,
            }),
            Changeset::Full {
                version,
                changes: impactful_changeset,
                seqs,
                last_seq,
                ts,
            },
        )
    };

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", "table" => table_name.to_string(), "source" => "remote").increment(count);
    }

    Ok::<_, rusqlite::Error>((known_version, new_changeset))
}

pub fn check_buffered_meta_to_clear(
    conn: &Connection,
    actor_id: ActorId,
    versions: RangeInclusive<Version>,
) -> rusqlite::Result<bool> {
    let should_clear: bool = conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_buffered_changes WHERE site_id = ? AND version >= ? AND version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))?;
    if should_clear {
        return Ok(true);
    }

    conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_seq_bookkeeping WHERE site_id = ? AND version >= ? AND version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))
}
