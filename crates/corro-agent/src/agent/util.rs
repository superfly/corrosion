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
        find_overwritten_versions, Agent, Bookie, ChangeError, CurrentVersion, KnownDbVersion,
        PartialVersion,
    },
    api::TableName,
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{ChangeSource, ChangeV1, Changeset, ChangesetParts, FocaCmd, FocaInput},
    change::store_empty_changeset,
    channel::CorroReceiver,
    config::AuthzConfig,
    pubsub::SubsManager,
};
use std::{
    cmp,
    collections::{BTreeMap, HashSet},
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
use corro_types::broadcast::Timestamp;
use foca::Member;
use futures::FutureExt;
use hyper::{server::conn::AddrIncoming, StatusCode};
use metrics::{counter, histogram};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{named_params, params, Connection, OptionalExtension, Savepoint, Transaction};
use spawn::spawn_counted;
use tokio::{net::TcpListener, task::block_in_place};
use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

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
    api_listeners: Vec<TcpListener>,
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

    for api_listener in api_listeners {
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
    }

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

/// Periodically initiate a sync with many other nodes.  Before we do
/// though, apply buffered/ partial changesets to avoid having to sync
/// things we should already know about.
///
/// Actual sync logic is handled by
/// [`handle_sync`](crate::agent::handlers::handle_sync).
pub async fn sync_loop(agent: Agent, bookie: Bookie, transport: Transport, mut tripwire: Tripwire) {
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(Duration::from_secs(1), MAX_SYNC_BACKOFF)
        .iter();
    let next_sync_at = tokio::time::sleep(sync_backoff.next().unwrap());
    tokio::pin!(next_sync_at);

    loop {
        tokio::select! {
            biased;

            _ = &mut next_sync_at => {},
            _ = &mut tripwire => {
                break;
            }
        };

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
}

pub async fn apply_fully_buffered_changes_loop(
    agent: Agent,
    bookie: Bookie,
    mut rx_apply: CorroReceiver<(ActorId, Version)>,
    mut tripwire: Tripwire,
) {
    info!("Starting apply_fully_buffered_changes loop");

    while let Outcome::Completed(Some((actor_id, version))) =
        rx_apply.recv().preemptible(&mut tripwire).await
    {
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

    info!("fully_buffered_changes_loop ended");
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
    tx: &mut Transaction,
    last_db_version: Option<CrsqlDbVersion>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let sp = tx.savepoint()?;
    let mut changes_per_table = BTreeMap::new();
    let (known, changeset) = if changeset.is_complete() {
        let (known, changeset, table) = process_complete_version(
            agent.clone(),
            &sp,
            actor_id,
            last_db_version,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
        )?;

        if check_buffered_meta_to_clear(&sp, actor_id, changeset.versions())? {
            if let Err(e) = agent
                .tx_clear_buf()
                .try_send((actor_id, changeset.versions()))
            {
                error!("could not schedule buffered meta clear: {e}");
            }
        }
        changes_per_table = table;

        (known, changeset)
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = process_incomplete_version(&sp, actor_id, &parts)?;

        (known, parts.into())
    };

    sp.commit()?;

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", "table" => table_name.to_string(), "source" => "remote").increment(count);
    }

    Ok((known, changeset))
}

#[tracing::instrument(skip(agent, bookie), err)]
pub async fn process_fully_buffered_changes(
    agent: &Agent,
    bookie: &Bookie,
    actor_id: ActorId,
    version: Version,
) -> Result<bool, ChangeError> {
    let db_version = {
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

        block_in_place(|| {
            let (last_seq, ts) = {
                match bookedw.partials.get(&version) {
                    Some(PartialVersion { seqs, last_seq, ts }) => {
                        if seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).count() != 0 {
                            error!(%actor_id, %version, "found sequence gaps: {:?}, aborting!", seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).collect::<RangeInclusiveSet<CrsqlSeq>>());
                            // TODO: return an error here
                            return Ok(None);
                        }
                        (*last_seq, *ts)
                    }
                    None => {
                        warn!(%actor_id, %version, "version not found in cache, returning");
                        return Ok(None);
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

            let db_version = if rows_impacted > 0 {
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

                Some(db_version)
            } else {
                store_empty_changeset(
                    &tx,
                    actor_id,
                    version..=version,
                    Timestamp::from(agent.clock().new_timestamp()),
                )?;
                debug!(%actor_id, %version, "inserted CLEARED bookkeeping row after buffered insert");
                None
            };

            let mut snap = bookedw.snapshot();
            snap.insert_db(&tx, [version..=version].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            let overwritten =
                find_overwritten_versions(&tx).map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            let mut last_cleared: Option<Timestamp> = None;
            for (actor_id, versions_set) in overwritten {
                if actor_id != agent.actor_id() {
                    warn!("clearing empties for another actor: {actor_id}")
                }
                for versions in versions_set {
                    let ts = Timestamp::from(agent.clock().new_timestamp());
                    let inserted = store_empty_changeset(&tx, actor_id, versions, ts)?;
                    if inserted > 0 {
                        last_cleared = Some(ts);
                    }
                }
            }

            let mut agent_booked = {
                agent
                    .booked()
                    .blocking_write("process_fully_buffered_changes(get snapshot)")
            };

            let mut agent_snap = agent_booked.snapshot();
            if let Some(ts) = last_cleared {
                agent_snap
                    .update_cleared_ts(&tx, ts)
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: Some(actor_id),
                        version: Some(version),
                    })?;
            }

            tx.commit().map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: Some(version),
            })?;

            bookedw.commit_snapshot(snap);
            agent_booked.commit_snapshot(agent_snap);

            Ok::<_, ChangeError>(db_version)
        })
    }?;

    if let Some(db_version) = db_version {
        let conn = agent.pool().read().await?;
        block_in_place(|| {
            if let Err(e) = agent
                .subs_manager()
                .match_changes_from_db_version(&conn, db_version)
            {
                error!(%db_version, "could not match changes from db version: {e}");
            }
        });
    }

    Ok(db_version.is_some())
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

        let booked_writer = {
            bookie
                .write(format!(
                    "process_multiple_changes(ensure):{}",
                    change.actor_id.as_simple()
                ))
                .await
                .ensure(change.actor_id)
        };
        if booked_writer
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
        let mut tx = conn
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
                let ts = change.ts();
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

                    let (known, changeset) = {
                        match process_single_version(&agent, &mut tx, last_db_version, change) {
                            Ok(res) => res,
                            Err(e) => {
                                error!("error processing single version: {e}");
                                continue;
                            }
                        }
                    };

                    let versions = changeset.versions();
                    if let KnownDbVersion::Current(CurrentVersion { db_version, .. }) = &known {
                        last_db_version = Some(*db_version);
                        changesets.push((actor_id, changeset, *db_version, src));
                    }

                    debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "got known to insert: {known:?}");
                    known
                };

                seen.insert(versions.clone(), known.clone());
                knowns
                    .entry(actor_id)
                    .or_default()
                    .push((versions, ts, known));
            }
            // if knowns.contains_key(&actor_id) {
            //     writers.insert(actor_id, booked_write);
            // }
        }

        let mut count = 0;
        let mut snapshots = BTreeMap::new();

        for (actor_id, knowns) in knowns.iter_mut() {
            debug!(%actor_id, self_actor_id = %agent.actor_id(), "processing {} knowns", knowns.len());

            let mut all_versions = RangeInclusiveSet::new();

            for (versions, ts, known) in knowns.iter() {
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
                        let ts = ts.unwrap_or(Timestamp::from(agent.clock().new_timestamp()));
                        store_empty_changeset(&tx, *actor_id, versions.clone(), ts)?;
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

            // FIXME: here we're making dangerous assumptions that nothing will modify booked versions
            let mut snap = match snapshots.remove(actor_id) {
                Some(snap) => snap,
                None => {
                    let booked_write = booked.blocking_write(format!(
                        "process_multiple_changes(booked writer, during knowns):{actor_id}",
                    ));
                    booked_write.snapshot()
                }
            };

            snap.insert_db(&tx, all_versions)
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(*actor_id),
                    version: None,
                })?;

            snapshots.insert(*actor_id, snap);
        }

        debug!("inserted {count} new changesets");

        let overwritten =
            find_overwritten_versions(&tx).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        let mut last_cleared: Option<Timestamp> = None;
        for (actor_id, versions_set) in overwritten {
            if actor_id != agent.actor_id() {
                warn!("clearing and setting timestamp for empties from a different node");
            }
            for versions in versions_set {
                let ts = Timestamp::from(agent.clock().new_timestamp());
                let inserted = store_empty_changeset(&tx, actor_id, versions, ts)?;
                if inserted > 0 {
                    last_cleared = Some(ts);
                }
            }
        }

        if let Some(ts) = last_cleared {
            let mut snap = {
                agent
                    .booked()
                    .blocking_write("process_multiple_changes(update_cleared_ts snapshot)")
                    .snapshot()
            };

            snap.update_cleared_ts(&tx, ts)
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: None,
                    version: None,
                })?;

            std::mem::forget(snap);
        }

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?;

        if let Some(ts) = last_cleared {
            let mut booked_writer = agent
                .booked()
                .blocking_write("process_multiple_changes(update_cleared_ts)");
            booked_writer.update_cleared_ts(ts);
        }

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

            if let Some(snap) = snapshots.remove(&actor_id) {
                booked_write.commit_snapshot(snap);
            }

            for (versions, _, known) in knowns {
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
                        debug!(%actor_id, %version, "still have {gaps_count} gaps in partially buffered seqs: {:?}", seqs.gaps(&full_seqs_range).collect::<Vec<_>>());
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

#[tracing::instrument(skip(sp, parts), err)]
pub fn process_incomplete_version(
    sp: &Savepoint,
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
        let new_insertion = sp.prepare_cached(
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

    let deleted: Vec<RangeInclusive<CrsqlSeq>> = sp
        .prepare_cached(
            "
            DELETE FROM __corro_seq_bookkeeping
                WHERE site_id = :actor_id AND version = :version AND
                (
                    -- [:start]---[start_seq]---[:end]
                    ( start_seq BETWEEN :start AND :end ) OR

                    -- [start_seq]---[:start]---[:end]---[end_seq]
                    ( start_seq <= :start AND end_seq >= :end ) OR

                    -- [:start]---[start_seq]---[:end]---[end_seq]
                    ( start_seq <= :end AND end_seq >= :end ) OR

                    -- [:start]---[end_seq]---[:end]
                    ( end_seq BETWEEN :start AND :end ) OR

                    -- ---[:end][start_seq]---[end_seq]
                    ( start_seq = :end + 1 AND end_seq ) OR

                    -- [end_seq][:start]---
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
        warn!("deleted non-contiguous seq ranges! {new_ranges:?}");
        // this serves as a failsafe
        return Err(rusqlite::Error::StatementChangedRows(new_ranges.len()));
    }

    // insert new seq ranges, there should only be one...
    for range in new_ranges.clone() {
        sp
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

#[tracing::instrument(skip(agent, sp, last_db_version, parts), err)]
pub fn process_complete_version(
    agent: Agent,
    sp: &Savepoint,
    actor_id: ActorId,
    last_db_version: Option<CrsqlDbVersion>,
    versions: RangeInclusive<Version>,
    parts: ChangesetParts,
) -> rusqlite::Result<(KnownDbVersion, Changeset, BTreeMap<TableName, u64>)> {
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
    sp
        .prepare_cached("SELECT CASE WHEN COALESCE(?, crsql_db_version()) >= ? THEN crsql_next_db_version(crsql_next_db_version() + 1) END")?
        .query_row(params![last_db_version, max_db_version], |_row| Ok(()))?;

    for change in changes {
        trace!("inserting change! {change:?}");

        sp.prepare_cached(
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
        let rows_impacted: i64 = sp
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
        (
            KnownDbVersion::Cleared,
            Changeset::Empty {
                versions,
                ts: Some(Timestamp::from(agent.clock().new_timestamp())),
            },
        )
    } else {
        // TODO: find a way to avoid this...
        let db_version: CrsqlDbVersion = sp
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

    Ok::<_, rusqlite::Error>((known_version, new_changeset, changes_per_table))
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

pub fn log_at_pow_10(msg: &str, count: &mut u64) {
    if is_pow_10(*count + 1) {
        warn!("{} (log count: {})", msg, count)
    }
    // reset count
    if *count == 100000000 {
        *count = 0;
    }
}

#[inline]
fn is_pow_10(i: u64) -> bool {
    matches!(
        i,
        1 | 10 | 100 | 1000 | 10000 | 1000000 | 10000000 | 100000000
    )
}
