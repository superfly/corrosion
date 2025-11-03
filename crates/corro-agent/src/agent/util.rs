//! Agent utility functions that don't have an obvious place in
//! another module
//!
//! Any set of functions that can create a coherent module (i.e. have
//! a similar API facade, handle the same kinds of data, etc) should
//! be pulled out of this file in future.

use crate::{
    agent::{handlers, CountedExecutor, TO_CLEAR_COUNT},
    api::public::{
        api_v1_db_schema, api_v1_queries, api_v1_table_stats, api_v1_transactions,
        pubsub::{api_v1_sub_by_id, api_v1_subs},
        update::SharedUpdateBroadcastCache,
    },
    transport::Transport,
};

use antithesis_sdk::assert_sometimes;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, Bookie, ChangeError, CurrentVersion, KnownDbVersion, PartialVersion},
    api::TableName,
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq},
    broadcast::{ChangeSource, ChangeV1, Changeset, ChangesetParts, FocaCmd, FocaInput},
    channel::CorroReceiver,
    config::AuthzConfig,
    pubsub::SubsManager,
    sqlite::{unnest_param, INSERT_CRSQL_CHANGES_QUERY},
    updates::{match_changes, match_changes_from_db_version},
};

use super::BcastCache;
use crate::api::public::update::api_v1_updates;
use antithesis_sdk::{assert_always, assert_unreachable};
use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    routing::{get, post},
    BoxError, Extension, Router,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use corro_types::broadcast::Timestamp;
use foca::Member;
use http::StatusCode;
use metrics::{counter, histogram};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{named_params, params, Connection};
use serde_json::json;
use spawn::spawn_counted;
use sqlite_pool::{Committable, InterruptibleTransaction};
use std::{
    cmp,
    collections::{BTreeMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    ops::{Deref, RangeInclusive},
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    task::{block_in_place, JoinHandle},
};
use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

pub async fn initialise_foca(agent: &Agent, states: Vec<(SocketAddr, Member<Actor>)>) {
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
                // Foca errors have 3 variants that are + Send but not + Sync,
                // so do a conversion here, which is unfortunate
                cb_rx
                    .await?
                    .map_err(|foca_err| eyre::eyre!("foca error: {foca_err}"))
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
    tripwire: &mut Tripwire,
    subs_bcast_cache: BcastCache,
    updates_bcast_cache: SharedUpdateBroadcastCache,
    subs_manager: &SubsManager,
    api_listeners: Vec<TcpListener>,
) -> eyre::Result<Vec<JoinHandle<()>>> {
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
            "/v1/updates/{table}",
            post(api_v1_updates).route_layer(
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
            "/v1/subscriptions/{id}",
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
                .layer(Extension(updates_bcast_cache))
                .layer(Extension(subs_manager.clone()))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http())
        .into_make_service_with_connect_info::<std::net::SocketAddr>();

    let mut handles: Vec<JoinHandle<()>> = vec![];
    for api_listener in api_listeners {
        let api_addr = api_listener.local_addr()?;
        info!("Starting API listener on tcp/{api_addr}");

        let mut svc = api.clone();
        let mut tw = tripwire.clone();
        let handle = spawn_counted(async move {
            loop {
                let (stream, addr) = tokio::select! {
                    res = api_listener.accept() => {
                        match res {
                            Ok(s) => s,
                            Err(error) => {
                                debug!(%api_addr, %error, "API listener closed");
                                break;
                            }
                        }
                    }
                    _ = &mut tw => {
                        break;
                    }
                };

                if let Err(error) = stream.set_nodelay(true) {
                    error!(%addr, %error, "failed to set nodelay");
                    continue;
                }

                use tower::Service;
                let Ok(svc) = svc.call(addr).await;
                let mut tw = tw.clone();
                tokio::spawn(async move {
                    let stream = hyper_util::rt::TokioIo::new(stream);

                    let hyper_service = hyper::service::service_fn(
                        move |request: hyper::Request<hyper::body::Incoming>| {
                            svc.clone().call(request)
                        },
                    );

                    let builder =
                        hyper_util::server::conn::auto::Builder::new(CountedExecutor).http1_only();
                    let conn = builder.serve_connection_with_upgrades(stream, hyper_service);
                    tokio::pin!(conn);

                    tokio::select! {
                        _res = conn.as_mut() => {
                            trace!("corrosion api is done");
                        }
                        _ = &mut tw => {
                            debug!("corrosion api http tripped {api_addr}");
                            conn.as_mut().graceful_shutdown();
                        }
                    };
                });
            }
        });

        handles.push(handle);
    }

    Ok(handles)
}

async fn require_authz(
    Extension(agent): Extension<Agent>,
    maybe_authz_header: Option<TypedHeader<Authorization<Bearer>>>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
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
    let min_sync_backoff = Duration::from_secs(agent.config().perf.min_sync_backoff as u64);
    let max_sync_backoff = Duration::from_secs(agent.config().perf.max_sync_backoff as u64);
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(min_sync_backoff, max_sync_backoff)
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
    mut rx_apply: CorroReceiver<(ActorId, CrsqlDbVersion)>,
    mut tripwire: Tripwire,
) {
    info!("Starting apply_fully_buffered_changes loop");

    let tx_timeout: Duration = Duration::from_secs(agent.config().perf.sql_tx_timeout as u64);
    while let Outcome::Completed(Some((actor_id, version))) =
        rx_apply.recv().preemptible(&mut tripwire).await
    {
        debug!(%actor_id, %version, "picked up background apply of buffered changes");
        match process_fully_buffered_changes(&agent, &bookie, actor_id, version, tx_timeout).await {
            Ok(false) => {
                warn!(%actor_id, %version, "did not apply buffered changes");
            }
            Ok(true) => {
                debug!(%actor_id, %version, "succesfully applied buffered changes");
            }
            Err(e) => {
                error!(%actor_id, %version, "could not apply fully buffered changes: {e}");
                let details = json!({"error": e.to_string()});
                assert_unreachable!("could not apply fully buffered changes", &details);
            }
        }
    }

    info!("fully_buffered_changes_loop ended");
}

/// Compact the database by finding cleared versions
pub async fn clear_buffered_meta_loop(
    agent: Agent,
    mut rx_partials: CorroReceiver<(ActorId, CrsqlDbVersionRange)>,
    tripwire: Tripwire,
) {
    let tx_timeout: Duration = Duration::from_secs(agent.config().perf.sql_tx_timeout as u64);

    let mut recv_tripwire = tripwire.clone();
    while let Outcome::Completed(Some((actor_id, versions))) =
        rx_partials.recv().preemptible(&mut recv_tripwire).await
    {
        let pool = agent.pool().clone();
        let self_actor_id = agent.actor_id();
        let mut tripwire = tripwire.clone();
        spawn_counted(async move {
            loop {
                let res = {
                    let mut conn = pool.write_low().await?;

                    block_in_place(|| {
                        let tx = InterruptibleTransaction::new(
                            conn.immediate_transaction()?,
                            Some(tx_timeout),
                            "clear_buffered_meta",
                        );

                        // TODO: delete buffered changes from deleted sequences only (maybe, it's kind of hard and may not be necessary)

                        // sub query required due to DELETE and LIMIT interaction
                        let seq_count = tx
                            .prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE (site_id, db_version, start_seq) IN (SELECT site_id, db_version, start_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version >= ? AND db_version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        // sub query required due to DELETE and LIMIT interaction
                        let buf_count = tx
                            .prepare_cached("DELETE FROM __corro_buffered_changes WHERE (site_id, db_version, seq) IN (SELECT site_id, db_version, seq FROM __corro_buffered_changes WHERE site_id = ? AND db_version >= ? AND db_version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        tx.commit()?;

                        Ok::<_, rusqlite::Error>((buf_count, seq_count))
                    })
                };

                match res {
                    Ok((buf_count, seq_count)) => {
                        if buf_count + seq_count > 0 {
                            assert_sometimes!(true, "Corrosion clears buffered meta");
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

                tokio::select! {
                    _ = &mut tripwire => {
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }

            Ok::<_, eyre::Report>(())
        });
    }
}

#[tracing::instrument(skip_all, err)]
pub fn process_single_version<T: Deref<Target = rusqlite::Connection> + Committable>(
    agent: &Agent,
    tx: &mut InterruptibleTransaction<T>,
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
    version: CrsqlDbVersion,
    tx_timeout: Duration,
) -> Result<bool, ChangeError> {
    let rows_impacted = {
        let mut conn = agent.pool().write_normal().await?;

        debug!(%actor_id, %version, "acquired write (normal) connection to process fully buffered changes");
        assert_sometimes!(true, "Corrosion processes fully buffered changes");
        let booked = {
            bookie
                .write("process_fully_buffered(ensure)", actor_id.as_simple())
                .await
                .ensure(actor_id)
        };

        let mut bookedw = booked
            .write(
                "process_fully_buffered(booked writer)",
                actor_id.as_simple(),
            )
            .await;
        debug!(%actor_id, %version, "acquired Booked write lock to process fully buffered changes");

        block_in_place(|| {
            let last_seq = {
                match bookedw.partials.get(&version) {
                    Some(PartialVersion { seqs, last_seq, .. }) => {
                        if seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).count() != 0 {
                            error!(%actor_id, %version, "found sequence gaps: {:?}, aborting!", seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).collect::<RangeInclusiveSet<CrsqlSeq>>());
                            // TODO: return an error here
                            return Ok(false);
                        }
                        *last_seq
                    }
                    None => {
                        warn!(%actor_id, %version, "version not found in cache, returning");
                        return Ok(false);
                    }
                }
            };

            let base_tx = conn
                .immediate_transaction()
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(version),
                })?;

            let tx = InterruptibleTransaction::new(
                base_tx,
                Some(tx_timeout),
                "process_buffered_changes",
            );

            info!(%actor_id, %version, "Processing buffered changes to crsql_changes (actor: {actor_id}, version: {version}, last_seq: {last_seq})");

            let rows_present: bool = tx.prepare_cached("SELECT EXISTS (SELECT 1 FROM __corro_buffered_changes WHERE site_id = ? AND db_version = ?)")
                                    .map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
                                    .query_row(params![actor_id, version], |row| row.get(0))
                                    .map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;

            let start = Instant::now();

            if rows_present {
                // insert all buffered changes into crsql_changes directly from the buffered changes table
                let count = tx
                    .prepare_cached(
                        r#"
                        INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
                            SELECT                 "table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts
                                FROM __corro_buffered_changes
                                    WHERE site_id = ?
                                    AND db_version = ?
                                    ORDER BY db_version ASC, seq ASC
                                    "#,
                    ).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?
                    .execute(params![actor_id.as_bytes(), version]).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;
                info!(%actor_id, %version, "Inserted {count} rows from buffered into crsql_changes in {:?}", start.elapsed());
            } else {
                info!(%actor_id, %version, "No buffered rows, skipped insertion into crsql_changes");
            }

            if let Err(e) = agent
                .tx_clear_buf()
                .try_send((actor_id, CrsqlDbVersionRange::single(version)))
            {
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

            let mut snap = bookedw.snapshot();
            snap.insert_db(&tx, [version..=version].into())
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

            bookedw.commit_snapshot(snap);

            Ok::<_, ChangeError>(rows_impacted > 0)
        })
    }?;

    if rows_impacted {
        let conn = agent.pool().read().await?;
        block_in_place(|| {
            if let Err(e) =
                match_changes_from_db_version(agent.subs_manager(), &conn, version, actor_id)
            {
                error!(%version, "could not match changes for subs from db version: {e}");
            }
        });

        block_in_place(|| {
            if let Err(e) =
                match_changes_from_db_version(agent.updates_manager(), &conn, version, actor_id)
            {
                error!(%version, "could not match changes for updates from db version: {e}");
            }
        });
    }

    Ok(rows_impacted)
}

#[tracing::instrument(skip(agent, bookie, changes), err)]
pub async fn process_multiple_changes(
    agent: Agent,
    bookie: Bookie,
    changes: Vec<(ChangeV1, ChangeSource, Instant)>,
    tx_timeout: Duration,
) -> Result<(), ChangeError> {
    let start = Instant::now();
    counter!("corro.agent.changes.processing.started").increment(changes.len() as u64);
    debug!(self_actor_id = %agent.actor_id(), "processing multiple changes, len: {}", changes.iter().map(|(change, _, _)| cmp::max(change.len(), 1)).sum::<usize>());
    trace!(self_actor_id = %agent.actor_id(), "changes: {changes:?}");

    const PROCESSING_WARN_THRESHOLD: Duration = Duration::from_secs(5);

    let mut seen = HashSet::new();
    let mut unknown_changes: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (change, src, queued_at) in changes {
        histogram!("corro.agent.changes.queued.seconds").record(queued_at.elapsed());
        let versions = change.versions();
        let seqs = change.seqs();

        if !seen.insert((change.actor_id, versions, seqs)) {
            continue;
        }

        let booked_writer = {
            bookie
                .write(
                    "process_multiple_changes(ensure)",
                    change.actor_id.as_simple(),
                )
                .await
                .ensure(change.actor_id)
        };
        if booked_writer
            .read(
                "process_multiple_changes(contains_all?)",
                change.actor_id.as_simple(),
            )
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
    let elapsed = start.elapsed();
    if elapsed >= PROCESSING_WARN_THRESHOLD {
        warn!("process_multiple_changes: removing duplicates took too long - {elapsed:?}");
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

        let mut tx =
            InterruptibleTransaction::new(tx, Some(tx_timeout), "process_multiple_changes");

        let mut processed: BTreeMap<ActorId, Vec<_>> = BTreeMap::new();
        let mut changesets = vec![];

        let mut count = 0;

        let sub_start = Instant::now();
        for (actor_id, changes) in unknown_changes {
            let booked = {
                bookie
                    .blocking_write(
                        "process_multiple_changes(for_actor_blocking)",
                        actor_id.as_simple(),
                    )
                    .ensure(actor_id)
            };
            let booked_write = booked.blocking_write(
                "process_multiple_changes(booked writer, unknown changes)",
                actor_id.as_simple(),
            );

            let max = booked_write.last();
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
                    Some(mut check_seqs) => match seen.get(&version) {
                        Some(maybe_partial) => match maybe_partial {
                            Some(PartialVersion { seqs, .. }) => {
                                check_seqs.all(|seq| seqs.contains(&seq))
                            }
                            // other kind of known version
                            None => true,
                        },
                        None => false,
                    },
                    None => seen.contains_key(&version),
                }) {
                    continue;
                }

                // optimizing this, insert later!
                let known = if change.is_complete() && change.is_empty() {
                    let versions = change.versions();
                    let end = versions.end();
                    // update db_version in db if it's greater than the max
                    // since we aren't passing any changes to crsql
                    if Some(end) > max {
                        process_empty_version(&tx, change.actor_id, &end).map_err(|e| {
                            ChangeError::Rusqlite {
                                source: e,
                                actor_id: Some(change.actor_id),
                                version: Some(end),
                            }
                        })?;
                    }
                    KnownDbVersion::Cleared
                } else {
                    if let Some(seqs) = change.seqs() {
                        if seqs.end() < seqs.start() {
                            warn!(%actor_id, versions = ?change.versions(), "received an invalid change, seqs start is greater than seqs end: {seqs:?}");
                            continue;
                        }
                    }

                    let (known, changeset) = {
                        match process_single_version(&agent, &mut tx, change) {
                            Ok(res) => {
                                count += 1;
                                res
                            }
                            Err(e) => {
                                error!(%actor_id, versions = ?versions, "error processing single version: {e}");
                                if e.sqlite_error_code().is_some_and(|code| {
                                    code != rusqlite::ErrorCode::DiskFull
                                        && code != rusqlite::ErrorCode::OperationInterrupted
                                }) {
                                    let details = json!({"error": e.to_string()});
                                    assert_unreachable!("error committing transaction", &details);
                                }
                                // the transaction was rolled back, so we need to return.
                                if tx.is_autocommit() {
                                    error!("error processing single version: {e} and transaction was rolled back");
                                    return Err(ChangeError::Rusqlite {
                                        source: e,
                                        actor_id: None,
                                        version: None,
                                    });
                                } else {
                                    error!("error processing single version: {e}");
                                    continue;
                                }
                            }
                        }
                    };

                    if let KnownDbVersion::Current(CurrentVersion { db_version, .. }) = &known {
                        // last_db_version = Some(*db_version);
                        changesets.push((actor_id, changeset, *db_version, src));
                    }

                    known
                };

                debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "got known to insert: {known:?}");
                let partial = match known {
                    KnownDbVersion::Partial(partial) => Some(partial),
                    _ => None,
                };

                seen.insert(versions.into(), partial.clone());
                processed
                    .entry(actor_id)
                    .or_default()
                    .push((versions, partial));
            }
        }

        let elapsed = sub_start.elapsed();
        if elapsed >= PROCESSING_WARN_THRESHOLD {
            warn!("process_multiple_changes:: process_single_version took too long - {elapsed:?}");
        }

        let sub_start = Instant::now();
        let mut snapshots = BTreeMap::new();

        for (actor_id, processed) in processed.iter() {
            debug!(%actor_id, self_actor_id = %agent.actor_id(), "processing {} changesets", processed.len());

            let booked = {
                bookie
                    .blocking_write(
                        "process_multiple_changes(for_actor_blocking)",
                        actor_id.as_simple(),
                    )
                    .ensure(*actor_id)
            };

            // FIXME: here we're making dangerous assumptions that nothing will modify booked versions
            let mut snap = match snapshots.remove(actor_id) {
                Some(snap) => snap,
                None => {
                    let booked_write = booked.blocking_write(
                        "process_multiple_changes(booked writer, during processed)",
                        actor_id.as_simple(),
                    );
                    booked_write.snapshot()
                }
            };

            snap.insert_db(
                &tx,
                processed
                    .iter()
                    .map(|(versions, _)| versions.into())
                    .collect(),
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(*actor_id),
                version: None,
            })?;

            snapshots.insert(*actor_id, snap);
        }

        debug!("inserted {count} new changesets");

        tx.commit().map_err(|source| {
            // only sqlite error we expect is SQLITE_FULL if disk is full
            if source.sqlite_error_code().is_some_and(|code| {
                code != rusqlite::ErrorCode::DiskFull
                    && code != rusqlite::ErrorCode::OperationInterrupted
            }) {
                let details =
                    json!({"elapsed": elapsed.as_secs_f32(), "error": source.to_string()});
                assert_unreachable!("error committing transaction", &details);
            }
            ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            }
        })?;

        let elapsed = sub_start.elapsed();
        if elapsed >= PROCESSING_WARN_THRESHOLD {
            warn!("process_multiple_changes: commiting transaction took too long - {elapsed:?}");
        }

        for (_, changeset, _, _) in changesets.iter() {
            if let Some(ts) = changeset.ts() {
                let dur = (agent.clock().new_timestamp().get_time() - ts.0).to_duration();
                histogram!("corro.agent.changes.commit.lag.seconds").record(dur);
            }
        }

        debug!("committed {count} changes in {:?}", start.elapsed());

        let sub_start = Instant::now();
        for (actor_id, processed) in processed {
            let booked = {
                bookie
                    .blocking_write(
                        "process_multiple_changes(for_actor_blocking)",
                        actor_id.as_simple(),
                    )
                    .ensure(actor_id)
            };
            let mut booked_write = booked.blocking_write(
                "process_multiple_changes(booked writer, before apply needed)",
                actor_id.as_simple(),
            );

            if let Some(snap) = snapshots.remove(&actor_id) {
                booked_write.commit_snapshot(snap);
            }

            for (versions, partial) in processed {
                let version = versions.start();
                if let Some(partial) = partial {
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

        let elapsed = sub_start.elapsed();
        let details = json!({"elapsed": elapsed.as_secs_f32()});
        assert_always!(
            elapsed < Duration::from_secs(60),
            "process_multiple_changes took too long",
            &details
        );
        if elapsed >= PROCESSING_WARN_THRESHOLD {
            warn!("process_multiple_changes: commiting snapshots took too long - {elapsed:?}");
        }

        Ok::<_, ChangeError>(changesets)
    })?;

    let mut change_chunk_size = 0;

    for (_actor_id, changeset, db_version, _src) in changesets {
        change_chunk_size += changeset.len();
        match_changes(agent.subs_manager(), &changeset, db_version);
        match_changes(agent.updates_manager(), &changeset, db_version);
    }

    histogram!("corro.agent.changes.processing.time.seconds", "source" => "remote")
        .record(start.elapsed());
    histogram!("corro.agent.changes.processing.chunk_size").record(change_chunk_size as f64);

    Ok(())
}

#[tracing::instrument(skip(tx), err)]
pub fn process_empty_version<T: Deref<Target = rusqlite::Connection> + Committable>(
    tx: &InterruptibleTransaction<T>,
    actor_id: ActorId,
    end_version: &CrsqlDbVersion,
) -> rusqlite::Result<()> {
    let _ = tx
        .prepare_cached("SELECT crsql_set_db_version(?, ?)")?
        .query_row((actor_id, end_version), |row| row.get::<_, String>(0))?;

    Ok(())
}

#[tracing::instrument(skip(sp, parts), err)]
pub fn process_incomplete_version<T: Deref<Target = rusqlite::Connection> + Committable>(
    sp: &InterruptibleTransaction<T>,
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
    assert_sometimes!(true, "Corrosion processes incomplete changes");
    let mut inserted = 0;
    for change in changes.iter() {
        trace!("buffering change! {change:?}");

        // insert change, do nothing on conflict
        let new_insertion = sp
            .prepare_cached(
                r#"
                INSERT INTO __corro_buffered_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
                VALUES
                    (:table, :pk, :cid, :val, :col_version, :db_version, :site_id, :cl, :seq, :ts)
                ON CONFLICT (site_id, db_version, seq)
                    DO NOTHING
            "#,
            )?
            .execute(named_params! {
                ":table": change.table.as_str(),
                ":pk": change.pk,
                ":cid": change.cid.as_str(),
                ":val": &change.val,
                ":col_version": change.col_version,
                ":db_version": change.db_version,
                ":site_id": &change.site_id,
                ":cl": change.cl,
                ":seq": change.seq,
                ":ts": ts,
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
                WHERE site_id = :actor_id AND db_version = :db_version AND
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
                ":db_version": version,
                ":start": seqs.start_int(),
                ":end": seqs.end_int(),
            ],
            |row| Ok(row.get(0)?..=row.get(1)?),
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert((*seqs).into());

    let details = json!({"new_ranges": new_ranges});
    assert_always!(
        new_ranges.len() == 1,
        "deleted non-contiguous seq ranges!",
        &details
    );
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
                INSERT INTO __corro_seq_bookkeeping (site_id, db_version, start_seq, end_seq, last_seq, ts)
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

#[tracing::instrument(skip(agent, sp, parts), err)]
pub fn process_complete_version<T: Deref<Target = rusqlite::Connection> + Committable>(
    agent: Agent,
    sp: &InterruptibleTransaction<T>,
    actor_id: ActorId,
    versions: CrsqlDbVersionRange,
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

    debug!(%actor_id, %version, "complete change, applying right away! seqs: {seqs:?}, last_seq: {last_seq}, changes len: {len}, db version: {version}");

    let details = json!({"len": len, "seqs": seqs.start_int(), "seqs_end": seqs.end_int(), "actor_id": actor_id, "version": version});
    assert_always!(
        len <= seqs.len(),
        "number of changes is equal to the seq len",
        &details
    );
    debug_assert!(len <= seqs.len(), "change from actor {actor_id} version {version} has len {len} but seqs range is {seqs:?} and last_seq is {last_seq}");

    // Insert all the changes in a single statement
    // This will return a non zero rowid only if the change impacted the database
    let mut stmt = sp.prepare_cached(INSERT_CRSQL_CHANGES_QUERY)?;
    trace!("inserting {:?} changes into crsql_changes", changes);
    let params = params![
        unnest_param(changes.iter().map(|c| c.table.as_str())),
        unnest_param(changes.iter().map(|c| &c.pk)),
        unnest_param(changes.iter().map(|c| c.cid.as_str())),
        unnest_param(changes.iter().map(|c| &c.val)),
        unnest_param(changes.iter().map(|c| &c.col_version)),
        unnest_param(changes.iter().map(|c| &c.db_version)),
        unnest_param(changes.iter().map(|c| &c.site_id)),
        unnest_param(changes.iter().map(|c| &c.cl)),
        unnest_param(changes.iter().map(|c| &c.seq)),
        unnest_param(changes.iter().map(|_| &ts)),
    ];
    let mut last_rowids = stmt
        .query_map(params, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<Vec<(CrsqlDbVersion, CrsqlSeq, i64)>>>()?;

    // Ignore this, it's just for debugging if we hit this very rare VDBE bug in production
    let last_rowids_len = last_rowids.len();
    if last_rowids_len != len {
        // This should never happen, but if it does, i need data for debugging
        let query_plan = sp
            .prepare(&format!("EXPLAIN QUERY PLAN {INSERT_CRSQL_CHANGES_QUERY}",))?
            .query_map(params, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            // id, parent, notused, detail
            .collect::<rusqlite::Result<Vec<(i32, i32, i32, String)>>>()?;
        let vdbe_program = sp
            .prepare(&format!("EXPLAIN {INSERT_CRSQL_CHANGES_QUERY}"))?
            .query_map(params, |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })?
            .collect::<rusqlite::Result<
                Vec<(
                    i32,            // addr
                    String,         // opcode
                    i32,            // p1
                    i32,            // p2
                    i32,            // p3
                    Option<String>, // p4
                    Option<i32>,    // p5
                    Option<String>, // comment
                )>,
            >>()?;
        let details = json!({
            "changes_len": len,
            "returned_rowids_len": last_rowids.len(),
            "query_plan": query_plan,
            "vdbe_program": vdbe_program,
        });
        error!("Possible returning statement BUG: {details}");
        assert_unreachable!("Possible returning statement BUG:", &details);
    }

    debug!("successfully inserted {len} changes into crsql_changes");
    trace!("last_rowids before shift: {last_rowids:?}");

    // RETURNING returns rows BEFORE the insert, not after
    // so the rowids we get will be shifted by one
    // we need to shift them back
    for i in 0..(last_rowids_len - 1) {
        last_rowids[i].2 = last_rowids[i + 1].2;
    }
    last_rowids[last_rowids_len - 1].2 = sp
        .prepare_cached("SELECT last_insert_rowid()")?
        .query_row(params![], |row| row.get(0))?;

    trace!("last_rowids after shift: {last_rowids:?}");

    // Now determine which exact changes impacted the database
    // This is mostly for keeping accurate metrics
    let mut impactful_changeset = vec![];
    let mut changes_per_table = BTreeMap::new();
    for (change, (db_version, seq, rowid)) in changes.into_iter().zip(last_rowids) {
        // Those asserts are only a sanity check
        assert!(db_version == change.db_version);
        assert!(seq == change.seq);
        if rowid != 0 {
            let table_name = change.table.clone();
            impactful_changeset.push(change);
            changes_per_table
                .entry(table_name)
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
                }
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
        (
            KnownDbVersion::Current(CurrentVersion {
                db_version: version,
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
    versions: CrsqlDbVersionRange,
) -> rusqlite::Result<bool> {
    let should_clear: bool = conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_buffered_changes WHERE site_id = ? AND db_version >= ? AND db_version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))?;
    if should_clear {
        return Ok(true);
    }

    conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version >= ? AND db_version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))
}

pub fn log_at_pow_10(msg: &str, count: &mut u64) {
    *count += 1;
    if is_pow_10(*count) {
        warn!("{msg} (log count: {count})")
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
