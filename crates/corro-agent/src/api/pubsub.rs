use std::{
    collections::HashMap,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, BytesMut};
use compact_str::{format_compact, CompactString, ToCompactString};
use corro_types::{
    agent::Agent,
    api::{QueryEvent, Statement},
    change::SqliteValue,
    pubsub::{normalize_sql, Matcher, MatcherCmd},
};
use futures::future::poll_fn;
use rusqlite::Connection;
use tokio::{
    sync::{broadcast, mpsc, RwLock as TokioRwLock},
    task::block_in_place,
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub async fn api_v1_sub_by_id(
    Extension(agent): Extension<Agent>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> impl IntoResponse {
    sub_by_id(agent, id).await
}

async fn sub_by_id(agent: Agent, id: Uuid) -> hyper::Response<hyper::Body> {
    let matcher = match { agent.matchers().read().get(&id).cloned() } {
        Some(matcher) => matcher,
        None => {
            return hyper::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(format_compact!(
                        "could not find subscription with id {id}"
                    )))
                    .expect("could not serialize queries stream error")
                    .into(),
                )
                .expect("could not build error response")
        }
    };

    let (tx, body) = hyper::Body::channel();

    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (init_tx, init_rx) = mpsc::channel(512);
    let change_rx = matcher.subscribe();
    let cancel = matcher.cancel();

    tokio::spawn(process_sub_channel(
        agent.clone(),
        id,
        tx,
        init_rx,
        change_rx,
        matcher.cmd_tx().clone(),
        cancel,
    ));

    let pool = agent.pool().dedicated_pool().clone();
    tokio::spawn(async move {
        if let Err(_e) = init_tx
            .send(QueryEvent::Columns(matcher.0.col_names.clone()))
            .await
        {
            warn!("could not send back column names, client is probably gone. returning.");
            return;
        }

        let conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                _ = init_tx.send(QueryEvent::Error(e.to_compact_string())).await;
                return;
            }
        };

        #[derive(Debug, thiserror::Error)]
        enum QueryTempError {
            #[error(transparent)]
            Sqlite(#[from] rusqlite::Error),
            #[error(transparent)]
            Send(#[from] mpsc::error::SendError<QueryEvent>),
        }

        let res = block_in_place(|| {
            let mut query_cols = vec![];
            for i in 0..(matcher.0.parsed.columns.len()) {
                query_cols.push(format!("col_{i}"));
            }
            let mut prepped = conn.prepare_cached(&format!(
                "SELECT __corro_rowid,{} FROM {}",
                query_cols.join(","),
                matcher.table_name()
            ))?;
            let col_count = prepped.column_count();

            init_tx.blocking_send(QueryEvent::Columns(matcher.0.col_names.clone()))?;

            let start = Instant::now();
            let mut rows = prepped.query(())?;
            let elapsed = start.elapsed();

            loop {
                let row = match rows.next()? {
                    Some(row) => row,
                    None => break,
                };
                let rowid = row.get(0)?;

                let cells = (1..col_count)
                    .map(|i| row.get::<_, SqliteValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                init_tx.blocking_send(QueryEvent::Row(rowid, cells))?;
            }

            _ = init_tx.blocking_send(QueryEvent::EndOfQuery {
                time: elapsed.as_secs_f64(),
            })?;

            Ok::<_, QueryTempError>(())
        });

        if let Err(QueryTempError::Sqlite(e)) = res {
            _ = init_tx.send(QueryEvent::Error(e.to_compact_string())).await;
        }
    });

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", id.to_string())
        .body(body)
        .expect("could not build query response body")
}

async fn process_sub_channel(
    agent: Agent,
    matcher_id: Uuid,
    mut tx: hyper::body::Sender,
    mut init_rx: mpsc::Receiver<QueryEvent>,
    mut change_rx: broadcast::Receiver<QueryEvent>,
    cmd_tx: mpsc::Sender<MatcherCmd>,
    cancel: CancellationToken,
) {
    let mut buf = BytesMut::new();

    let mut init_done = false;
    let mut check_ready = interval(Duration::from_secs(1));
    let mut cancelled = false;
    loop {
        // either we get data we need to transmit
        // or we check every 1s if the client is still ready to receive data
        let query_evt = tokio::select! {
            _ = cancel.cancelled() => {
                debug!("canceled!");
                cancelled = true;
                break;
            },
            maybe_query_evt = init_rx.recv(), if !init_done => match maybe_query_evt {
                Some(query_evt) => query_evt,
                None => {
                    init_done = true;
                    continue;
                }
            },
            res = change_rx.recv(), if init_done => match res {
                Ok(query_evt) => query_evt,
                Err(e) => {
                    warn!("could not receive change: {e}");
                    break;
                }
            },
            _ = check_ready.tick() => {
                if let Err(_) = poll_fn(|cx| tx.poll_ready(cx)).await {
                    break;
                }
                continue;
            }
        };

        {
            let mut writer = (&mut buf).writer();

            let mut query_evt = query_evt;

            loop {
                if matches!(query_evt, QueryEvent::EndOfQuery { .. }) {
                    init_done = true;
                }

                let mut recv = if init_done {
                    TryReceiver::Broadcast(&mut change_rx)
                } else {
                    TryReceiver::Mpsc(&mut init_rx)
                };

                if let Err(e) = serde_json::to_writer(&mut writer, &query_evt) {
                    _ = tx
                        .send_data(
                            serde_json::to_vec(&serde_json::json!(QueryEvent::Error(
                                e.to_compact_string()
                            )))
                            .expect("could not serialize error json")
                            .into(),
                        )
                        .await;
                    return;
                }
                // NOTE: I think that's infaillible...
                writer
                    .write(b"\n")
                    .expect("could not write new line to BytesMut Writer");

                // accumulate up to ~64KB
                // TODO: predict if we can fit one more in 64KB or not based on previous writes
                if writer.get_ref().len() >= 64 * 1024 {
                    break;
                }

                match recv.try_recv() {
                    Ok(new) => query_evt = new,
                    Err(e) => {
                        match e {
                            TryRecvError::Empty => break,
                            TryRecvError::Closed => break,

                            TryRecvError::Lagged(lagged) => {
                                error!("change recv lagged by {lagged}, stopping subscription processing");
                                return;
                            }
                        }
                    }
                }
            }
        }

        if let Err(e) = tx.send_data(buf.split().freeze()).await {
            error!("could not send data through body's channel: {e}");
            return;
        }
    }
    debug!("query body channel done");

    drop(change_rx);

    if cancelled {
        // try to remove if it exists.
        info!("matcher {matcher_id} was cancelled, removing");
        agent.matchers().write().remove(&matcher_id);
    } else {
        _ = cmd_tx.send(MatcherCmd::Unsubscribe).await;
        let mut matchers = agent.matchers().write();
        let no_mo_receivers = matchers
            .get(&matcher_id)
            .map(|m| m.receiver_count() == 0)
            .unwrap_or(false);
        if no_mo_receivers {
            info!("no more receivers for matcher {matcher_id}, removing");
            matchers.remove(&matcher_id);
        }
    }
}

enum TryReceiver<'a, T> {
    Mpsc(&'a mut mpsc::Receiver<T>),
    Broadcast(&'a mut broadcast::Receiver<T>),
}

impl<'a, T> TryReceiver<'a, T>
where
    T: Clone,
{
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        Ok(match self {
            TryReceiver::Mpsc(r) => r.try_recv()?,
            TryReceiver::Broadcast(b) => b.try_recv()?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum TryRecvError {
    #[error("empty")]
    Empty,
    #[error("closed")]
    Closed,
    #[error("lagged by {0}")]
    Lagged(u64),
}

impl From<broadcast::error::TryRecvError> for TryRecvError {
    fn from(value: broadcast::error::TryRecvError) -> Self {
        match value {
            broadcast::error::TryRecvError::Empty => TryRecvError::Empty,
            broadcast::error::TryRecvError::Closed => TryRecvError::Closed,
            broadcast::error::TryRecvError::Lagged(lagged) => TryRecvError::Lagged(lagged),
        }
    }
}

impl From<mpsc::error::TryRecvError> for TryRecvError {
    fn from(value: mpsc::error::TryRecvError) -> Self {
        match value {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Closed,
        }
    }
}

fn expanded_statement(conn: &Connection, stmt: &Statement) -> rusqlite::Result<Option<String>> {
    Ok(match stmt {
        Statement::Simple(q) => conn.prepare(q)?.expanded_sql(),
        Statement::WithParams(q, params) => {
            let mut prepped = conn.prepare(q)?;
            for (i, param) in params.iter().enumerate() {
                prepped.raw_bind_parameter(i + 1, param)?;
            }
            prepped.expanded_sql()
        }
        Statement::WithNamedParams(q, params) => {
            let mut prepped = conn.prepare(q)?;
            for (k, v) in params.iter() {
                let idx = match prepped.parameter_index(k)? {
                    Some(idx) => idx,
                    None => continue,
                };
                prepped.raw_bind_parameter(idx, v)?;
            }
            prepped.expanded_sql()
        }
    })
}

async fn expand_sql(
    agent: &Agent,
    stmt: &Statement,
) -> Result<String, (StatusCode, CompactString)> {
    match agent.pool().read().await {
        Ok(conn) => match expanded_statement(&conn, stmt) {
            Ok(Some(stmt)) => match normalize_sql(&stmt) {
                Ok(stmt) => Ok(stmt),
                Err(e) => Err((StatusCode::BAD_REQUEST, e.to_compact_string())),
            },
            Ok(None) => Err((
                StatusCode::BAD_REQUEST,
                "could not expand statement's sql w/ params".into(),
            )),
            Err(e) => Err((StatusCode::BAD_REQUEST, e.to_compact_string())),
        },
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_compact_string())),
    }
}

pub type MatcherCache = Arc<TokioRwLock<HashMap<String, Uuid>>>;

pub async fn api_v1_subs(
    Extension(agent): Extension<Agent>,
    Extension(subscription_cache): Extension<MatcherCache>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let stmt = match expand_sql(&agent, &stmt).await {
        Ok(stmt) => stmt,
        Err((status, e)) => {
            return hyper::Response::builder()
                .status(status)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(e))
                        .expect("could not serialize queries stream error")
                        .into(),
                )
                .expect("could not build error response");
        }
    };

    let matcher_id = { subscription_cache.read().await.get(&stmt).cloned() };

    if let Some(matcher_id) = matcher_id {
        let contains = { agent.matchers().read().contains_key(&matcher_id) };
        if contains {
            info!("reusing matcher id {matcher_id}");
            return sub_by_id(agent, matcher_id).await;
        } else {
            subscription_cache.write().await.remove(&stmt);
        }
    }

    let conn = match agent.pool().dedicated().await {
        Ok(conn) => conn,
        Err(e) => {
            return hyper::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(e.to_compact_string()))
                        .expect("could not serialize queries stream error")
                        .into(),
                )
                .expect("could not build error response")
        }
    };

    let (tx, body) = hyper::Body::channel();

    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (data_tx, data_rx) = mpsc::channel(512);
    let (change_tx, change_rx) = broadcast::channel(10240);

    let matcher_id = Uuid::new_v4();
    let cancel = CancellationToken::new();

    let matcher = match block_in_place(|| {
        Matcher::new(
            matcher_id,
            &agent.schema().read(),
            conn,
            data_tx.clone(),
            change_tx,
            &stmt,
            cancel.clone(),
        )
    }) {
        Ok(m) => m,
        Err(e) => {
            return hyper::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(e.to_compact_string()))
                        .expect("could not serialize queries stream error")
                        .into(),
                )
                .expect("could not build error response")
        }
    };

    {
        agent.matchers().write().insert(matcher_id, matcher.clone());
        subscription_cache.write().await.insert(stmt, matcher_id);
    }

    tokio::spawn(process_sub_channel(
        agent.clone(),
        matcher_id,
        tx,
        data_rx,
        change_rx,
        matcher.cmd_tx().clone(),
        cancel.clone(),
    ));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", matcher_id.to_string())
        .body(body)
        .expect("could not generate ok http response for query request")
}

#[cfg(test)]
mod tests {
    use arc_swap::ArcSwap;
    use corro_types::{actor::ActorId, agent::SplitPool, config::Config, pubsub::ChangeType};
    use http_body::Body;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use crate::{
        agent::migrate,
        api::client::{api_v1_db_schema, api_v1_transactions},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_v1_subs() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let pool = SplitPool::create(dir.path().join("./test.sqlite"), tripwire.clone()).await?;

        {
            let mut conn = pool.write_priority().await?;
            migrate(&mut conn)?;
        }

        let (tx_bcast, _rx_bcast) = mpsc::channel(1);
        let (tx_apply, _rx_apply) = mpsc::channel(1);

        let agent = Agent::new(corro_types::agent::AgentConfig {
            actor_id: ActorId(Uuid::new_v4()),
            pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .db_path(dir.path().join("corrosion.db").display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .api_addr("127.0.0.1:8080".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: "127.0.0.1:0".parse().unwrap(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            tx_bcast,
            tx_apply,
            schema: Default::default(),
            tripwire,
        });

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        println!("schema done");

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::Json(vec![
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id".into(), "service-name".into()],
                ),
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-2".into(), "service-name-2".into()],
                ),
            ]),
        )
        .await;

        println!("tx 1 done");

        // println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 2);

        let res = api_v1_subs(
            Extension(agent.clone()),
            Extension(Default::default()),
            axum::Json(Statement::Simple("select * from tests".into())),
        )
        .await
        .into_response();

        assert_eq!(res.status(), StatusCode::OK);

        let (status_code, _) = api_v1_transactions(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(
                "insert into tests (id, text) values (?,?)".into(),
                vec!["service-id-3".into(), "service-name-3".into()],
            )]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        let mut rows = RowsIter {
            body: res.into_body(),
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            done: false,
        };

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Columns(vec!["id".into(), "text".into()])
        );

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Row(1, vec!["service-id".into(), "service-name".into()])
        );

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Row(2, vec!["service-id-2".into(), "service-name-2".into()])
        );

        assert!(matches!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::EndOfQuery { .. }
        ));

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                3,
                vec!["service-id-3".into(), "service-name-3".into()]
            )
        );

        let (status_code, _) = api_v1_transactions(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(
                "insert into tests (id, text) values (?,?)".into(),
                vec!["service-id-4".into(), "service-name-4".into()],
            )]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                4,
                vec!["service-id-4".into(), "service-name-4".into()]
            )
        );

        Ok(())
    }

    struct RowsIter {
        body: axum::body::BoxBody,
        codec: LinesCodec,
        buf: BytesMut,
        done: bool,
    }

    impl RowsIter {
        async fn recv(&mut self) -> Option<eyre::Result<QueryEvent>> {
            if self.done {
                return None;
            }
            loop {
                loop {
                    match self.codec.decode(&mut self.buf) {
                        Ok(Some(line)) => match serde_json::from_str(&line) {
                            Ok(res) => return Some(Ok(res)),
                            Err(e) => {
                                self.done = true;
                                return Some(Err(e.into()));
                            }
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e.into()));
                        }
                    }
                }
                let bytes_res = self.body.data().await;
                match bytes_res {
                    Some(Ok(b)) => {
                        // debug!("read {} bytes", b.len());
                        self.buf.extend_from_slice(&b)
                    }
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(e.into()));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }
        }
    }
}
