use std::{collections::HashMap, io::Write, sync::Arc, task::Poll, time::Instant};

use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::{format_compact, ToCompactString};
use corro_types::{
    agent::Agent,
    api::{QueryEvent, Statement},
    change::SqliteValue,
    pubsub::{Matcher, MatcherError, MatcherHandle, NormalizeStatementError},
    sqlite::SqlitePoolError,
};
use futures::{future::poll_fn, ready};
use rusqlite::Connection;
use tokio::{
    sync::{broadcast, mpsc, RwLock as TokioRwLock},
    task::block_in_place,
};
use tracing::{debug, error, warn};
use uuid::Uuid;

pub async fn api_v1_sub_by_id(
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<MatcherBroadcastCache>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> impl IntoResponse {
    sub_by_id(agent, id, &bcast_cache).await
}

async fn sub_by_id(
    agent: Agent,
    id: Uuid,
    bcast_cache: &MatcherBroadcastCache,
) -> hyper::Response<hyper::Body> {
    let (matcher, rx) = match bcast_cache.read().await.get(&id).and_then(|tx| {
        agent
            .matchers()
            .read()
            .get(&id)
            .cloned()
            .and_then(|matcher| (tx.receiver_count() > 0).then(|| (matcher, tx.subscribe())))
    }) {
        Some(matcher_rx) => matcher_rx,
        None => {
            // ensure this goes!
            bcast_cache.write().await.remove(&id);
            agent.matchers().write().remove(&id);

            return hyper::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(format_compact!(
                        "could not find subscription with id {id}"
                    )))
                    .expect("could not serialize queries stream error")
                    .into(),
                )
                .expect("could not build error response");
        }
    };

    let (evt_tx, evt_rx) = mpsc::channel(512);

    tokio::spawn(catch_up_sub(agent, matcher, rx, evt_tx));

    let (tx, body) = hyper::Body::channel();

    tokio::spawn(forward_bytes_to_body_sender(evt_rx, tx));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", id.to_string())
        .body(body)
        .expect("could not build query response body")
}

fn make_query_event_bytes(buf: &mut BytesMut, query_evt: QueryEvent) -> serde_json::Result<Bytes> {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, &query_evt)?;

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    Ok(buf.split().freeze())
}

async fn process_sub_channel(
    agent: Agent,
    id: Uuid,
    tx: broadcast::Sender<Bytes>,
    mut evt_rx: mpsc::Receiver<QueryEvent>,
) {
    let mut buf = BytesMut::new();

    while let Some(query_evt) = evt_rx.recv().await {
        let send_res = match make_query_event_bytes(&mut buf, query_evt) {
            Ok(b) => tx.send(b),
            Err(e) => {
                _ = tx.send(error_to_query_event_bytes(&mut buf, e));
                break;
            }
        };

        if let Err(_e) = send_res {
            debug!("no more active receivers for subscription");
            break;
        }
    }

    debug!("subscription query channel done");

    agent.matchers().write().remove(&id);
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

async fn expand_sql(agent: &Agent, stmt: &Statement) -> Result<String, MatcherUpsertError> {
    let conn = agent.pool().read().await?;
    expanded_statement(&conn, stmt)?.ok_or(MatcherUpsertError::CouldNotExpand)
}

#[derive(Debug, thiserror::Error)]
pub enum MatcherUpsertError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    PoolDedicated(#[from] corro_types::sqlite::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("could not expand sql statement")]
    CouldNotExpand,
    #[error(transparent)]
    NormalizeStatement(#[from] NormalizeStatementError),
    #[error(transparent)]
    Matcher(#[from] MatcherError),
}

impl MatcherUpsertError {
    fn status_code(&self) -> StatusCode {
        match self {
            MatcherUpsertError::Pool(_)
            | MatcherUpsertError::PoolDedicated(_)
            | MatcherUpsertError::CouldNotExpand => StatusCode::INTERNAL_SERVER_ERROR,
            MatcherUpsertError::Sqlite(_)
            | MatcherUpsertError::NormalizeStatement(_)
            | MatcherUpsertError::Matcher(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<MatcherUpsertError> for hyper::Response<hyper::Body> {
    fn from(value: MatcherUpsertError) -> Self {
        hyper::Response::builder()
            .status(value.status_code())
            .body(
                serde_json::to_vec(&QueryEvent::Error(value.to_compact_string()))
                    .expect("could not serialize queries stream error")
                    .into(),
            )
            .expect("could not build error response")
    }
}

pub type MatcherIdCache = Arc<TokioRwLock<HashMap<String, Uuid>>>;
pub type MatcherBroadcastCache = Arc<TokioRwLock<HashMap<Uuid, broadcast::Sender<Bytes>>>>;

#[derive(Debug, thiserror::Error)]
pub enum CatchUpError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Send(#[from] mpsc::error::SendError<Bytes>),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

fn error_to_query_event_bytes<E: ToCompactString>(buf: &mut BytesMut, e: E) -> Bytes {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, &QueryEvent::Error(e.to_compact_string()))
            .expect("could not write QueryEvent::Error to buffer");

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    buf.split().freeze()
}

pub async fn catch_up_sub(
    agent: Agent,
    matcher: MatcherHandle,
    mut rx: broadcast::Receiver<Bytes>,
    tx: mpsc::Sender<Bytes>,
) -> eyre::Result<()> {
    {
        let mut buf = BytesMut::new();

        let pool = agent.pool().dedicated_pool();

        let conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                tx.send(error_to_query_event_bytes(&mut buf, e)).await?;
                return Ok(());
            }
        };

        let res = block_in_place(|| {
            let mut query_cols = vec![];
            for i in 0..(matcher.parsed_columns().len()) {
                query_cols.push(format!("col_{i}"));
            }
            let mut prepped = conn.prepare_cached(&format!(
                "SELECT __corro_rowid,{} FROM {}",
                query_cols.join(","),
                matcher.table_name()
            ))?;
            let col_count = prepped.column_count();

            tx.blocking_send(make_query_event_bytes(
                &mut buf,
                QueryEvent::Columns(matcher.col_names().to_vec()),
            )?)?;

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

                tx.blocking_send(make_query_event_bytes(
                    &mut buf,
                    QueryEvent::Row(rowid, cells),
                )?)?;
            }

            tx.blocking_send(make_query_event_bytes(
                &mut buf,
                QueryEvent::EndOfQuery {
                    time: elapsed.as_secs_f64(),
                },
            )?)?;

            Ok::<_, CatchUpError>(())
        });

        if let Err(CatchUpError::Sqlite(e)) = res {
            _ = tx.send(error_to_query_event_bytes(&mut buf, e)).await;
        }
    }

    // FIXME: if this is a query watching a frequently changing dataset, it will
    //        always lag! should explore receiving asynchronously before making the query.
    //        It's also possible we've missed messages entirely or are sending dups?
    loop {
        match rx.recv().await {
            Ok(query_evt) => {
                if let Err(_e) = tx.send(query_evt).await {
                    debug!("subscriber must be done, can't send back query event");
                    break;
                }
            }
            Err(e) => match e {
                broadcast::error::RecvError::Closed => {
                    warn!("broadcast is done, that's a little weird?");
                    break;
                }
                broadcast::error::RecvError::Lagged(count) => {
                    debug!("subscriber lagged, skipped {count} messages");
                    let mut buf = BytesMut::new();
                    tx.send(error_to_query_event_bytes(
                        &mut buf,
                        format!("too slow, skipped {count} messages"),
                    ))
                    .await?;
                    break;
                }
            },
        }
    }

    Ok(())
}

pub async fn upsert_sub(
    agent: &Agent,
    cache: &MatcherIdCache,
    bcast_cache: &MatcherBroadcastCache,
    stmt: Statement,
    tx: mpsc::Sender<Bytes>,
) -> Result<Uuid, MatcherUpsertError> {
    let stmt = expand_sql(agent, &stmt).await?;

    let mut cache_write = cache.write().await;
    let mut bcast_write = bcast_cache.write().await;

    let maybe_matcher = cache_write
        .get(&stmt)
        .and_then(|id| bcast_write.get(id).map(|sender| (*id, sender)));

    if let Some((matcher_id, sender)) = maybe_matcher {
        let maybe_matcher = (sender.receiver_count() > 0)
            .then(|| agent.matchers().read().get(&matcher_id).cloned())
            .flatten();
        if let Some(matcher) = maybe_matcher {
            let rx = sender.subscribe();
            tokio::spawn(catch_up_sub(agent.clone(), matcher, rx, tx));
            return Ok(matcher_id);
        } else {
            cache_write.remove(&stmt);
            bcast_write.remove(&matcher_id);
        }
    }

    let conn = agent.pool().dedicated().await?;

    let (evt_tx, evt_rx) = mpsc::channel(512);

    let matcher_id = Uuid::new_v4();

    let matcher = Matcher::create(matcher_id, &agent.schema().read(), conn, evt_tx, &stmt)?;

    let (sub_tx, mut sub_rx) = broadcast::channel(10240);

    cache_write.insert(stmt, matcher_id);
    bcast_write.insert(matcher_id, sub_tx.clone());

    {
        agent.matchers().write().insert(matcher_id, matcher);
    }

    tokio::spawn(async move {
        loop {
            match sub_rx.recv().await {
                Ok(b) => {
                    if let Err(_e) = tx.send(b).await {
                        error!("could not forward subscription query event to receiver, channel is closed!");
                        return;
                    }
                }
                Err(e) => {
                    error!("could not receive subscription query event: {e}");
                    let mut buf = BytesMut::new();
                    if let Err(_e) = tx.send(error_to_query_event_bytes(&mut buf, e)).await {
                        debug!("could not send back subscription receive error! channel is closed");
                    }
                    return;
                }
            }
        }
    });

    tokio::spawn(process_sub_channel(
        agent.clone(),
        matcher_id,
        sub_tx,
        evt_rx,
    ));

    Ok(matcher_id)
}

pub async fn api_v1_subs(
    Extension(agent): Extension<Agent>,
    Extension(sub_cache): Extension<MatcherIdCache>,
    Extension(bcast_cache): Extension<MatcherBroadcastCache>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (tx, body) = hyper::Body::channel();
    let (forward_tx, forward_rx) = mpsc::channel(512);

    let matcher_id = match upsert_sub(&agent, &sub_cache, &bcast_cache, stmt, forward_tx).await {
        Ok(id) => id,
        Err(e) => return hyper::Response::<hyper::Body>::from(e),
    };

    tokio::spawn(forward_bytes_to_body_sender(forward_rx, tx));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", matcher_id.to_string())
        .body(body)
        .expect("could not generate ok http response for query request")
}

async fn forward_bytes_to_body_sender(mut rx: mpsc::Receiver<Bytes>, mut tx: hyper::body::Sender) {
    loop {
        let res = {
            poll_fn(|cx| {
                ready!(tx.poll_ready(cx))?;
                Poll::Ready(Ok::<_, hyper::Error>(ready!(rx.poll_recv(cx))))
            })
            .await
        };
        match res {
            Ok(Some(b)) => {
                if let Err(e) = tx.send_data(b).await {
                    error!("could not send query event data through body: {e}");
                    break;
                }
            }
            Ok(None) => {
                // done...
                break;
            }
            Err(e) => {
                error!("body was not ready anymore: {e}");
                break;
            }
        }
    }
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
                match self.codec.decode(&mut self.buf) {
                    Ok(Some(line)) => match serde_json::from_str(&line) {
                        Ok(res) => return Some(Ok(res)),
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e.into()));
                        }
                    },
                    Ok(None) => {
                        // fall through
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e.into()));
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
