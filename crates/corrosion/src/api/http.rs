use std::{
    cmp,
    time::{Duration, Instant},
};

use axum::Extension;
use bb8::RunError;
use corro_types::{
    agent::Agent,
    api::{RqliteResponse, RqliteResult, Statement},
    broadcast::Timestamp,
};
use hyper::StatusCode;
use rusqlite::{params, params_from_iter, ToSql, Transaction};
use tokio::task::block_in_place;
use tracing::{error, trace};

use corro_types::{
    broadcast::{BroadcastInput, Message, MessageV1},
    change::Change,
};

use crate::agent::process_subs;

// TODO: accept a few options
// #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// pub struct RqliteRequestOptions {
//     pretty: Option<bool>,
//     timings: Option<bool>,
//     transaction: Option<bool>,
//     q: Option<String>,
// }

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("could not acquire pooled connection: {0}")]
    ConnAcquisition(#[from] RunError<bb8_rusqlite::Error>),
    #[error("rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),
}

pub async fn make_broadcastable_changes<F, T>(
    agent: &Agent,
    f: F,
) -> Result<(T, Duration), ChangeError>
where
    F: Fn(&Transaction) -> Result<T, ChangeError>,
{
    trace!("getting conn...");
    let mut conn = agent.read_write_pool().get().await?;
    trace!("got conn");

    let actor_id = agent.actor_id();

    let start = Instant::now();
    block_in_place(move || {
        let tx = conn.transaction()?;

        let start_version: i64 = tx
            .prepare_cached("SELECT crsql_dbversion();")?
            .query_row((), |row| row.get(0))?;

        let ret = f(&tx)?;

        let last_version = agent.bookie().last(&actor_id).unwrap_or(0);
        trace!("last_version: {last_version}");
        let version = last_version + 1;
        trace!("version: {version}");

        let (changes, db_version) = {
            let mut prepped = tx.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version FROM crsql_changes WHERE site_id IS NULL AND db_version > ?"#)?;

            let mut end_version = start_version;

            let mapped = prepped.query_map([start_version], |row| {
                let change = Change {
                    table: row.get(0)?,
                    pk: row.get(1)?,
                    cid: row.get(2)?,
                    val: row.get(3)?,
                    col_version: row.get(4)?,
                    db_version: row.get(5)?,
                    site_id: actor_id.to_bytes(),
                };
                end_version = cmp::max(end_version, change.db_version);
                Ok(change)
            })?;

            let changes = mapped.collect::<Result<Vec<Change>, rusqlite::Error>>()?;

            let db_version = if end_version > start_version {
                tx.prepare_cached(
                    r#"
                        INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, ts)
                            VALUES (?, ?, ?, ?);
                    "#,
                )?
                .execute(params![
                    actor_id,
                    version,
                    end_version,
                    Timestamp::from(agent.clock().new_timestamp()).to_time()
                ])?;
                Some(end_version)
            } else {
                None
            };

            (changes, db_version)
        };

        tx.commit()?;
        let elapsed = start.elapsed();

        if !changes.is_empty() {
            let ts: Timestamp = agent.clock().new_timestamp().into();
            agent.bookie().add(actor_id, version, db_version, ts);

            if let Some(db_version) = db_version {
                process_subs(agent, &changes, db_version);
            }

            let tx_bcast = agent.tx_bcast().clone();
            tokio::spawn(async move {
                if let Err(e) = tx_bcast
                    .send(BroadcastInput::AddBroadcast(Message::V1(
                        MessageV1::Change {
                            actor_id,
                            version,
                            changeset: changes,
                            ts,
                        },
                    )))
                    .await
                {
                    error!("could not send change message for broadcast: {e}");
                }
            });
        }

        Ok::<_, ChangeError>((ret, elapsed))
    })
}

const MAX_STATEMENTS_PER_REQUEST: usize = 50;

pub async fn api_v1_db_execute(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<RqliteResponse>) {
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: None,
            }),
        );
    }

    if statements.len() > MAX_STATEMENTS_PER_REQUEST {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: format!("too many statements, please restrict the number of statements per request to {MAX_STATEMENTS_PER_REQUEST}"),
                }],
                time: None,
            }),
        );
    }

    let res = make_broadcastable_changes(&agent, move |tx| {
        let mut total_rows_affected = 0;

        let results = statements
            .iter()
            .filter_map(|stmt| {
                let start = Instant::now();
                let res = match stmt {
                    Statement::Simple(q) => tx.execute(&q, []),
                    Statement::WithParams(params) => {
                        let mut params = params.into_iter();

                        let first = params.next();
                        match first.as_ref().and_then(|q| q.as_str()) {
                            Some(q) => tx.execute(&q, params_from_iter(params)),
                            None => return None,
                        }
                    }
                    Statement::WithNamedParams(q, params) => tx.execute(
                        &q,
                        params
                            .iter()
                            .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                            .collect::<Vec<(&str, &dyn ToSql)>>()
                            .as_slice(),
                    ),
                };
                Some(match res {
                    Ok(rows_affected) => {
                        total_rows_affected += rows_affected;
                        RqliteResult::Execute {
                            rows_affected,
                            time: Some(start.elapsed().as_secs_f64()),
                        }
                    }
                    Err(e) => RqliteResult::Error {
                        error: e.to_string(),
                    },
                })
            })
            .collect::<Vec<RqliteResult>>();

        Ok(results)
    })
    .await;

    let (results, elapsed) = match res {
        Ok(res) => res,
        Err(e) => {
            error!("could not execute statement(s): {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RqliteResponse {
                    results: vec![RqliteResult::Error {
                        error: e.to_string(),
                    }],
                    time: None,
                }),
            );
        }
    };

    (
        StatusCode::OK,
        axum::Json(RqliteResponse {
            results,
            time: Some(elapsed.as_secs_f64()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use corro_types::{
        actor::ActorId,
        config::Config,
        schema::{apply_schema, NormalizedSchema},
        sqlite::CrConnManager,
    };
    use tokio::sync::mpsc::{channel, error::TryRecvError};
    use ulid::Ulid;

    use super::*;

    use crate::agent::migrate;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn rqlite_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir()?;
        let schema_path = dir.path().join("schema");
        tokio::fs::create_dir_all(&schema_path).await?;

        tokio::fs::write(schema_path.join("test.sql"), corro_tests::TEST_SCHEMA).await?;

        let rw_pool = bb8::Pool::builder()
            .max_size(1)
            .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

        {
            let mut conn = rw_pool.get().await?;
            migrate(&mut conn)?;
            apply_schema(&mut conn, &[&schema_path], &NormalizedSchema::default())?;
        }

        let (tx, mut rx) = channel(1);

        let agent = Agent(Arc::new(corro_types::agent::AgentInner {
            actor_id: ActorId(Ulid::new()),
            ro_pool: bb8::Pool::builder()
                .max_size(1)
                .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite"))),
            rw_pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .base_path(dir.path().display().to_string())
                    .add_schema_path(schema_path.display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: Default::default(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            subscribers: Default::default(),
            tx_bcast: tx,
            schema: Default::default(),
        }));

        let (status_code, body) = api_v1_db_execute(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "insert into tests (id, text) values (?,?)".into(),
                "service-id".into(),
                "service-name".into(),
            ])]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        let msg = rx.recv().await.expect("not msg received on bcast channel");

        assert!(matches!(
            msg,
            BroadcastInput::AddBroadcast(Message::V1(MessageV1::Change { version: 1, .. }))
        ));

        assert_eq!(agent.bookie().last(&agent.actor_id()), Some(1));

        println!("second req...");

        let (status_code, body) = api_v1_db_execute(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "update tests SET text = ? where id = ?".into(),
                "service-name".into(),
                "service-id".into(),
            ])]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        // no actual changes!
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        Ok(())
    }
}
