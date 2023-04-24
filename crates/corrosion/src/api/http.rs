use std::{cmp, time::Instant};

use axum::Extension;
use corro_types::api::{RqliteResponse, RqliteResult, Statement};
use hyper::StatusCode;
use rusqlite::{params, params_from_iter, ToSql};
use serde_json::Value;
use tokio::{sync::mpsc::Sender, task::block_in_place};
use tracing::{error, trace};

use crate::{
    actor::ActorId,
    broadcast::{BroadcastInput, Message, MessageV1},
    types::change::Change,
    Bookie, SqlitePool,
};

// #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// pub struct RqliteRequestOptions {
//     pretty: Option<bool>,
//     timings: Option<bool>,
//     transaction: Option<bool>,
//     q: Option<String>,
// }

pub async fn api_v1_db_execute(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(rw_pool): Extension<SqlitePool>,
    Extension(tx_local_bcast): Extension<Sender<BroadcastInput>>,
    Extension(actor_id): Extension<ActorId>,
    Extension(bookie): Extension<Bookie>,
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<RqliteResponse>) {
    let res: Result<_, rusqlite::Error> = {
        trace!("getting conn...");
        let mut conn = match rw_pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("could not get rw pool conn: {e}");
                return (
                    // TODO: better status code
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(RqliteResponse {
                        results: vec![RqliteResult::Error {
                            error: format!("could not acquire pool connection: {e}"),
                        }],
                        time: None,
                    }),
                );
            }
        };
        trace!("got conn");

        let start = Instant::now();
        block_in_place(move || {
            let tx = conn.transaction()?;

            let start_version: i64 = tx
                .prepare_cached("SELECT crsql_dbversion();")?
                .query_row((), |row| row.get(0))?;

            let mut total_rows_affected = 0;

            let results = statements
                .into_iter()
                .filter_map(|stmt| {
                    let start = Instant::now();
                    let res = match stmt {
                        Statement::Simple(q) => tx.execute(&q, []),
                        Statement::WithParams(params) => {
                            let mut params = params.into_iter();

                            let first = params.next();
                            match first.as_ref().and_then(|q| q.as_str()) {
                                Some(q) => tx.execute(
                                    &q,
                                    params_from_iter(params.map(|v| match v {
                                        Value::Null => rusqlite::types::Value::Null,
                                        Value::Bool(b) => {
                                            rusqlite::types::Value::Text(b.to_string())
                                        }
                                        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
                                            (Some(n), None) => rusqlite::types::Value::Integer(n),
                                            (None, Some(f)) => rusqlite::types::Value::Real(f),
                                            _ => rusqlite::types::Value::Text(n.to_string()),
                                        },
                                        Value::String(s) => rusqlite::types::Value::Text(s),
                                        Value::Array(a) => {
                                            serde_json::to_string(&a).unwrap().into()
                                        }
                                        Value::Object(o) => {
                                            serde_json::to_string(&o).unwrap().into()
                                        }
                                    })),
                                ),
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

            let last_version = bookie.last(&actor_id).unwrap_or(0);
            let version = last_version + 1;

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
                        site_id: actor_id.0.into_bytes(),
                    };
                    end_version = cmp::max(end_version, change.db_version);
                    Ok(change)
                })?;

                let changes = mapped.collect::<Result<Vec<Change>, rusqlite::Error>>()?;

                let db_version = if end_version > start_version {
                    Some(end_version)
                } else {
                    None
                };

                tx.prepare_cached(
                    r#"
                    INSERT INTO __corro_bookkeeping (actor_id, version, db_version)
                        VALUES (?, ?, ?);
                "#,
                )?
                .execute(params![actor_id.0, version, db_version])?;

                (changes, db_version)
            };

            tx.commit()?;
            let elapsed = start.elapsed();

            let msg = if !changes.is_empty() {
                bookie.add(actor_id, version, db_version);
                Some(BroadcastInput::AddBroadcast(Message::V1(
                    MessageV1::Change {
                        actor_id,
                        version,
                        changeset: changes,
                    },
                )))
            } else {
                None
            };

            Ok((results, msg, elapsed))
        })
    };

    let (results, elapsed) = match res {
        Ok((results, msg, elapsed)) => {
            if let Some(msg) = msg {
                // we've got new versions!
                tokio::spawn(async move {
                    if let Err(e) = tx_local_bcast.send(msg).await {
                        error!("could not send start + end version for broadcast: {e}");
                    }
                });
            }
            (results, elapsed)
        }
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
    use serde_json::json;
    use tokio::sync::mpsc::channel;
    use uuid::Uuid;

    use super::*;

    use crate::{apply_schema, CrConnManager};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn rqlite_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir()?;

        let pool = bb8::Pool::builder()
            .max_size(1)
            .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

        apply_schema(&pool, "../../schema").await?;

        let (tx, mut rx) = channel(1);

        let actor_id = ActorId(Uuid::new_v4());

        let bookie = Bookie::default();

        let (status_code, body) = api_v1_db_execute(
            Extension(pool.clone()),
            Extension(tx.clone()),
            Extension(actor_id),
            Extension(bookie.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "insert into consul_services (id, name, address) values (?,?,?)".into(),
                json!("service-id"),
                json!("service-name"),
                json!("blah"),
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

        assert_eq!(bookie.last(&actor_id), Some(1));

        let (status_code, body) = api_v1_db_execute(
            Extension(pool.clone()),
            Extension(tx),
            Extension(actor_id),
            Extension(bookie.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "update consul_services SET name = ? where id = ?".into(),
                json!("service-name"),
                json!("service-id"),
            ])]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        // no actual changes!
        assert!(rx.recv().await.is_none());

        Ok(())
    }
}
