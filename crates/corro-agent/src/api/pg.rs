use std::sync::Arc;

use async_trait::async_trait;
use corro_types::sqlite::SqlitePool;
use futures::stream;
use futures::Stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{
    DataRowEncoder, DescribeResponse, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::api::{ClientInfo, MakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use rusqlite::Rows;
use rusqlite::{types::ValueRef, Statement, ToSql};
use tokio::net::TcpListener;
use tracing::debug;
use tracing::info;

pub struct SqliteBackend {
    pool: SqlitePool,
    portal_store: Arc<MemPortalStore<String>>,
    query_parser: Arc<NoopQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for SqliteBackend {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.pool.get().await.map_err(PgApiError::from)?;
        debug!("got pooled conn for simple query: {query}");

        if query.to_uppercase().starts_with("SELECT") {
            let mut stmt = conn.prepare(query).map_err(PgApiError::from)?;
            let header = Arc::new(row_desc_from_stmt(&stmt, &Format::UnifiedText)?);
            stmt.query(())
                .map(|rows| {
                    let s = encode_row_data(rows, header.clone());
                    vec![Response::Query(QueryResponse::new(header, s))]
                })
                .map_err(PgApiError::from)
                .map_err(PgWireError::from)
        } else {
            conn.execute(query, ())
                .map(|affected_rows| {
                    vec![Response::Execution(Tag::new_for_execution(
                        "OK",
                        Some(affected_rows),
                    ))]
                })
                .map_err(PgApiError::from)
                .map_err(PgWireError::from)
        }
    }
}

fn name_to_type(name: &str) -> PgWireResult<Type> {
    match name.to_uppercase().as_ref() {
        "INT" | "INTEGER" | "BIGINT" => Ok(Type::INT8),
        "VARCHAR" => Ok(Type::VARCHAR),
        "TEXT" => Ok(Type::TEXT),
        "BINARY" => Ok(Type::BYTEA),
        "FLOAT" => Ok(Type::FLOAT8),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unsupported data type: {name}"),
        )))),
    }
}

fn row_desc_from_stmt(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    stmt.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let field_type = name_to_type(
                col.decl_type()
                    .ok_or_else(|| PgApiError::UnknownColumnType(col.name().to_owned()))?,
            )?;
            Ok(FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                field_type,
                format.format_for(idx),
            ))
        })
        .collect()
}

fn encode_row_data(
    mut rows: Rows,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();
    let ncols = schema.len();

    'outer: while let Ok(Some(row)) = rows.next() {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for idx in 0..ncols {
            let data = match row.get_ref(idx) {
                Ok(data) => data,
                Err(e) => {
                    results.push(Err(PgApiError::Rusqlite(e).into()));
                    continue 'outer;
                }
            };
            match data {
                ValueRef::Null => {
                    if let Err(e) = encoder.encode_field(&None::<i8>) {
                        results.push(Err(e));
                        continue 'outer;
                    }
                }
                ValueRef::Integer(i) => {
                    if let Err(e) = encoder.encode_field(&i) {
                        results.push(Err(e));
                        continue 'outer;
                    }
                }
                ValueRef::Real(f) => {
                    if let Err(e) = encoder.encode_field(&f) {
                        results.push(Err(e));
                        continue 'outer;
                    }
                }
                ValueRef::Text(t) => {
                    if let Err(e) = encoder.encode_field(&String::from_utf8_lossy(t).as_ref()) {
                        results.push(Err(e));
                        continue 'outer;
                    }
                }
                ValueRef::Blob(b) => {
                    if let Err(e) = encoder.encode_field(&b) {
                        results.push(Err(e));
                        continue 'outer;
                    }
                }
            }
        }

        results.push(encoder.finish());
    }

    stream::iter(results.into_iter())
}

#[derive(Debug, thiserror::Error)]
enum PgApiError {
    #[error(transparent)]
    PoolCheckout(#[from] bb8::RunError<bb8_rusqlite::Error>),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("parameter type unsupported: {0}")]
    ParamtererTypeUnsupported(Type),
    #[error("parameter expected at index {0}")]
    ParameterExpected(usize),
    #[error("unknown column type for column '{0}'")]
    UnknownColumnType(String),
}

impl From<PgApiError> for PgWireError {
    fn from(val: PgApiError) -> Self {
        PgWireError::ApiError(Box::new(val))
    }
}

fn get_params(portal: &Portal<String>) -> PgWireResult<Vec<Box<dyn ToSql>>> {
    let mut results = Vec::with_capacity(portal.parameter_len());
    for i in 0..portal.parameter_len() {
        let param_type = portal
            .statement()
            .parameter_types()
            .get(i)
            .ok_or(PgApiError::ParameterExpected(i))?;
        // we only support a small amount of types for demo
        match param_type {
            &Type::BOOL => {
                let param = portal.parameter::<bool>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT2 => {
                let param = portal.parameter::<i16>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT4 => {
                let param = portal.parameter::<i32>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::INT8 => {
                let param = portal.parameter::<i64>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::TEXT | &Type::VARCHAR => {
                let param = portal.parameter::<String>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT4 => {
                let param = portal.parameter::<f32>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            &Type::FLOAT8 => {
                let param = portal.parameter::<f64>(i)?;
                results.push(Box::new(param) as Box<dyn ToSql>);
            }
            t => return Err(PgApiError::ParamtererTypeUnsupported(t.clone()).into()),
        }
    }

    Ok(results)
}

#[async_trait]
impl ExtendedQueryHandler for SqliteBackend {
    type Statement = String;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.pool.get().await.map_err(PgApiError::from)?;
        debug!("got pooled conn");
        let query = portal.statement().statement();
        debug!("query: {query:?}");
        let mut stmt = conn.prepare_cached(query).map_err(PgApiError::from)?;
        let params = get_params(portal)?;
        let params_ref = params
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn rusqlite::ToSql>>();

        if query.to_uppercase().starts_with("SELECT") {
            let header = Arc::new(row_desc_from_stmt(&stmt, portal.result_column_format())?);
            stmt.query::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|rows| {
                    let s = encode_row_data(rows, header.clone());
                    Response::Query(QueryResponse::new(header, s))
                })
                .map_err(PgApiError::from)
                .map_err(PgWireError::from)
        } else {
            stmt.execute::<&[&dyn rusqlite::ToSql]>(params_ref.as_ref())
                .map(|affected_rows| {
                    Response::Execution(Tag::new_for_execution("OK", Some(affected_rows)))
                })
                .map_err(PgApiError::from)
                .map_err(PgWireError::from)
        }
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.pool.get().await.map_err(PgApiError::from)?;
        match target {
            StatementOrPortal::Statement(stmt) => {
                let param_types = Some(stmt.parameter_types().clone());
                let stmt = conn
                    .prepare_cached(stmt.statement())
                    .map_err(PgApiError::from)?;
                row_desc_from_stmt(&stmt, &Format::UnifiedBinary)
                    .map(|fields| DescribeResponse::new(param_types, fields))
            }
            StatementOrPortal::Portal(portal) => {
                let stmt = conn
                    .prepare_cached(portal.statement().statement())
                    .map_err(PgApiError::from)?;
                row_desc_from_stmt(&stmt, portal.result_column_format())
                    .map(|fields| DescribeResponse::new(None, fields))
            }
        }
    }
}

/// The parent handler that creates a handler instance for each incoming
/// connection.
struct MakeSqliteBackend {
    pool: SqlitePool,
    query_parser: Arc<NoopQueryParser>,
}

impl MakeSqliteBackend {
    fn new(pool: SqlitePool) -> MakeSqliteBackend {
        MakeSqliteBackend {
            pool,
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

impl MakeHandler for MakeSqliteBackend {
    type Handler = Arc<SqliteBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(SqliteBackend {
            pool: self.pool.clone(),
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: self.query_parser.clone(),
        })
    }
}

pub async fn start(listener: TcpListener, pool: SqlitePool) {
    let authenticator = Arc::new(NoopStartupHandler);
    let processor = Arc::new(MakeSqliteBackend::new(pool));

    info!(
        "Listening to {} for the pg wire protocol",
        listener.local_addr().unwrap()
    );
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let processor_ref = processor.make();
        tokio::spawn(process_socket(
            incoming_socket.0,
            None,
            authenticator.clone(),
            processor_ref.clone(),
            processor_ref,
        ));
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     use tokio::{net::TcpListener, sync::mpsc::channel};
//     // use corro_types::{Operation, OperationV1, Ops, Organization};
//     // use spawn::wait_for_all_pending_handles;
//     use tokio_postgres::NoTls;

//     use crate::{CrConnManager, CrSqlite};
//     // use tripwire::Tripwire;

//     // use crate::agent::tests::launch_test_agent_w_builder;

//     #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
//     async fn blah() -> eyre::Result<()> {
//         _ = tracing_subscriber::fmt::try_init();
//         let dir = tempfile::tempdir()?;

//         let pool = bb8::Pool::builder()
//             .max_size(1)
//             .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

//         let listener = TcpListener::bind("127.0.0.1:0").await?;
//         let pg_addr = listener.local_addr()?;

//         let (tx_bcast, _rx_bcast) = channel(128);
//         tokio::spawn(start(listener, pool, tx_bcast));

//         println!("connecting to pg... {pg_addr}");
//         let (mut pg, connection) = tokio_postgres::connect(
//             format!("host={} port={}", pg_addr.ip(), pg_addr.port()).as_str(),
//             NoTls,
//         )
//         .await?;

//         tokio::spawn(async move {
//             if let Err(e) = connection.await {
//                 eprintln!("connection error: {e}");
//             }
//         });

//         let rows = {
//             let tx = pg.transaction().await?;
//             let rows = tx.query("SELECT * FROM sqlite_master;", &[]).await?;
//             tx.commit().await?;
//             rows
//         };

//         println!("rows: {rows:?}");

//         Ok(())
//     }
// }
