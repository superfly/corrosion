pub mod proto;
pub mod proto_ext;
pub mod sql_state;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use compact_str::CompactString;
use corro_types::{agent::Agent, change::SqliteValue, config::PgConfig};
use futures::{Sink, SinkExt, StreamExt};
use pgwire::{
    api::{
        results::{DataRowEncoder, FieldFormat, FieldInfo, Tag},
        ClientInfo, ClientInfoHolder,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        data::{FieldDescription, ParameterDescription, RowDescription},
        extendedquery::{CloseComplete, ParseComplete},
        response::{ReadyForQuery, READY_STATUS_IDLE},
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::PgWireMessageServerCodec,
};
use postgres_types::{FromSql, Oid, Type};
use rusqlite::{types::ValueRef, Statement};
use tokio::{net::TcpListener, sync::mpsc::channel, task::block_in_place};
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, info, warn};

use crate::{
    proto::{Bind, ConnectionCodec, ProtocolError},
    sql_state::SqlState,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

struct PgServer {
    local_addr: SocketAddr,
}

async fn start(agent: Agent, pg: PgConfig) -> Result<PgServer, BoxError> {
    let mut server = TcpListener::bind(pg.bind_addr).await?;
    let local_addr = server.local_addr()?;

    tokio::spawn(async move {
        loop {
            let (conn, remote_addr) = server.accept().await?;
            info!("accepted a conn, addr: {remote_addr}");

            let agent = agent.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(
                    conn,
                    PgWireMessageServerCodec::new(ClientInfoHolder::new(remote_addr, false)),
                );

                let msg = framed.next().await.unwrap()?;
                println!("msg: {msg:?}");

                match msg {
                    PgWireFrontendMessage::Startup(startup) => {
                        info!("received startup message: {startup:?}");
                        println!("huh...");
                    }
                    _ => {
                        framed
                            .send(PgWireBackendMessage::ErrorResponse(
                                ErrorInfo::new(
                                    "FATAL".into(),
                                    SqlState::PROTOCOL_VIOLATION.code().into(),
                                    "expected startup message".into(),
                                )
                                .into(),
                            ))
                            .await?;
                        return Ok(());
                    }
                }

                framed.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);

                framed
                    .feed(PgWireBackendMessage::Authentication(
                        pgwire::messages::startup::Authentication::Ok,
                    ))
                    .await?;
                framed
                    .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        READY_STATUS_IDLE,
                    )))
                    .await?;

                framed.flush().await?;

                println!("sent auth ok and ReadyForQuery");

                let (front_tx, mut front_rx) = channel(1024);
                let (back_tx, mut back_rx) = channel(1024);

                let (mut sink, mut stream) = framed.split();

                tokio::spawn({
                    let back_tx = back_tx.clone();
                    async move {
                        while let Some(decode_res) = stream.next().await {
                            println!("decode_res: {decode_res:?}");
                            let msg = match decode_res {
                                Ok(msg) => msg,
                                Err(PgWireError::IoError(io_error)) => {
                                    debug!("postgres io error: {io_error}");
                                    break;
                                }
                                Err(e) => {
                                    // attempt to send this...
                                    _ = back_tx.try_send((
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "FATAL".to_owned(),
                                                "XX000".to_owned(),
                                                e.to_string(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    ));
                                    break;
                                }
                            };

                            front_tx.send(msg).await?;
                        }

                        Ok::<_, BoxError>(())
                    }
                });

                tokio::spawn(async move {
                    while let Some((back, flush)) = back_rx.recv().await {
                        println!("sending: {back:?}");
                        sink.feed(back).await?;
                        if flush {
                            sink.flush().await?;
                        }
                    }
                    Ok::<_, std::io::Error>(())
                });

                block_in_place(|| {
                    let conn = rusqlite::Connection::open_in_memory().unwrap();
                    println!("opened in-memory conn");

                    let mut prepared: HashMap<CompactString, (String, Statement, Vec<Type>)> =
                        HashMap::new();

                    let mut portals: HashMap<CompactString, (CompactString, Statement)> =
                        HashMap::new();

                    let mut row_cache: Vec<SqliteValue> = vec![];

                    'outer: while let Some(msg) = front_rx.blocking_recv() {
                        println!("msg: {msg:?}");

                        match msg {
                            PgWireFrontendMessage::Startup(_) => {
                                back_tx.blocking_send((
                                    PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "FATAL".into(),
                                            SqlState::PROTOCOL_VIOLATION.code().into(),
                                            "unexpected startup message".into(),
                                        )
                                        .into(),
                                    ),
                                    true,
                                ))?;
                                continue;
                            }
                            PgWireFrontendMessage::Parse(parse) => {
                                let prepped = match conn.prepare(parse.query()) {
                                    Ok(prepped) => prepped,
                                    Err(e) => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".to_owned(),
                                                    "XX000".to_owned(),
                                                    e.to_string(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                };

                                prepared.insert(
                                    parse.name().as_deref().unwrap_or("").into(),
                                    (
                                        parse.query().clone(),
                                        prepped,
                                        parse
                                            .type_oids()
                                            .iter()
                                            .filter_map(|oid| Type::from_oid(*oid))
                                            .collect(),
                                    ),
                                );

                                back_tx.blocking_send((
                                    PgWireBackendMessage::ParseComplete(ParseComplete::new()),
                                    true,
                                ))?;
                            }
                            PgWireFrontendMessage::Describe(desc) => {
                                let name = desc.name().as_deref().unwrap_or("");
                                match desc.target_type() {
                                    // statement
                                    b'S' => {
                                        if let Some((_, prepped, _)) = prepared.get(name) {
                                            let mut oids = vec![];
                                            let mut fields = vec![];
                                            for col in prepped.columns() {
                                                let col_type = match name_to_type(
                                                    col.decl_type().unwrap_or("text"),
                                                ) {
                                                    Ok(t) => t,
                                                    Err(e) => {
                                                        back_tx.blocking_send((
                                                            PgWireBackendMessage::ErrorResponse(
                                                                e.into(),
                                                            ),
                                                            true,
                                                        ))?;
                                                        continue 'outer;
                                                    }
                                                };
                                                oids.push(col_type.oid());
                                                fields.push(FieldInfo::new(
                                                    col.name().to_string(),
                                                    None,
                                                    None,
                                                    col_type,
                                                    FieldFormat::Text,
                                                ));
                                            }
                                            back_tx.blocking_send((
                                                PgWireBackendMessage::ParameterDescription(
                                                    ParameterDescription::new(oids),
                                                ),
                                                false,
                                            ))?;
                                            back_tx.blocking_send((
                                                PgWireBackendMessage::RowDescription(
                                                    RowDescription::new(
                                                        fields.iter().map(Into::into).collect(),
                                                    ),
                                                ),
                                                true,
                                            ))?;
                                            continue;
                                        }
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".into(),
                                                    "statement not found".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                    }
                                    // portal
                                    b'P' => {
                                        if let Some((_, prepped)) = portals.get(name) {
                                            let mut oids = vec![];
                                            let mut fields = vec![];
                                            for col in prepped.columns() {
                                                let col_type = match name_to_type(
                                                    col.decl_type().unwrap_or("text"),
                                                ) {
                                                    Ok(t) => t,
                                                    Err(e) => {
                                                        back_tx.blocking_send((
                                                            PgWireBackendMessage::ErrorResponse(
                                                                e.into(),
                                                            ),
                                                            true,
                                                        ))?;
                                                        continue 'outer;
                                                    }
                                                };
                                                oids.push(col_type.oid());
                                                fields.push(FieldInfo::new(
                                                    col.name().to_string(),
                                                    None,
                                                    None,
                                                    col_type,
                                                    FieldFormat::Text,
                                                ));
                                            }
                                            back_tx.blocking_send((
                                                PgWireBackendMessage::RowDescription(
                                                    RowDescription::new(
                                                        fields.iter().map(Into::into).collect(),
                                                    ),
                                                ),
                                                true,
                                            ))?;
                                            continue;
                                        }
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".into(),
                                                    "portal not found".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                    }
                                    _ => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "FATAL".into(),
                                                    SqlState::PROTOCOL_VIOLATION.code().into(),
                                                    "unexpected describe type".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                }
                            }
                            PgWireFrontendMessage::Bind(bind) => {
                                let portal_name = bind
                                    .portal_name()
                                    .as_deref()
                                    .map(CompactString::from)
                                    .unwrap_or_default();

                                let stmt_name = bind.statement_name().as_deref().unwrap_or("");

                                let (sql, _, param_types) = match prepared.get(stmt_name) {
                                    None => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".to_owned(),
                                                    "XX000".to_owned(),
                                                    "statement not found".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                    Some(stmt) => stmt,
                                };

                                let mut prepped = match conn.prepare(sql) {
                                    Ok(prepped) => prepped,
                                    Err(e) => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".to_owned(),
                                                    "XX000".to_owned(),
                                                    e.to_string(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                };

                                for (i, param) in bind.parameters().iter().enumerate() {
                                    let idx = i + 1;
                                    let b = match param {
                                        None => {
                                            if let Err(e) = prepped
                                                .raw_bind_parameter(idx, rusqlite::types::Null)
                                            {
                                                back_tx.blocking_send((
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                ))?;
                                                continue 'outer;
                                            }
                                            continue;
                                        }
                                        Some(b) => b,
                                    };

                                    match param_types.get(i) {
                                        None => {
                                            back_tx.blocking_send((
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        "missing parameter type".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            ))?;
                                            continue 'outer;
                                        }
                                        Some(param_type) => match param_type {
                                            &Type::BOOL => {
                                                let value: bool =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::INT2 => {
                                                let value: i16 =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::INT4 => {
                                                let value: i32 =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::INT8 => {
                                                let value: i64 =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::TEXT | &Type::VARCHAR => {
                                                let value: &str =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::FLOAT4 => {
                                                let value: f32 =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::FLOAT8 => {
                                                let value: f64 =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            &Type::BYTEA => {
                                                let value: &[u8] =
                                                    FromSql::from_sql(param_type, b.as_ref())?;
                                                prepped.raw_bind_parameter(idx, value)?;
                                            }
                                            t => {
                                                warn!("unsupported type: {t:?}");
                                                back_tx.blocking_send((
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            format!(
                                                                "unsupported type {t} at index {i}"
                                                            ),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                ))?;
                                                continue 'outer;
                                            }
                                        },
                                    }
                                }

                                portals.insert(portal_name, (stmt_name.into(), prepped));
                            }
                            PgWireFrontendMessage::Sync(_) => {
                                back_tx.blocking_send((
                                    PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                                        READY_STATUS_IDLE,
                                    )),
                                    true,
                                ))?;
                            }
                            PgWireFrontendMessage::Execute(_) => todo!(),
                            PgWireFrontendMessage::Query(query) => {
                                let mut prepped = match conn.prepare(query.query()) {
                                    Ok(prepped) => prepped,
                                    Err(e) => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".to_owned(),
                                                    "XX000".to_owned(),
                                                    e.to_string(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                };

                                let mut fields = vec![];
                                for col in prepped.columns() {
                                    let col_type =
                                        match name_to_type(col.decl_type().unwrap_or("text")) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                back_tx.blocking_send((
                                                    PgWireBackendMessage::ErrorResponse(e.into()),
                                                    true,
                                                ))?;
                                                continue 'outer;
                                            }
                                        };
                                    fields.push(FieldInfo::new(
                                        col.name().to_string(),
                                        None,
                                        None,
                                        col_type,
                                        FieldFormat::Text,
                                    ));
                                }

                                back_tx.blocking_send((
                                    PgWireBackendMessage::RowDescription(RowDescription::new(
                                        fields.iter().map(Into::into).collect(),
                                    )),
                                    true,
                                ))?;

                                let schema = Arc::new(fields);

                                let mut rows = prepped.raw_query();
                                let ncols = schema.len();

                                let mut count = 0;
                                while let Ok(Some(row)) = rows.next() {
                                    count += 1;
                                    let mut encoder = DataRowEncoder::new(schema.clone());
                                    for idx in 0..ncols {
                                        let data = row.get_ref_unwrap::<usize>(idx);
                                        match data {
                                            ValueRef::Null => {
                                                encoder.encode_field(&None::<i8>).unwrap()
                                            }
                                            ValueRef::Integer(i) => {
                                                encoder.encode_field(&i).unwrap();
                                            }
                                            ValueRef::Real(f) => {
                                                encoder.encode_field(&f).unwrap();
                                            }
                                            ValueRef::Text(t) => {
                                                encoder
                                                    .encode_field(
                                                        &String::from_utf8_lossy(t).as_ref(),
                                                    )
                                                    .unwrap();
                                            }
                                            ValueRef::Blob(b) => {
                                                encoder.encode_field(&b).unwrap();
                                            }
                                        }
                                    }
                                }

                                // TODO: figure out what kind of execution it is: SELECT, INSERT, etc.
                                back_tx.blocking_send((
                                    PgWireBackendMessage::CommandComplete(
                                        Tag::new_for_query(count).into(),
                                    ),
                                    true,
                                ))?;
                            }
                            PgWireFrontendMessage::Terminate(_) => {
                                break;
                            }

                            PgWireFrontendMessage::PasswordMessageFamily(_) => todo!(),
                            PgWireFrontendMessage::Close(close) => {
                                let name = close.name().as_deref().unwrap_or("");
                                match close.target_type() {
                                    // statement
                                    b'S' => {
                                        if let Some((_, prepped, _)) = prepared.remove(name) {
                                            portals.retain(|_, (stmt_name, _)| {
                                                stmt_name.as_str() != name
                                            });
                                            back_tx.blocking_send((
                                                PgWireBackendMessage::CloseComplete(
                                                    CloseComplete::new(),
                                                ),
                                                true,
                                            ))?;
                                            continue;
                                        }
                                        // not finding a statement is not an error
                                    }
                                    // portal
                                    b'P' => {
                                        portals.remove(name);
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::CloseComplete(
                                                CloseComplete::new(),
                                            ),
                                            true,
                                        ))?;
                                    }
                                    _ => {
                                        back_tx.blocking_send((
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "FATAL".into(),
                                                    SqlState::PROTOCOL_VIOLATION.code().into(),
                                                    "unexpected Close target_type".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        ))?;
                                        continue;
                                    }
                                }
                            }
                            PgWireFrontendMessage::Flush(_) => todo!(),
                            PgWireFrontendMessage::CopyData(_) => todo!(),
                            PgWireFrontendMessage::CopyFail(_) => todo!(),
                            PgWireFrontendMessage::CopyDone(_) => todo!(),
                        }
                    }

                    Ok::<_, BoxError>(())
                })?;

                Ok::<_, BoxError>(())
            });
        }

        info!("postgres server done");

        Ok::<_, BoxError>(())
    });

    return Ok(PgServer { local_addr });
}

fn name_to_type(name: &str) -> Result<Type, ErrorInfo> {
    match name.to_uppercase().as_ref() {
        "INT" => Ok(Type::INT8),
        "VARCHAR" => Ok(Type::VARCHAR),
        "TEXT" => Ok(Type::TEXT),
        "BINARY" => Ok(Type::BYTEA),
        "FLOAT" => Ok(Type::FLOAT8),
        _ => Err(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unsupported data type: {name}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use corro_tests::launch_test_agent;
    use tokio_postgres::NoTls;
    use tripwire::Tripwire;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pg() -> Result<(), BoxError> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let ta = launch_test_agent(|builder| builder.build(), tripwire).await?;

        let server = start(
            ta.agent.clone(),
            PgConfig {
                bind_addr: "127.0.0.1:0".parse()?,
            },
        )
        .await?;

        let conn_str = format!(
            "host={} port={} user=testuser",
            server.local_addr.ip(),
            server.local_addr.port()
        );

        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
        println!("client is ready!");
        tokio::spawn(client_conn);

        let stmt = client.prepare("SELECT 1").await?;
        println!("after prepare");

        let rows = client.query(&stmt, &[]).await?;
        println!("rows: {rows:?}");

        Ok(())
    }
}
