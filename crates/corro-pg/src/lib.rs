pub mod proto;
pub mod proto_ext;
pub mod sql_state;

use std::{collections::HashMap, net::SocketAddr};

use compact_str::CompactString;
use corro_types::{agent::Agent, change::SqliteValue, config::PgConfig};
use futures::{Sink, SinkExt, StreamExt};
use pgwire::{
    api::ClientInfoHolder,
    error::{ErrorInfo, PgWireError},
    messages::{
        response::{ReadyForQuery, READY_STATUS_IDLE},
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::PgWireMessageServerCodec,
};
use rusqlite::Statement;
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, info};

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

                match msg {
                    PgWireFrontendMessage::Startup(startup) => {
                        info!("received startup message: {startup:?}");
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

                framed
                    .send(PgWireBackendMessage::Authentication(
                        pgwire::messages::startup::Authentication::Ok,
                    ))
                    .await?;
                framed
                    .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        READY_STATUS_IDLE,
                    )))
                    .await?;

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                std::thread::spawn(move || -> Result<(), BoxError> {
                    rt.block_on(async move {
                        let conn = rusqlite::Connection::open_in_memory().unwrap();

                        let mut prepared: HashMap<CompactString, (String, Vec<_>)> = HashMap::new();

                        let mut portals: HashMap<CompactString, (CompactString, Statement)> =
                            HashMap::new();

                        let mut row_cache: Vec<SqliteValue> = vec![];

                        while let Some(decode_res) = framed.next().await {
                            let msg = match decode_res {
                                Ok(msg) => msg,
                                Err(PgWireError::IoError(io_error)) => {
                                    debug!("postgres io error: {io_error}");
                                    break;
                                }
                                // Err(ProtocolError::ParserError) => {
                                //     framed
                                //         .send(proto::ErrorResponse::new(
                                //             proto::SqlState::SyntaxError,
                                //             proto::Severity::Error,
                                //             "parsing error",
                                //         ))
                                //         .await?;
                                //     continue;
                                // }
                                Err(e) => {
                                    framed
                                        .send(PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "FATAL".to_owned(),
                                                "XX000".to_owned(),
                                                e.to_string(),
                                            )
                                            .into(),
                                        ))
                                        .await?;
                                    break;
                                }
                            };

                            match msg {
                                PgWireFrontendMessage::Startup(_) => {
                                    framed
                                        .send(PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "FATAL".into(),
                                                SqlState::PROTOCOL_VIOLATION.code().into(),
                                                "unexpected startup message".into(),
                                            )
                                            .into(),
                                        ))
                                        .await?;
                                    continue;
                                }
                                PgWireFrontendMessage::Parse(parse) => {
                                    if let Err(e) = conn.prepare_cached(parse.query()) {
                                        framed
                                            .send(PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".to_owned(),
                                                    "XX000".to_owned(),
                                                    e.to_string(),
                                                )
                                                .into(),
                                            ))
                                            .await?;
                                        continue;
                                    }

                                    prepared.insert(
                                        parse.name().as_deref().unwrap_or("").into(),
                                        (parse.query().clone(), parse.type_oids().clone()),
                                    );
                                }
                                PgWireFrontendMessage::Describe(_) => todo!(),
                                PgWireFrontendMessage::Bind(bind) => {
                                    let portal_name = bind
                                        .portal_name()
                                        .as_deref()
                                        .map(CompactString::from)
                                        .unwrap_or_default();

                                    let stmt_name = bind.statement_name().as_deref().unwrap_or("");
                                }
                                PgWireFrontendMessage::Sync(_) => todo!(),
                                PgWireFrontendMessage::Execute(_) => todo!(),
                                PgWireFrontendMessage::Query(_) => todo!(),
                                PgWireFrontendMessage::Terminate(_) => todo!(),

                                PgWireFrontendMessage::PasswordMessageFamily(_) => todo!(),
                                PgWireFrontendMessage::Close(_) => todo!(),
                                PgWireFrontendMessage::Flush(_) => todo!(),
                                PgWireFrontendMessage::CopyData(_) => todo!(),
                                PgWireFrontendMessage::CopyFail(_) => todo!(),
                                PgWireFrontendMessage::CopyDone(_) => todo!(),
                            }
                        }

                        Ok::<_, BoxError>(())
                    })
                })
                .join()
                .unwrap()?;

                Ok::<_, BoxError>(())
            });
        }

        info!("postgres server done");

        Ok::<_, BoxError>(())
    });

    return Ok(PgServer { local_addr });
}

// #[cfg(test)]
// mod tests {
//     use tokio_postgres::NoTls;

//     use super::*;

//     #[tokio::test]
//     async fn test_pg() -> Result<(), BoxError> {
//         _ = tracing_subscriber::fmt::try_init();
//         let server = TcpListener::bind("127.0.0.1:0").await?;
//         let local_addr = server.local_addr()?;

//         let conn_str = format!(
//             "host={} port={} user=testuser",
//             local_addr.ip(),
//             local_addr.port()
//         );

//         let client_task = tokio::spawn(async move {
//             let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
//             println!("client is ready!");
//             tokio::spawn(client_conn);

//             client.prepare("SELECT 1").await?;
//             Ok::<_, BoxError>(())
//         });

//         let (conn, remote_addr) = server.accept().await?;
//         println!("accepted a conn, addr: {remote_addr}");

//         let mut framed = Framed::new(conn, ConnectionCodec::new());

//         let msg = framed.next().await.unwrap()?;
//         println!("recv msg: {msg:?}");

//         framed.send(proto::AuthenticationOk).await?;
//         framed.send(proto::ReadyForQuery).await?;

//         let msg = framed.next().await.unwrap()?;
//         println!("recv msg: {msg:?}");

//         let query = if let PgWireFrontendMessage::Parse(Parse { query, .. }) = msg {
//             query
//         } else {
//             panic!("unexpected message");
//         };

//         println!("query: {query}");

//         assert!(client_task.await?.is_ok());

//         Ok(())
//     }
// }
