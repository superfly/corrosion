pub mod sql_state;
mod vtab;

use std::{
    collections::{HashMap, VecDeque},
    future::poll_fn,
    net::SocketAddr,
    sync::Arc,
};

use bytes::Buf;
use compact_str::CompactString;
use corro_types::{
    agent::{Agent, KnownDbVersion},
    broadcast::Timestamp,
    config::PgConfig,
    schema::{parse_sql, Schema, SchemaError, SqliteType, Table},
    sqlite::SqlitePoolError,
};
use fallible_iterator::FallibleIterator;
use futures::{SinkExt, StreamExt};
use pgwire::{
    api::{
        results::{DataRowEncoder, FieldFormat, FieldInfo, Tag},
        ClientInfo, ClientInfoHolder,
    },
    error::{ErrorInfo, PgWireError},
    messages::{
        data::{ParameterDescription, RowDescription},
        extendedquery::{BindComplete, CloseComplete, ParseComplete, PortalSuspended},
        response::{
            EmptyQueryResponse, ReadyForQuery, READY_STATUS_IDLE, READY_STATUS_TRANSACTION_BLOCK,
        },
        startup::{ParameterStatus, SslRequest},
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::PgWireMessageServerCodec,
};
use postgres_types::{FromSql, Type};
use rusqlite::{named_params, types::ValueRef, vtab::eponymous_only_module, Connection, Statement};
use spawn::spawn_counted;
use sqlite3_parser::ast::{
    As, Cmd, CreateTableBody, Expr, FromClause, Id, InsertBody, Name, OneSelect, ResultColumn,
    Select, SelectTable, Stmt,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::mpsc::channel,
    task::block_in_place,
};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

use crate::{
    sql_state::SqlState,
    vtab::{pg_range::PgRangeTable, pg_type::PgTypeTable},
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct PgServer {
    pub local_addr: SocketAddr,
}

enum BackendResponse {
    Message {
        message: PgWireBackendMessage,
        flush: bool,
    },
    Flush,
}

impl From<(PgWireBackendMessage, bool)> for BackendResponse {
    fn from((message, flush): (PgWireBackendMessage, bool)) -> Self {
        Self::Message { message, flush }
    }
}

#[derive(Clone, Debug)]
struct ParsedCmd(Cmd);

impl ParsedCmd {
    pub fn returns_rows_affected(&self) -> bool {
        matches!(
            self.0,
            Cmd::Stmt(Stmt::Insert { .. })
                | Cmd::Stmt(Stmt::Update { .. })
                | Cmd::Stmt(Stmt::Delete { .. })
        )
    }
    pub fn returns_num_rows(&self) -> bool {
        matches!(
            self.0,
            Cmd::Stmt(Stmt::Select(_))
                | Cmd::Stmt(Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                })
        )
    }
    pub fn is_begin(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Begin(_, _)))
    }
    pub fn is_commit(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Commit(_)))
    }
    pub fn is_rollback(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Rollback { .. }))
    }

    fn tag(&self, rows: Option<usize>) -> Tag {
        match &self.0 {
            Cmd::Stmt(stmt) => match stmt {
                Stmt::Select(_)
                | Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                } => Tag::new_for_query(rows.unwrap_or_default()),
                Stmt::AlterTable(_, _) => Tag::new_for_execution("ALTER", rows),
                Stmt::Analyze(_) => Tag::new_for_execution("ANALYZE", rows),
                Stmt::Attach { .. } => Tag::new_for_execution("ATTACH", rows),
                Stmt::Begin(_, _) => Tag::new_for_execution("BEGIN", rows),
                Stmt::Commit(_) => Tag::new_for_execution("COMMIT", rows),
                Stmt::CreateIndex { .. }
                | Stmt::CreateTable { .. }
                | Stmt::CreateTrigger { .. }
                | Stmt::CreateView { .. }
                | Stmt::CreateVirtualTable { .. } => Tag::new_for_execution("CREATE", rows),
                Stmt::Delete { .. } => Tag::new_for_execution("DELETE", rows),
                Stmt::Detach(_) => Tag::new_for_execution("DETACH", rows),
                Stmt::DropIndex { .. }
                | Stmt::DropTable { .. }
                | Stmt::DropTrigger { .. }
                | Stmt::DropView { .. } => Tag::new_for_execution("DROP", rows),
                Stmt::Insert { .. } => Tag::new_for_execution("INSERT", rows),
                Stmt::Pragma(_, _) => Tag::new_for_execution("PRAGMA", rows),
                Stmt::Reindex { .. } => Tag::new_for_execution("REINDEX", rows),
                Stmt::Release(_) => Tag::new_for_execution("RELEASE", rows),
                Stmt::Rollback { .. } => Tag::new_for_execution("ROLLBACK", rows),
                Stmt::Savepoint(_) => Tag::new_for_execution("SAVEPOINT", rows),

                Stmt::Update { .. } => Tag::new_for_execution("UPDATE", rows),
                Stmt::Vacuum(_, _) => Tag::new_for_execution("VACUUM", rows),
            },
            _ => Tag::new_for_execution("OK", rows),
        }
    }
}

fn parse_query(sql: &str) -> Result<VecDeque<ParsedCmd>, sqlite3_parser::lexer::sql::Error> {
    let mut cmds = VecDeque::new();

    let mut parser = sqlite3_parser::lexer::sql::Parser::new(sql.as_bytes());
    loop {
        match parser.next() {
            Ok(Some(cmd)) => {
                cmds.push_back(ParsedCmd(cmd));
            }
            Ok(None) => {
                break;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(cmds)
}

enum OpenTx {
    Implicit,
    Explicit,
}

async fn peek_for_sslrequest(
    tcp_socket: &mut TcpStream,
    ssl_supported: bool,
) -> std::io::Result<bool> {
    let mut ssl = false;
    let mut buf = [0u8; SslRequest::BODY_SIZE];
    let mut buf = ReadBuf::new(&mut buf);
    loop {
        let size = poll_fn(|cx| tcp_socket.poll_peek(cx, &mut buf)).await?;
        if size == 0 {
            // the tcp_stream has ended
            return Ok(false);
        }
        if size == SslRequest::BODY_SIZE {
            let mut buf_ref = buf.filled();
            // skip first 4 bytes
            buf_ref.get_i32();
            if buf_ref.get_i32() == SslRequest::BODY_MAGIC_NUMBER {
                // the socket is sending sslrequest, read the first 8 bytes
                // skip first 8 bytes
                tcp_socket
                    .read_exact(&mut [0u8; SslRequest::BODY_SIZE])
                    .await?;
                // ssl configured
                if ssl_supported {
                    ssl = true;
                    tcp_socket.write_all(b"S").await?;
                } else {
                    tcp_socket.write_all(b"N").await?;
                }
            }

            return Ok(ssl);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PgStartError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
}

pub async fn start(
    agent: Agent,
    pg: PgConfig,
    mut tripwire: Tripwire,
) -> Result<PgServer, PgStartError> {
    let server = TcpListener::bind(pg.bind_addr).await?;
    let local_addr = server.local_addr()?;

    // let tmp_dir = tempfile::TempDir::new()?;
    // let pg_system_path = tmp_dir.path().join("pg_system.sqlite");

    tokio::spawn(async move {
        loop {
            let (mut conn, remote_addr) = match server.accept().preemptible(&mut tripwire).await {
                Outcome::Completed(res) => res?,
                Outcome::Preempted(_) => break,
            };
            info!("accepted a conn, addr: {remote_addr}");

            let agent = agent.clone();
            tokio::spawn(async move {
                conn.set_nodelay(true)?;
                let ssl = peek_for_sslrequest(&mut conn, false).await?;
                println!("SSL? {ssl}");

                let mut framed = Framed::new(
                    conn,
                    PgWireMessageServerCodec::new(ClientInfoHolder::new(remote_addr, false)),
                );

                let msg = framed.next().await.unwrap()?;
                println!("msg: {msg:?}");

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

                framed.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);

                framed
                    .feed(PgWireBackendMessage::Authentication(
                        pgwire::messages::startup::Authentication::Ok,
                    ))
                    .await?;

                framed
                    .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                        "server_version".into(),
                        "14.0.0".into(),
                    )))
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
                                    _ = back_tx.try_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "FATAL".to_owned(),
                                                    "XX000".to_owned(),
                                                    e.to_string(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    );
                                    break;
                                }
                            };

                            front_tx.send(msg).await?;
                        }

                        Ok::<_, BoxError>(())
                    }
                });

                tokio::spawn(async move {
                    while let Some(back) = back_rx.recv().await {
                        match back {
                            BackendResponse::Message { message, flush } => {
                                println!("sending: {message:?}");
                                sink.feed(message).await?;
                                if flush {
                                    sink.flush().await?;
                                }
                            }
                            BackendResponse::Flush => {
                                sink.flush().await?;
                            }
                        }
                    }
                    Ok::<_, std::io::Error>(())
                });

                block_in_place(|| {
                    let conn = agent.pool().client_dedicated().unwrap();
                    println!("opened connection");

                    conn.create_module("pg_type", eponymous_only_module::<PgTypeTable>(), None)?;
                    conn.create_module("pg_range", eponymous_only_module::<PgRangeTable>(), None)?;

                    let schema = match compute_schema(&conn) {
                        Ok(schema) => schema,
                        Err(e) => {
                            error!("could not parse schema: {e}");
                            back_tx.blocking_send(
                                (
                                    PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "FATAL".into(),
                                            "XX000".into(),
                                            "could not parse database schema".into(),
                                        )
                                        .into(),
                                    ),
                                    true,
                                )
                                    .into(),
                            )?;
                            return Ok(());
                        }
                    };

                    let mut prepared: HashMap<
                        CompactString,
                        (String, Option<ParsedCmd>, Statement, Vec<Type>),
                    > = HashMap::new();

                    let mut portals: HashMap<
                        CompactString,
                        (
                            CompactString,
                            Option<ParsedCmd>,
                            Statement,
                            Vec<FieldFormat>,
                        ),
                    > = HashMap::new();

                    let mut open_tx = None;

                    'outer: while let Some(msg) = front_rx.blocking_recv() {
                        println!("msg: {msg:?}");

                        match msg {
                            PgWireFrontendMessage::Startup(_) => {
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "FATAL".into(),
                                                SqlState::PROTOCOL_VIOLATION.code().into(),
                                                "unexpected startup message".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                continue;
                            }
                            PgWireFrontendMessage::Parse(parse) => {
                                let mut cmds = match parse_query(parse.query()) {
                                    Ok(cmds) => cmds,
                                    Err(e) => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        e.to_string(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                };

                                let parsed_cmd = match cmds.pop_front() {
                                    Some(cmd) => cmd,
                                    None => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        sql_state::SqlState::PROTOCOL_VIOLATION
                                                            .code()
                                                            .into(),
                                                        "only 1 command per Parse is allowed"
                                                            .into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                };

                                println!("parsed cmd: {parsed_cmd:?}");

                                let prepped = match conn.prepare(parse.query()) {
                                    Ok(prepped) => prepped,
                                    Err(e) => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        e.to_string(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                };

                                prepared.insert(
                                    parse.name().as_deref().unwrap_or("").into(),
                                    (
                                        parse.query().clone(),
                                        Some(parsed_cmd),
                                        prepped,
                                        parse
                                            .type_oids()
                                            .iter()
                                            .filter_map(|oid| Type::from_oid(*oid))
                                            .collect(),
                                    ),
                                );

                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ParseComplete(ParseComplete::new()),
                                        false,
                                    )
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Describe(desc) => {
                                let name = desc.name().as_deref().unwrap_or("");
                                match desc.target_type() {
                                    // statement
                                    b'S' => {
                                        if let Some((_, cmd, prepped, param_types)) =
                                            prepared.get(name)
                                        {
                                            let mut oids = vec![];
                                            let mut fields = vec![];
                                            for col in prepped.columns() {
                                                let col_type =
                                                    match name_to_type(
                                                        col.decl_type().unwrap_or("text"),
                                                    ) {
                                                        Ok(t) => t,
                                                        Err(e) => {
                                                            back_tx.blocking_send((
                                                            PgWireBackendMessage::ErrorResponse(
                                                                e.into(),
                                                            ),
                                                            true,
                                                        ).into())?;
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

                                            if param_types.len() != prepped.parameter_count() {
                                                if let Some(ParsedCmd(Cmd::Stmt(stmt))) = &cmd {
                                                    let params = parameter_types(&schema, stmt);
                                                    println!("GOT PARAMS TO OVERRIDE: {params:?}");
                                                    for param in params {
                                                        oids.push(match param {
                                                            SqliteType::Null => unreachable!(),
                                                            SqliteType::Integer => Type::INT8.oid(),
                                                            SqliteType::Real => Type::FLOAT8.oid(),
                                                            SqliteType::Text => Type::TEXT.oid(),
                                                            SqliteType::Blob => Type::BYTEA.oid(),
                                                        })
                                                    }
                                                }
                                            } else {
                                                for param in 0..prepped.parameter_count() {
                                                    // if let Some(t) = param_types.get(param) {
                                                    //     oids.push(t.oid());
                                                    // }
                                                    oids.push(
                                                        param_types
                                                            .get(param)
                                                            .map(|t| t.oid())
                                                            // this should not happen...
                                                            .unwrap_or(Type::TEXT.oid()),
                                                    );
                                                }
                                            }

                                            if !oids.is_empty() {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ParameterDescription(
                                                            ParameterDescription::new(oids),
                                                        ),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;
                                            }

                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::RowDescription(
                                                        RowDescription::new(
                                                            fields.iter().map(Into::into).collect(),
                                                        ),
                                                    ),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".into(),
                                                        "XX000".into(),
                                                        "statement not found".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                    }
                                    // portal
                                    b'P' => {
                                        if let Some((_, _, prepped, result_formats)) =
                                            portals.get(name)
                                        {
                                            let mut oids = vec![];
                                            let mut fields = vec![];
                                            for (i, col) in
                                                prepped.columns().into_iter().enumerate()
                                            {
                                                let col_type =
                                                    match name_to_type(
                                                        col.decl_type().unwrap_or("text"),
                                                    ) {
                                                        Ok(t) => t,
                                                        Err(e) => {
                                                            back_tx.blocking_send((
                                                            PgWireBackendMessage::ErrorResponse(
                                                                e.into(),
                                                            ),
                                                            true,
                                                        ).into())?;
                                                            continue 'outer;
                                                        }
                                                    };
                                                oids.push(col_type.oid());
                                                fields.push(FieldInfo::new(
                                                    col.name().to_string(),
                                                    None,
                                                    None,
                                                    col_type,
                                                    result_formats
                                                        .get(i)
                                                        .copied()
                                                        .unwrap_or(FieldFormat::Text),
                                                ));
                                            }
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::RowDescription(
                                                        RowDescription::new(
                                                            fields.iter().map(Into::into).collect(),
                                                        ),
                                                    ),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".into(),
                                                        "XX000".into(),
                                                        "portal not found".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                    }
                                    _ => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "FATAL".into(),
                                                        SqlState::PROTOCOL_VIOLATION.code().into(),
                                                        "unexpected describe type".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
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

                                let (sql, query, _, param_types) = match prepared.get(stmt_name) {
                                    None => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        "statement not found".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                    Some(stmt) => stmt,
                                };

                                let mut prepped = match conn.prepare(sql) {
                                    Ok(prepped) => prepped,
                                    Err(e) => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        e.to_string(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                };

                                let param_types = if bind.parameters().len() != param_types.len() {
                                    if let Some(ParsedCmd(Cmd::Stmt(stmt))) = &query {
                                        let params = parameter_types(&schema, stmt);
                                        println!("computed params: {params:?}",);
                                        params
                                            .iter()
                                            .map(|param| match param {
                                                SqliteType::Null => unreachable!(),
                                                SqliteType::Integer => Type::INT8,
                                                SqliteType::Real => Type::FLOAT8,
                                                SqliteType::Text => Type::TEXT,
                                                SqliteType::Blob => Type::BYTEA,
                                            })
                                            .collect()
                                    } else {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        "could not determine parameter type".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue 'outer;
                                    }
                                } else {
                                    param_types.clone()
                                };

                                println!("CMD: {query:?}");
                                if bind.parameters().len() != param_types.len() {
                                    if let Some(ParsedCmd(Cmd::Stmt(stmt))) = &query {
                                        let params = parameter_types(&schema, stmt);
                                        println!("computed params: {params:?}",);
                                    }
                                }

                                for (i, param) in bind.parameters().iter().enumerate() {
                                    let idx = i + 1;
                                    let b = match param {
                                        None => {
                                            if let Err(e) = prepped
                                                .raw_bind_parameter(idx, rusqlite::types::Null)
                                            {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            ErrorInfo::new(
                                                                "ERROR".to_owned(),
                                                                "XX000".to_owned(),
                                                                e.to_string(),
                                                            )
                                                            .into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                continue 'outer;
                                            }
                                            continue;
                                        }
                                        Some(b) => b,
                                    };

                                    match param_types.get(i) {
                                        None => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            "missing parameter type".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
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
                                                back_tx.blocking_send(
                                                    (
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
                                                    )
                                                        .into(),
                                                )?;
                                                continue 'outer;
                                            }
                                        },
                                    }
                                }

                                portals.insert(
                                    portal_name,
                                    (
                                        stmt_name.into(),
                                        query.clone(),
                                        prepped,
                                        bind.result_column_format_codes()
                                            .iter()
                                            .copied()
                                            .map(FieldFormat::from)
                                            .collect(),
                                    ),
                                );

                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::BindComplete(BindComplete::new()),
                                        false,
                                    )
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Sync(_) => {
                                let ready_status = if open_tx.is_some() {
                                    READY_STATUS_TRANSACTION_BLOCK
                                } else {
                                    READY_STATUS_IDLE
                                };
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                                            ready_status,
                                        )),
                                        true,
                                    )
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Execute(execute) => {
                                let name = execute.name().as_deref().unwrap_or("");
                                let (parsed_cmd, prepped, result_formats) =
                                    match portals.get_mut(name) {
                                        Some((_, Some(parsed_cmd), prepped, result_formats)) => {
                                            (parsed_cmd, prepped, result_formats)
                                        }
                                        Some((_, None, _, _)) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::EmptyQueryResponse(
                                                        EmptyQueryResponse::new(),
                                                    ),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        None => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".into(),
                                                            "XX000".into(),
                                                            "portal not found".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                    };

                                let mut fields = vec![];
                                for (i, col) in prepped.columns().into_iter().enumerate() {
                                    let col_type =
                                        match name_to_type(col.decl_type().unwrap_or("text")) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            e.into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                continue 'outer;
                                            }
                                        };
                                    fields.push(FieldInfo::new(
                                        col.name().to_string(),
                                        None,
                                        None,
                                        col_type,
                                        result_formats.get(i).copied().unwrap_or(FieldFormat::Text),
                                    ));
                                }

                                let schema = Arc::new(fields);

                                let mut rows = prepped.raw_query();
                                let ncols = schema.len();

                                let max_rows = *execute.max_rows();
                                let max_rows = if max_rows == 0 {
                                    usize::MAX
                                } else {
                                    max_rows as usize
                                };
                                let mut count = 0;

                                loop {
                                    if count >= max_rows {
                                        // forget the Rows iterator here so as to not reset the statement!
                                        std::mem::forget(rows);
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::PortalSuspended(
                                                    PortalSuspended::new(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue 'outer;
                                    }
                                    let row = match rows.next() {
                                        Ok(Some(row)) => row,
                                        Ok(None) => {
                                            println!("done w/ rows");
                                            break;
                                        }
                                        Err(e) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            continue 'outer;
                                        }
                                    };
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
                                    match encoder.finish() {
                                        Ok(data_row) => {
                                            back_tx.blocking_send(
                                                (PgWireBackendMessage::DataRow(data_row), false)
                                                    .into(),
                                            )?;
                                        }
                                        Err(e) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            continue 'outer;
                                        }
                                    }
                                }

                                let tag = if parsed_cmd.returns_num_rows() {
                                    parsed_cmd.tag(Some(count))
                                } else if parsed_cmd.returns_rows_affected() {
                                    parsed_cmd.tag(Some(conn.changes() as usize))
                                } else {
                                    parsed_cmd.tag(None)
                                };

                                // done!
                                back_tx.blocking_send(
                                    (PgWireBackendMessage::CommandComplete(tag.into()), true)
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Query(query) => {
                                let trimmed = query.query().trim_matches(';');

                                let parsed_query = match parse_query(trimmed) {
                                    Ok(q) => q,
                                    Err(e) => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        e.to_string(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                };

                                if parsed_query.is_empty() {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::EmptyQueryResponse(
                                                EmptyQueryResponse::new(),
                                            ),
                                            false,
                                        )
                                            .into(),
                                    )?;

                                    let ready_status = if open_tx.is_some() {
                                        ReadyForQuery::new(READY_STATUS_TRANSACTION_BLOCK)
                                    } else {
                                        ReadyForQuery::new(READY_STATUS_IDLE)
                                    };

                                    back_tx.blocking_send(
                                        (PgWireBackendMessage::ReadyForQuery(ready_status), true)
                                            .into(),
                                    )?;
                                    continue;
                                }

                                for cmd in parsed_query.into_iter() {
                                    // need to start an implicit transaction
                                    if open_tx.is_none() && !cmd.is_begin() {
                                        if let Err(e) = conn.execute_batch("BEGIN") {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        println!("started IMPLICIT tx");
                                        open_tx = Some(OpenTx::Implicit);
                                    }

                                    // close the current implement tx first
                                    if matches!(open_tx, Some(OpenTx::Implicit)) && cmd.is_begin() {
                                        println!("committing IMPLICIT tx");
                                        open_tx = None;

                                        if let Err(e) = handle_commit(&agent, &conn, "COMMIT") {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;

                                            continue 'outer;
                                        }
                                        println!("committed IMPLICIT tx");
                                    }

                                    let count = if cmd.is_commit() {
                                        open_tx = None;

                                        if let Err(e) =
                                            handle_commit(&agent, &conn, &cmd.0.to_string())
                                        {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;

                                            continue 'outer;
                                        }
                                        None
                                    } else {
                                        let mut prepped = match conn.prepare(&cmd.0.to_string()) {
                                            Ok(prepped) => prepped,
                                            Err(e) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            ErrorInfo::new(
                                                                "ERROR".to_owned(),
                                                                "XX000".to_owned(),
                                                                e.to_string(),
                                                            )
                                                            .into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                continue 'outer;
                                            }
                                        };

                                        let mut fields = vec![];
                                        for col in prepped.columns() {
                                            let col_type = match name_to_type(
                                                col.decl_type().unwrap_or("text"),
                                            ) {
                                                Ok(t) => t,
                                                Err(e) => {
                                                    back_tx.blocking_send(
                                                        (
                                                            PgWireBackendMessage::ErrorResponse(
                                                                e.into(),
                                                            ),
                                                            true,
                                                        )
                                                            .into(),
                                                    )?;
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

                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::RowDescription(
                                                    RowDescription::new(
                                                        fields.iter().map(Into::into).collect(),
                                                    ),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;

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
                                                                &String::from_utf8_lossy(t)
                                                                    .as_ref(),
                                                            )
                                                            .unwrap();
                                                    }
                                                    ValueRef::Blob(b) => {
                                                        encoder.encode_field(&b).unwrap();
                                                    }
                                                }
                                            }
                                            match encoder.finish() {
                                                Ok(data_row) => {
                                                    back_tx.blocking_send(
                                                        (
                                                            PgWireBackendMessage::DataRow(data_row),
                                                            false,
                                                        )
                                                            .into(),
                                                    )?;
                                                }
                                                Err(e) => {
                                                    back_tx.blocking_send(
                                                        (
                                                            PgWireBackendMessage::ErrorResponse(
                                                                ErrorInfo::new(
                                                                    "ERROR".to_owned(),
                                                                    "XX000".to_owned(),
                                                                    e.to_string(),
                                                                )
                                                                .into(),
                                                            ),
                                                            true,
                                                        )
                                                            .into(),
                                                    )?;
                                                    continue 'outer;
                                                }
                                            }
                                        }
                                        Some(count)
                                    };

                                    let tag = if cmd.returns_num_rows() {
                                        cmd.tag(count)
                                    } else if cmd.returns_rows_affected() {
                                        cmd.tag(Some(conn.changes() as usize))
                                    } else {
                                        cmd.tag(None)
                                    };

                                    back_tx.blocking_send(
                                        (PgWireBackendMessage::CommandComplete(tag.into()), true)
                                            .into(),
                                    )?;

                                    if cmd.is_begin() {
                                        println!("setting EXPLICIT tx");
                                        // explicit tx
                                        open_tx = Some(OpenTx::Explicit)
                                    } else if cmd.is_rollback() || cmd.is_commit() {
                                        println!("clearing current open tx");
                                        // if this was a rollback, remove the current open tx
                                        open_tx = None;
                                    }
                                }

                                // automatically commit an implicit tx
                                if matches!(open_tx, Some(OpenTx::Implicit)) {
                                    println!("committing IMPLICIT tx");
                                    open_tx = None;

                                    if let Err(e) = handle_commit(&agent, &conn, "COMMIT") {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "ERROR".to_owned(),
                                                        "XX000".to_owned(),
                                                        e.to_string(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                    println!("committed IMPLICIT tx");
                                }

                                let ready_status = if open_tx.is_some() {
                                    ReadyForQuery::new(READY_STATUS_TRANSACTION_BLOCK)
                                } else {
                                    ReadyForQuery::new(READY_STATUS_IDLE)
                                };

                                back_tx.blocking_send(
                                    (PgWireBackendMessage::ReadyForQuery(ready_status), true)
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Terminate(_) => {
                                break;
                            }

                            PgWireFrontendMessage::PasswordMessageFamily(_) => {
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "ERROR".into(),
                                                "XX000".to_owned(),
                                                "PasswordMessage is not implemented".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                continue;
                            }
                            PgWireFrontendMessage::Close(close) => {
                                let name = close.name().as_deref().unwrap_or("");
                                match close.target_type() {
                                    // statement
                                    b'S' => {
                                        if prepared.remove(name).is_some() {
                                            portals.retain(|_, (stmt_name, _, _, _)| {
                                                stmt_name.as_str() != name
                                            });
                                        }
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::CloseComplete(
                                                    CloseComplete::new(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                    // portal
                                    b'P' => {
                                        portals.remove(name);
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::CloseComplete(
                                                    CloseComplete::new(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                    }
                                    _ => {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::ErrorResponse(
                                                    ErrorInfo::new(
                                                        "FATAL".into(),
                                                        SqlState::PROTOCOL_VIOLATION.code().into(),
                                                        "unexpected Close target_type".into(),
                                                    )
                                                    .into(),
                                                ),
                                                true,
                                            )
                                                .into(),
                                        )?;
                                        continue;
                                    }
                                }
                            }
                            PgWireFrontendMessage::Flush(_) => {
                                back_tx.blocking_send(BackendResponse::Flush)?;
                            }
                            PgWireFrontendMessage::CopyData(_) => {
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "ERROR".into(),
                                                "XX000".to_owned(),
                                                "CopyData is not implemented".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                continue;
                            }
                            PgWireFrontendMessage::CopyFail(_) => {
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "ERROR".into(),
                                                "XX000".to_owned(),
                                                "CopyFail is not implemented".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                continue;
                            }
                            PgWireFrontendMessage::CopyDone(_) => {
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "ERROR".into(),
                                                "XX000".to_owned(),
                                                "CopyDone is not implemented".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                continue;
                            }
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

    Ok(PgServer { local_addr })
}

#[allow(clippy::result_large_err)]
fn name_to_type(name: &str) -> Result<Type, ErrorInfo> {
    match name.to_uppercase().as_ref() {
        "ANY" => Ok(Type::ANY),
        "INT" | "INTEGER" => Ok(Type::INT8),
        "VARCHAR" => Ok(Type::VARCHAR),
        "TEXT" => Ok(Type::TEXT),
        "BINARY" | "BLOB" => Ok(Type::BYTEA),
        "FLOAT" => Ok(Type::FLOAT8),
        _ => Err(ErrorInfo::new(
            "ERROR".to_owned(),
            "42846".to_owned(),
            format!("Unsupported data type: {name}"),
        )),
    }
}

fn handle_commit(agent: &Agent, conn: &Connection, commit_stmt: &str) -> rusqlite::Result<()> {
    let actor_id = agent.actor_id();

    let ts = Timestamp::from(agent.clock().new_timestamp());

    let db_version: i64 = conn
        .prepare_cached("SELECT crsql_next_db_version()")?
        .query_row((), |row| row.get(0))?;

    let has_changes: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM crsql_changes WHERE site_id IS NULL AND db_version = ?);",
        )?
        .query_row([db_version], |row| row.get(0))?;

    if !has_changes {
        conn.execute_batch(commit_stmt)?;
        return Ok(());
    }

    let booked = {
        agent
            .bookie()
            .blocking_write("handle_write_tx(for_actor)")
            .for_actor(actor_id)
    };

    let last_seq: i64 = conn
        .prepare_cached(
            "SELECT MAX(seq) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?",
        )?
        .query_row([db_version], |row| row.get(0))?;

    let mut book_writer = booked.blocking_write("handle_write_tx(book_writer)");

    let last_version = book_writer.last().unwrap_or_default();
    trace!("last_version: {last_version}");
    let version = last_version + 1;
    trace!("version: {version}");

    conn.prepare_cached(
        r#"
            INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                VALUES (:actor_id, :start_version, :db_version, :last_seq, :ts);
        "#,
    )?
    .execute(named_params! {
        ":actor_id": actor_id,
        ":start_version": version,
        ":db_version": db_version,
        ":last_seq": last_seq,
        ":ts": ts
    })?;

    debug!(%actor_id, %version, %db_version, "inserted local bookkeeping row!");

    conn.execute_batch(commit_stmt)?;

    trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

    book_writer.insert(
        version,
        KnownDbVersion::Current {
            db_version,
            last_seq,
            ts,
        },
    );

    drop(book_writer);

    spawn_counted({
        let agent = agent.clone();
        async move {
            let conn = agent.pool().read().await?;
            block_in_place(|| agent.process_subs_by_db_version(&conn, db_version));
            Ok::<_, SqlitePoolError>(())
        }
    });

    Ok(())
}

fn compute_schema(conn: &Connection) -> Result<Schema, SchemaError> {
    let mut dump = String::new();

    let tables: HashMap<String, String> = conn
        .prepare(r#"SELECT name, sql FROM sqlite_schema WHERE type = "table" AND name IS NOT NULL AND sql IS NOT NULL ORDER BY tbl_name"#)?
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    for sql in tables.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    let indexes: HashMap<String, String> = conn
        .prepare(r#"SELECT name, sql FROM sqlite_schema WHERE type = "index" AND name IS NOT NULL AND sql IS NOT NULL ORDER BY tbl_name"#)?
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    for sql in indexes.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    parse_sql(dump.as_str())
}

fn is_param(expr: &Expr) -> bool {
    matches!(expr, Expr::Variable(_))
}

enum SqliteNameRef<'a> {
    Id(&'a Id),
    Name(&'a Name),
    Qualified(&'a Name, &'a Name),
    DoublyQualified(&'a Name, &'a Name, &'a Name),
}

impl<'a> SqliteNameRef<'a> {
    fn to_owned(&self) -> SqliteName {
        match self {
            SqliteNameRef::Id(id) => SqliteName::Id((*id).clone()),
            SqliteNameRef::Name(name) => SqliteName::Name((*name).clone()),
            SqliteNameRef::Qualified(n0, n1) => SqliteName::Qualified((*n0).clone(), (*n1).clone()),
            SqliteNameRef::DoublyQualified(n0, n1, n2) => {
                SqliteName::DoublyQualified((*n0).clone(), (*n1).clone(), (*n2).clone())
            }
        }
    }
}

#[derive(Clone, Debug)]
enum SqliteName {
    Id(Id),
    Name(Name),
    Qualified(Name, Name),
    DoublyQualified(Name, Name, Name),
}

fn expr_to_name(expr: &Expr) -> Option<SqliteNameRef> {
    match expr {
        Expr::Id(id) => Some(SqliteNameRef::Id(id)),
        Expr::Name(name) => Some(SqliteNameRef::Name(name)),
        Expr::Qualified(n0, n1) => Some(SqliteNameRef::Qualified(n0, n1)),
        Expr::DoublyQualified(n0, n1, n2) => Some(SqliteNameRef::DoublyQualified(n0, n1, n2)),
        _ => None,
    }
}

// determines the type of a literal type if any
// fn literal_type(expr: &Expr) -> Option<SqliteType> {
//     match expr {
//         Expr::Literal(lit) => match lit {
//             Literal::Numeric(num) => {
//                 if num.parse::<i64>().is_ok() {
//                     Some(SqliteType::Integer)
//                 } else if num.parse::<f64>().is_ok() {
//                     Some(SqliteType::Real)
//                 } else {
//                     // this should be unreachable...
//                     None
//                 }
//             }
//             Literal::String(_) => Some(SqliteType::Text),
//             Literal::Blob(_) => Some(SqliteType::Blob),
//             Literal::Keyword(keyword) => {
//                 // TODO: figure out what this is...
//                 warn!("got a keyword: {keyword}");
//                 None
//             }
//             Literal::Null => Some(SqliteType::Null),
//             Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
//                 // TODO: make this configurable at connection time or something
//                 Some(SqliteType::Text)
//             }
//         },
//         _ => None,
//     }
// }

fn handle_lhs_rhs(lhs: &Expr, rhs: &Expr) -> Option<SqliteName> {
    match (
        (expr_to_name(lhs), is_param(lhs)),
        (expr_to_name(rhs), is_param(rhs)),
    ) {
        ((Some(name), _), (_, true)) | ((_, true), (Some(name), _)) => Some(name.to_owned()),
        _ => None,
    }
}

fn extract_param(
    schema: &Schema,
    expr: &Expr,
    tables: &HashMap<String, &Table>,
    params: &mut Vec<SqliteType>,
) {
    match expr {
        // expr BETWEEN expr AND expr
        Expr::Between {
            lhs: _,
            start: _,
            end: _,
            not: _,
        } => {}

        // expr operator expr
        Expr::Binary(lhs, _, rhs) => {
            if let Some(name) = handle_lhs_rhs(lhs, rhs) {
                match name {
                    // not aliased!
                    SqliteName::Id(id) => {
                        // find the first one to match
                        for (_, table) in tables.iter() {
                            if let Some(col) = table.columns.get(&id.0) {
                                params.push(col.sql_type);
                                break;
                            }
                        }
                    }
                    SqliteName::Name(_) => {}
                    SqliteName::Qualified(tbl_name, col_name)
                    | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                        println!("looking tbl {} for col {}", tbl_name.0, col_name.0);
                        if let Some(table) = tables.get(&tbl_name.0) {
                            println!("found table! {}", table.name);
                            if let Ok(unquoted) = enquote::unquote(&col_name.0) {
                                println!("unquoted column as: {unquoted}");
                                if let Some(col) = table.columns.get(&unquoted) {
                                    params.push(col.sql_type);
                                }
                            } else if let Some(col) = table.columns.get(&col_name.0) {
                                println!("could not unquote, using original");
                                params.push(col.sql_type);
                            }
                        }
                    }
                }
            }
        }

        // CASE expr [WHEN expr THEN expr, ..., ELSE expr]
        Expr::Case {
            base: _,
            when_then_pairs: _,
            else_expr: _,
        } => {}

        // CAST ( expr AS type-name )
        Expr::Cast {
            expr: _,
            type_name: _,
        } => {}

        // expr COLLATE collation-name
        Expr::Collate(_, _) => {}

        // schema-name.table-name.column-name
        Expr::DoublyQualified(_, _, _) => {}

        // EXISTS ( select )
        Expr::Exists(select) => handle_select(schema, select, params),

        // function-name ( [DISTINCT] expr, ... ) filter-clause over-clause
        Expr::FunctionCall {
            name: _,
            distinctness: _,
            args: _,
            filter_over: _,
        } => {}

        Expr::FunctionCallStar {
            name: _,
            filter_over: _,
        } => {}

        // id
        Expr::Id(_) => {}

        // expr IN ( expr, ... )
        Expr::InList { lhs, not: _, rhs } => {
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    if let Some(name) = handle_lhs_rhs(lhs, expr) {
                        println!("HANDLED LHS RHS: {name:?}");
                        match name {
                            // not aliased!
                            SqliteName::Id(id) => {
                                // find the first one to match
                                for (_, table) in tables.iter() {
                                    if let Some(col) = table.columns.get(&id.0) {
                                        params.push(col.sql_type);
                                        break;
                                    }
                                }
                            }
                            SqliteName::Name(_) => {}
                            SqliteName::Qualified(tbl_name, col_name)
                            | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                                if let Some(table) = tables.get(&tbl_name.0) {
                                    if let Some(col) = table.columns.get(&col_name.0) {
                                        params.push(col.sql_type);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // expr IN ( select )
        Expr::InSelect {
            lhs: _,
            not: _,
            rhs,
        } => {
            // TODO: check LHS here
            handle_select(schema, rhs.as_ref(), params);
        }

        // expr IN schema-name.table-name | schema-name.table-function ( expr, ... )
        Expr::InTable {
            lhs: _,
            not: _,
            rhs: _,
            args: _,
        } => {}

        // expr IS NULL
        Expr::IsNull(_) => {}

        // expr [NOT] LIKE | GLOB | REGEXP | MATCH expr
        Expr::Like {
            lhs: _,
            not: _,
            op: _,
            rhs: _,
            escape: _,
        } => {}

        // NULL | integer | float | text | blob
        Expr::Literal(_) => {
            // nothing to do
        }

        // TODO:
        Expr::Name(_) => {}

        // expr NOT NULL
        Expr::NotNull(_) => {}

        // ( expr, ... )
        Expr::Parenthesized(exprs) => {
            for expr in exprs.iter() {
                extract_param(schema, expr, tables, params)
            }
        }

        // schema-name.table-name
        Expr::Qualified(_, _) => {}

        // RAISE ( IGNORE | ROLLBACK | ABORT | FAIL [ error ] )
        Expr::Raise(_, _) => {}

        // SELECT
        Expr::Subquery(select) => handle_select(schema, select, params),

        // NOT | ~ | - | + expr
        Expr::Unary(_, _) => {}

        // ? | $ | :
        Expr::Variable(_) => {}
    }
}

fn handle_select(schema: &Schema, select: &Select, params: &mut Vec<SqliteType>) {
    match &select.body.select {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            distinctness: _,
            group_by: _,
            window_clause: _,
        } => {
            if let Some(from) = from {
                let tables = handle_from(schema, from, params);
                if let Some(where_clause) = where_clause {
                    println!("WHERE CLAUSE: {where_clause:?}");
                    extract_param(schema, where_clause, &tables, params);
                }
            }
            for col in columns.iter() {
                if let ResultColumn::Expr(expr, _) = col {
                    // TODO: check against table if we can...
                    if is_param(expr) {
                        params.push(SqliteType::Text);
                    }
                }
            }
        }
        OneSelect::Values(values_values) => {
            for values in values_values.iter() {
                for value in values.iter() {
                    if is_param(value) {
                        params.push(SqliteType::Text);
                    }
                }
            }
        }
    }
}

fn handle_from<'a>(
    schema: &'a Schema,
    from: &FromClause,
    params: &mut Vec<SqliteType>,
) -> HashMap<String, &'a Table> {
    let mut tables: HashMap<String, &Table> = HashMap::new();
    if let Some(select) = from.select.as_deref() {
        match select {
            SelectTable::Table(qname, maybe_alias, _) => {
                let maybe_table = if let Ok(unquoted) = enquote::unquote(&qname.name.0) {
                    schema.tables.get(&unquoted)
                } else {
                    schema.tables.get(&qname.name.0)
                };

                if let Some(table) = maybe_table {
                    if let Some(alias) = maybe_alias {
                        let alias = match alias {
                            As::As(name) | As::Elided(name) => name.0.clone(),
                        };
                        tables.insert(alias, table);
                    } else {
                        tables.insert(table.name.clone(), table);
                    }
                }
            }
            SelectTable::TableCall(_, _, _) => {}
            SelectTable::Select(select, _) => {
                handle_select(schema, select, params);
            }
            SelectTable::Sub(_, _) => {}
        }
    }
    if let Some(joins) = &from.joins {
        for join in joins.iter() {
            match &join.table {
                SelectTable::Table(qname, maybe_alias, _) => {
                    let maybe_table = if let Ok(unquoted) = enquote::unquote(&qname.name.0) {
                        schema.tables.get(&unquoted)
                    } else {
                        schema.tables.get(&qname.name.0)
                    };

                    if let Some(table) = maybe_table {
                        if let Some(alias) = maybe_alias {
                            let alias = match alias {
                                As::As(name) | As::Elided(name) => name.0.clone(),
                            };
                            tables.insert(alias, table);
                        } else {
                            tables.insert(table.name.clone(), table);
                        }
                    }
                }
                SelectTable::TableCall(_, _, _) => {}
                SelectTable::Select(select, _) => {
                    handle_select(schema, select, params);
                }
                SelectTable::Sub(_, _) => {}
            }
        }
    }
    tables
}

fn parameter_types(schema: &Schema, stmt: &Stmt) -> Vec<SqliteType> {
    let mut params = vec![];

    match stmt {
        Stmt::Select(select) => handle_select(schema, select, &mut params),
        Stmt::Delete {
            tbl_name,
            where_clause: Some(where_clause),
            ..
        } => {
            let mut tables = HashMap::new();
            if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                tables.insert(tbl_name.name.0.clone(), tbl);
            }
            extract_param(schema, where_clause, &tables, &mut params);
        }
        Stmt::Insert {
            tbl_name,
            columns,
            body,
            ..
        } => {
            println!("GOT AN INSERT TO {tbl_name:?} on columns: {columns:?} w/ body: {body:?}");
            if let Some(table) = schema.tables.get(&tbl_name.name.0) {
                match body {
                    InsertBody::Select(select, _) => {
                        if let OneSelect::Values(values_values) = &select.body.select {
                            for values in values_values.iter() {
                                for (i, expr) in values.iter().enumerate() {
                                    if is_param(expr) {
                                        // specified columns
                                        let col = if let Some(columns) = columns {
                                            columns
                                                .get(i)
                                                .and_then(|name| table.columns.get(&name.0))
                                        } else {
                                            table.columns.get_index(i).map(|(_name, col)| col)
                                        };
                                        if let Some(col) = col {
                                            params.push(col.sql_type);
                                        }
                                    }
                                }
                            }
                        } else {
                            handle_select(schema, select, &mut params)
                        }
                    }
                    InsertBody::DefaultValues => {
                        // nothing to do!
                    }
                }
            }
        }
        Stmt::Update {
            with: _,
            or_conflict: _,
            tbl_name,
            indexed: _,
            sets: _,
            from,
            where_clause,
            returning: _,
            order_by: _,
            limit: _,
        } => {
            let mut tables = if let Some(from) = from {
                handle_from(schema, from, &mut params)
            } else {
                Default::default()
            };
            if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                tables.insert(tbl_name.name.0.clone(), tbl);
            }
            if let Some(where_clause) = where_clause {
                println!("WHERE CLAUSE: {where_clause:?}");
                extract_param(schema, where_clause, &tables, &mut params);
            }
        }
        _ => {
            // do nothing, there can't be bound params here!
        }
    }

    params
}

#[cfg(test)]
mod tests {
    use corro_tests::launch_test_agent;
    use spawn::wait_for_all_pending_handles;
    use tokio_postgres::NoTls;
    use tripwire::Tripwire;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pg() -> Result<(), BoxError> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let ta = launch_test_agent(|builder| builder.build(), tripwire.clone()).await?;

        let server = start(
            ta.agent.clone(),
            PgConfig {
                bind_addr: "127.0.0.1:0".parse()?,
            },
            tripwire,
        )
        .await?;

        let conn_str = format!(
            "host={} port={} user=testuser",
            server.local_addr.ip(),
            server.local_addr.port()
        );

        {
            let (mut client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
            // let (mut client, client_conn) =
            // tokio_postgres::connect("host=localhost port=5432 user=jerome", NoTls).await?;
            println!("client is ready!");
            tokio::spawn(client_conn);

            println!("before prepare");
            let stmt = client.prepare("SELECT 1").await?;
            println!(
                "after prepare: params: {:?}, columns: {:?}",
                stmt.params(),
                stmt.columns()
            );

            println!("before query");
            let rows = client.query(&stmt, &[]).await?;

            println!("rows count: {}", rows.len());
            for row in rows {
                println!("ROW!!! {row:?}");
            }

            println!("before execute");
            let affected = client
                .execute("INSERT INTO tests VALUES (1,2)", &[])
                .await?;
            println!("after execute, affected: {affected}");

            let row = client.query_one("SELECT * FROM crsql_changes", &[]).await?;
            println!("CHANGE ROW: {row:?}");

            client
                .batch_execute("SELECT 1; SELECT 2; SELECT 3;")
                .await?;
            println!("after batch exec");

            client.batch_execute("SELECT 1; BEGIN; SELECT 3;").await?;
            println!("after batch exec 2");

            client.batch_execute("SELECT 3; COMMIT; SELECT 3;").await?;
            println!("after batch exec 3");

            let tx = client.transaction().await?;
            println!("after begin I assume");
            let res = tx
                .execute(
                    "INSERT INTO tests VALUES ($1, $2)",
                    &[&2i64, &"hello world"],
                )
                .await?;
            println!("res (rows affected): {res}");
            let res = tx
                .execute(
                    "INSERT INTO tests2 VALUES ($1, $2)",
                    &[&2i64, &"hello world 2"],
                )
                .await?;
            println!("res (rows affected): {res}");
            tx.commit().await?;
            println!("after commit");

            let row = client
                .query_one("SELECT * FROM tests t WHERE t.id = ?", &[&2i64])
                .await?;
            println!("ROW: {row:?}");

            let row = client
                .query_one("SELECT * FROM tests t WHERE t.id = ?", &[&2i64])
                .await?;
            println!("ROW: {row:?}");

            let row = client
                .query_one("SELECT * FROM tests t WHERE t.id IN (?)", &[&2i64])
                .await?;
            println!("ROW: {row:?}");

            let row = client
        .query_one("SELECT t.id, t.text, t2.text as t2text FROM tests t LEFT JOIN tests2 t2 WHERE t.id = ?", &[&2i64])
        .await?;
            println!("ROW: {row:?}");

            println!("t.id: {:?}", row.try_get::<_, i64>(0));
            println!("t.text: {:?}", row.try_get::<_, String>(1));
            println!("t2text: {:?}", row.try_get::<_, String>(2));
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
