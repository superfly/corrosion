pub mod sql_state;
mod vtab;

use std::{
    collections::{HashMap, VecDeque},
    future::poll_fn,
    net::SocketAddr,
    str::{FromStr, Utf8Error},
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
        data::{NoData, ParameterDescription, RowDescription},
        extendedquery::{BindComplete, CloseComplete, ParseComplete, PortalSuspended},
        response::{
            CommandComplete, EmptyQueryResponse, ReadyForQuery, READY_STATUS_IDLE,
            READY_STATUS_TRANSACTION_BLOCK,
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
use tokio_util::{codec::Framed, sync::CancellationToken};
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

#[derive(Clone, Copy, Debug)]
enum StmtTag {
    Select,
    InsertAsSelect,

    Insert,
    Update,
    Delete,

    Alter,
    Analyze,
    Attach,
    Begin,
    Commit,
    Create,
    Detach,
    Drop,
    Pragma,
    Reindex,
    Release,
    Rollback,
    Savepoint,
    Vacuum,

    Other,
}

impl StmtTag {
    fn into_command_complete(self, rows: usize, conn: &Connection) -> CommandComplete {
        if self.returns_num_rows() {
            self.tag(Some(rows)).into()
        } else if self.returns_rows_affected() {
            let changes = conn.changes();
            if matches!(self, StmtTag::Insert) {
                CommandComplete::new(format!("INSERT 0 {changes}"))
            } else {
                self.tag(Some(changes as usize)).into()
            }
        } else {
            self.tag(None).into()
        }
    }

    fn returns_rows_affected(&self) -> bool {
        matches!(self, StmtTag::Insert | StmtTag::Update | StmtTag::Delete)
    }
    fn returns_num_rows(&self) -> bool {
        matches!(self, StmtTag::Select | StmtTag::InsertAsSelect)
    }
    pub fn tag(&self, rows: Option<usize>) -> Tag {
        match self {
            StmtTag::Select => Tag::new_for_execution("SELECT", rows),
            StmtTag::InsertAsSelect | StmtTag::Insert => Tag::new_for_execution("INSERT", rows),
            StmtTag::Update => Tag::new_for_execution("UPDATE", rows),
            StmtTag::Delete => Tag::new_for_execution("DELETE", rows),
            StmtTag::Alter => Tag::new_for_execution("ALTER", rows),
            StmtTag::Analyze => Tag::new_for_execution("ANALYZE", rows),
            StmtTag::Attach => Tag::new_for_execution("ATTACH", rows),
            StmtTag::Begin => Tag::new_for_execution("BEGIN", rows),
            StmtTag::Commit => Tag::new_for_execution("COMMIT", rows),
            StmtTag::Create => Tag::new_for_execution("CREATE", rows),
            StmtTag::Detach => Tag::new_for_execution("DETACH", rows),
            StmtTag::Drop => Tag::new_for_execution("DROP", rows),
            StmtTag::Pragma => Tag::new_for_execution("PRAGMA", rows),
            StmtTag::Reindex => Tag::new_for_execution("REINDEX", rows),
            StmtTag::Release => Tag::new_for_execution("RELEASE", rows),
            StmtTag::Rollback => Tag::new_for_execution("ROLLBACK", rows),
            StmtTag::Savepoint => Tag::new_for_execution("SAVEPOINT", rows),
            StmtTag::Vacuum => Tag::new_for_execution("VACUUM", rows),
            StmtTag::Other => Tag::new_for_execution("OK", rows),
        }
    }
}

enum Prepared {
    Empty,
    NonEmpty {
        sql: String,
        param_types: Vec<Type>,
        fields: Vec<FieldInfo>,
        tag: StmtTag,
    },
}

enum Portal<'a> {
    Empty {
        stmt_name: CompactString,
    },
    Parsed {
        stmt_name: CompactString,
        stmt: Statement<'a>,
        result_formats: Vec<FieldFormat>,
        tag: StmtTag,
    },
}

impl<'a> Portal<'a> {
    fn stmt_name(&self) -> &str {
        match self {
            Portal::Empty { stmt_name } | Portal::Parsed { stmt_name, .. } => stmt_name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
struct ParsedCmd(Cmd);

impl ParsedCmd {
    pub fn is_begin(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Begin(_, _)))
    }
    pub fn is_commit(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Commit(_)))
    }
    pub fn is_rollback(&self) -> bool {
        matches!(self.0, Cmd::Stmt(Stmt::Rollback { .. }))
    }

    fn tag(&self) -> StmtTag {
        match &self.0 {
            Cmd::Stmt(stmt) => match stmt {
                Stmt::Select(_) => StmtTag::Select,
                Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                } => StmtTag::InsertAsSelect,
                Stmt::AlterTable(_, _) => StmtTag::Alter,
                Stmt::Analyze(_) => StmtTag::Analyze,
                Stmt::Attach { .. } => StmtTag::Attach,
                Stmt::Begin(_, _) => StmtTag::Begin,
                Stmt::Commit(_) => StmtTag::Commit,
                Stmt::CreateIndex { .. }
                | Stmt::CreateTable { .. }
                | Stmt::CreateTrigger { .. }
                | Stmt::CreateView { .. }
                | Stmt::CreateVirtualTable { .. } => StmtTag::Create,
                Stmt::Delete { .. } => StmtTag::Delete,
                Stmt::Detach(_) => StmtTag::Detach,
                Stmt::DropIndex { .. }
                | Stmt::DropTable { .. }
                | Stmt::DropTrigger { .. }
                | Stmt::DropView { .. } => StmtTag::Drop,
                Stmt::Insert { .. } => StmtTag::Insert,
                Stmt::Pragma(_, _) => StmtTag::Pragma,
                Stmt::Reindex { .. } => StmtTag::Reindex,
                Stmt::Release(_) => StmtTag::Release,
                Stmt::Rollback { .. } => StmtTag::Rollback,
                Stmt::Savepoint(_) => StmtTag::Savepoint,

                Stmt::Update { .. } => StmtTag::Update,
                Stmt::Vacuum(_, _) => StmtTag::Vacuum,
            },
            _ => StmtTag::Other,
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
                trace!("SSL? {ssl}");

                let mut framed = Framed::new(
                    conn,
                    PgWireMessageServerCodec::new(ClientInfoHolder::new(remote_addr, false)),
                );

                let msg = match framed.next().await {
                    Some(msg) => msg?,
                    None => {
                        return Ok(());
                    }
                };

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

                trace!("sent auth ok and ReadyForQuery");

                let (front_tx, mut front_rx) = channel(1024);
                let (back_tx, mut back_rx) = channel(1024);

                let (mut sink, mut stream) = framed.split();

                let conn = agent.pool().client_dedicated().unwrap();
                trace!("opened connection");

                let cancel = CancellationToken::new();

                tokio::spawn({
                    let back_tx = back_tx.clone();
                    let cancel = cancel.clone();
                    async move {
                        // cancel stuff if this loop breaks
                        let _drop_guard = cancel.drop_guard();

                        while let Some(decode_res) = stream.next().await {
                            let msg = match decode_res {
                                Ok(msg) => msg,
                                Err(PgWireError::IoError(io_error)) => {
                                    debug!("postgres io error: {io_error}");
                                    break;
                                }
                                Err(e) => {
                                    warn!("could not receive pg frontend message: {e}");
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
                        debug!("frontend stream is done");

                        Ok::<_, BoxError>(())
                    }
                });

                tokio::spawn({
                    let cancel = cancel.clone();
                    async move {
                        let _drop_guard = cancel.drop_guard();
                        while let Some(back) = back_rx.recv().await {
                            match back {
                                BackendResponse::Message { message, flush } => {
                                    debug!("sending: {message:?}");
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
                        debug!("backend stream is done");
                        Ok::<_, std::io::Error>(())
                    }
                });

                let int_handle = conn.get_interrupt_handle();
                tokio::spawn(async move {
                    cancel.cancelled().await;
                    int_handle.interrupt();
                });

                block_in_place(|| {
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

                    let mut prepared: HashMap<CompactString, Prepared> = HashMap::new();

                    let mut portals: HashMap<CompactString, Portal> = HashMap::new();

                    let mut open_tx = None;

                    'outer: while let Some(msg) = front_rx.blocking_recv() {
                        debug!("msg: {msg:?}");

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
                                let name: &str = parse.name().as_deref().unwrap_or("");
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

                                match cmds.pop_front() {
                                    None => {
                                        prepared.insert(name.into(), Prepared::Empty);
                                    }
                                    Some(parsed_cmd) => {
                                        if !cmds.is_empty() {
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

                                        trace!("parsed cmd: {parsed_cmd:#?}");

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

                                        let mut param_types: Vec<Type> = parse
                                            .type_oids()
                                            .iter()
                                            .filter_map(|oid| Type::from_oid(*oid))
                                            .collect();

                                        if param_types.len() != prepped.parameter_count() {
                                            param_types = parameter_types(&schema, &parsed_cmd.0)
                                                .into_iter()
                                                .map(|param| match param {
                                                    (SqliteType::Null, _) => unreachable!(),
                                                    (SqliteType::Text, src) => Type::TEXT,
                                                    (SqliteType::Numeric, Some(src)) => match src {
                                                        "BOOLEAN" => Type::BOOL,
                                                        _ => Type::FLOAT8,
                                                    },
                                                    (SqliteType::Numeric, None) => Type::FLOAT8,
                                                    (SqliteType::Integer, src) => Type::INT8,
                                                    (SqliteType::Real, src) => Type::FLOAT8,
                                                    (SqliteType::Blob, src) => Type::BYTEA,
                                                })
                                                .collect();
                                        }

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

                                        prepared.insert(
                                            name.into(),
                                            Prepared::NonEmpty {
                                                sql: parse.query().clone(),
                                                param_types,
                                                fields,
                                                tag: parsed_cmd.tag(),
                                            },
                                        );
                                    }
                                }

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
                                    b'S' => match prepared.get(name) {
                                        None => {
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
                                        Some(Prepared::Empty) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::NoData(NoData::new()),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                        }
                                        Some(Prepared::NonEmpty {
                                            param_types,
                                            fields,
                                            ..
                                        }) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ParameterDescription(
                                                        ParameterDescription::new(
                                                            param_types
                                                                .iter()
                                                                .map(|t| t.oid())
                                                                .collect(),
                                                        ),
                                                    ),
                                                    false,
                                                )
                                                    .into(),
                                            )?;

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
                                        }
                                    },
                                    // portal
                                    b'P' => match portals.get(name) {
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
                                        }
                                        Some(Portal::Empty { .. }) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::NoData(NoData::new()),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                        }
                                        Some(Portal::Parsed {
                                            stmt,
                                            result_formats,
                                            ..
                                        }) => {
                                            let mut oids = vec![];
                                            let mut fields = vec![];
                                            for (i, col) in stmt.columns().into_iter().enumerate() {
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
                                        }
                                    },
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

                                match prepared.get(stmt_name) {
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
                                    Some(Prepared::Empty) => {
                                        portals.insert(
                                            portal_name,
                                            Portal::Empty {
                                                stmt_name: stmt_name.into(),
                                            },
                                        );
                                    }
                                    Some(Prepared::NonEmpty {
                                        sql,
                                        param_types,
                                        tag,
                                        ..
                                    }) => {
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

                                        trace!(
                                            "bind params count: {}, statement params count: {}",
                                            bind.parameters().len(),
                                            prepped.parameter_count()
                                        );

                                        trace!("param types: {param_types:?}");

                                        let mut format_codes = match bind
                                            .parameter_format_codes()
                                            .iter()
                                            .map(|code| {
                                                Ok(match *code {
                                                    0 => FormatCode::Text,
                                                    1 => FormatCode::Binary,
                                                    n => return Err(UnknownFormatCode(n)),
                                                })
                                            })
                                            .collect::<Result<Vec<FormatCode>, UnknownFormatCode>>()
                                        {
                                            Ok(v) => v,
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
                                        if format_codes.is_empty() {
                                            // no format codes? default to text
                                            format_codes =
                                                vec![FormatCode::Text; bind.parameters().len()];
                                        } else if format_codes.len() == 1 {
                                            // single code means we should use it for all others
                                            format_codes =
                                                vec![format_codes[0]; bind.parameters().len()];
                                        }

                                        for (i, param) in bind.parameters().iter().enumerate() {
                                            let idx = i + 1;
                                            let b = match param {
                                                None => {
                                                    trace!("binding idx {idx} w/ NULL");
                                                    if let Err(e) = prepped.raw_bind_parameter(
                                                        idx,
                                                        rusqlite::types::Null,
                                                    ) {
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

                                            trace!("got param bytes: {b:?}");

                                            match param_types.get(i) {
                                                None => {
                                                    trace!("no param type found!");
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
                                                Some(param_type) => {
                                                    let format_code = format_codes[i];
                                                    trace!("parsing param_type {param_type:?}, format_code: {format_code:?}, bytes: {b:?}");
                                                    match param_type {
                                                        t @ &Type::BOOL => {
                                                            let value: bool = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        t @ &Type::INT2 => {
                                                            let value: i16 = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        t @ &Type::INT4 => {
                                                            let value: i32 = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        t @ &Type::INT8 => {
                                                            let value: i64 = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }

                                                        t @ &Type::TEXT | t @ &Type::VARCHAR => {
                                                            let value: &str = match format_code {
                                                                FormatCode::Text => {
                                                                    std::str::from_utf8(b)?
                                                                }
                                                                FormatCode::Binary => {
                                                                    FromSql::from_sql(t, b)?
                                                                }
                                                            };
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        t @ &Type::FLOAT4 => {
                                                            let value: f32 = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        t @ &Type::FLOAT8 => {
                                                            let value: f64 = from_type_and_format(
                                                                t,
                                                                b,
                                                                format_code,
                                                            )?;
                                                            trace!("binding idx {idx} w/ value: {value}");
                                                            prepped
                                                                .raw_bind_parameter(idx, value)?;
                                                        }
                                                        &Type::BYTEA => {
                                                            let maybe_decoded = matches!(
                                                                format_code,
                                                                FormatCode::Text
                                                            )
                                                            .then(|| hex::decode(b).ok())
                                                            .flatten();

                                                            trace!("binding idx {idx} w/ decoded value: {maybe_decoded:?} (bytes: {b:?})");
                                                            prepped.raw_bind_parameter(
                                                                idx,
                                                                maybe_decoded
                                                                    .as_deref()
                                                                    .unwrap_or(b),
                                                            )?;
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
                                                    }
                                                }
                                            }
                                        }

                                        debug!("EXPANDED SQL: {:?}", prepped.expanded_sql());

                                        portals.insert(
                                            portal_name,
                                            Portal::Parsed {
                                                stmt_name: stmt_name.into(),
                                                stmt: prepped,
                                                result_formats: bind
                                                    .result_column_format_codes()
                                                    .iter()
                                                    .copied()
                                                    .map(FieldFormat::from)
                                                    .collect(),
                                                tag: *tag,
                                            },
                                        );
                                    }
                                }

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
                                let (prepped, result_formats, tag) = match portals.get_mut(name) {
                                    Some(Portal::Empty { .. }) => {
                                        trace!("empty portal");
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
                                    Some(Portal::Parsed {
                                        stmt,
                                        result_formats,
                                        tag,
                                        ..
                                    }) => (stmt, result_formats, tag),
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

                                trace!("non-empty portal!");

                                // TODO: maybe we don't need to recompute this...
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

                                trace!("fields: {fields:?}");

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

                                trace!("starting loop");

                                loop {
                                    if count >= max_rows {
                                        trace!("attained max rows");
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
                                        Ok(Some(row)) => {
                                            trace!("got a row: {row:?}");
                                            row
                                        }
                                        Ok(None) => {
                                            trace!("done w/ rows");
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

                                trace!("done w/ rows, computing tag: {tag:?}");

                                // done!
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::CommandComplete(
                                            (*tag).into_command_complete(count, &conn),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                            }
                            PgWireFrontendMessage::Query(query) => {
                                let mut normalized = query.query().trim_matches(';');

                                if normalized.starts_with("BEGIN READ")
                                    || normalized.starts_with("BEGIN WORK")
                                    || normalized.starts_with("BEGIN ISOLATION")
                                    || normalized.starts_with("BEGIN NOT ")
                                {
                                    normalized = "BEGIN";
                                }

                                let parsed_query = match parse_query(normalized) {
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
                                        trace!("started IMPLICIT tx");
                                        open_tx = Some(OpenTx::Implicit);
                                    }

                                    // close the current implement tx first
                                    if matches!(open_tx, Some(OpenTx::Implicit)) && cmd.is_begin() {
                                        trace!("committing IMPLICIT tx");
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
                                        trace!("committed IMPLICIT tx");
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
                                        0
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
                                        count
                                    };

                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::CommandComplete(
                                                cmd.tag().into_command_complete(count, &conn),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;

                                    if cmd.is_begin() {
                                        trace!("setting EXPLICIT tx");
                                        // explicit tx
                                        open_tx = Some(OpenTx::Explicit)
                                    } else if cmd.is_rollback() || cmd.is_commit() {
                                        trace!("clearing current open tx");
                                        // if this was a rollback, remove the current open tx
                                        open_tx = None;
                                    }
                                }

                                // automatically commit an implicit tx
                                if matches!(open_tx, Some(OpenTx::Implicit)) {
                                    trace!("committing IMPLICIT tx");
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
                                    trace!("committed IMPLICIT tx");
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
                                            portals.retain(|_, portal| portal.stmt_name() != name);
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

#[derive(Clone, Copy, Debug)]
#[repr(i16)]
pub enum FormatCode {
    Text = 0,
    Binary,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown format code {0}")]
pub struct UnknownFormatCode(i16);

#[derive(Debug, thiserror::Error)]
pub enum ToParamError<E> {
    #[error("conversion from bytes to types failed: {0}")]
    FromSql(Box<dyn std::error::Error + Sync + Send>),
    #[error(transparent)]
    Utf8(#[from] Utf8Error),
    #[error("parse error: {0}")]
    Parse(E),
}

fn from_type_and_format<'a, E, T: FromSql<'a> + FromStr<Err = E>>(
    t: &Type,
    b: &'a [u8],
    format_code: FormatCode,
) -> Result<T, ToParamError<E>>
// where
//     FromStr::Err =,
{
    Ok(match format_code {
        FormatCode::Text => {
            T::from_str(std::str::from_utf8(b)?).map_err(|e| ToParamError::<E>::Parse(e))?
        }
        FormatCode::Binary => T::from_sql(t, b).map_err(ToParamError::FromSql)?,
    })
}

#[allow(clippy::result_large_err)]
fn name_to_type(name: &str) -> Result<Type, ErrorInfo> {
    match name.to_uppercase().as_ref() {
        "ANY" => Ok(Type::ANY),
        "INT" | "INTEGER" | "DATETIME" => Ok(Type::INT8),
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

fn extract_param<'a>(
    schema: &'a Schema,
    expr: &Expr,
    tables: &HashMap<String, &'a Table>,
    params: &mut Vec<(SqliteType, Option<&'a str>)>,
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
                                params.push(col.sql_type());
                                break;
                            }
                        }
                    }
                    SqliteName::Name(_) => {}
                    SqliteName::Qualified(tbl_name, col_name)
                    | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                        trace!("looking tbl {} for col {}", tbl_name.0, col_name.0);
                        if let Some(table) = tables.get(&tbl_name.0) {
                            trace!("found table! {}", table.name);
                            let col_name = if col_name.0.starts_with('"') {
                                rem_first_and_last(&col_name.0)
                            } else {
                                &col_name.0
                            };

                            if let Some(col) = table.columns.get(col_name) {
                                params.push(col.sql_type());
                            }
                        }
                    }
                }
            } else {
                extract_param(schema, lhs, tables, params);
                extract_param(schema, rhs, tables, params);
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
                        trace!("HANDLED LHS RHS: {name:?}");
                        match name {
                            // not aliased!
                            SqliteName::Id(id) => {
                                // find the first one to match
                                for (_, table) in tables.iter() {
                                    if let Some(col) = table.columns.get(&id.0) {
                                        params.push(col.sql_type());
                                        break;
                                    }
                                }
                            }
                            SqliteName::Name(_) => {}
                            SqliteName::Qualified(tbl_name, col_name)
                            | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                                if let Some(table) = tables.get(&tbl_name.0) {
                                    if let Some(col) = table.columns.get(&col_name.0) {
                                        params.push(col.sql_type());
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

fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str()
}

fn handle_select<'a>(
    schema: &'a Schema,
    select: &Select,
    params: &mut Vec<(SqliteType, Option<&'a str>)>,
) {
    let tables = match &select.body.select {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            distinctness: _,
            group_by: _,
            window_clause: _,
        } => {
            let tables = if let Some(from) = from {
                let tables = handle_from(schema, from, params);
                if let Some(where_clause) = where_clause {
                    trace!("WHERE CLAUSE: {where_clause:?}");
                    extract_param(schema, where_clause, &tables, params);
                }
                tables
            } else {
                HashMap::new()
            };
            for col in columns.iter() {
                if let ResultColumn::Expr(expr, _) = col {
                    // TODO: check against table if we can...
                    if is_param(expr) {
                        params.push((SqliteType::Text, None));
                    }
                }
            }
            tables
        }
        OneSelect::Values(values_values) => {
            for values in values_values.iter() {
                for value in values.iter() {
                    if is_param(value) {
                        params.push((SqliteType::Text, None));
                    }
                }
            }
            HashMap::new()
        }
    };
    if let Some(limit) = &select.limit {
        if is_param(&limit.expr) {
            trace!("limit was a param (variable), pushing Integer type");
            params.push((SqliteType::Integer, None));
        } else {
            extract_param(schema, &limit.expr, &tables, params);
        }
        if let Some(offset) = &limit.offset {
            if is_param(offset) {
                trace!("offset was a param (variable), pushing Integer type");
                params.push((SqliteType::Integer, None));
            } else {
                extract_param(schema, offset, &tables, params);
            }
        }
    }
}

fn handle_from<'a>(
    schema: &'a Schema,
    from: &FromClause,
    params: &mut Vec<(SqliteType, Option<&'a str>)>,
) -> HashMap<String, &'a Table> {
    let mut tables: HashMap<String, &Table> = HashMap::new();
    if let Some(select) = from.select.as_deref() {
        match select {
            SelectTable::Table(qname, maybe_alias, _) => {
                let actual_tbl_name = if qname.name.0.starts_with('"') {
                    rem_first_and_last(&qname.name.0)
                } else {
                    &qname.name.0
                };

                if let Some(table) = schema.tables.get(actual_tbl_name) {
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
                    let actual_tbl_name = if qname.name.0.starts_with('"') {
                        rem_first_and_last(&qname.name.0)
                    } else {
                        &qname.name.0
                    };

                    if let Some(table) = schema.tables.get(actual_tbl_name) {
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

fn parameter_types<'a>(schema: &'a Schema, cmd: &Cmd) -> Vec<(SqliteType, Option<&'a str>)> {
    let mut params = vec![];

    if let Cmd::Stmt(stmt) = cmd {
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
                trace!("GOT AN INSERT TO {tbl_name:?} on columns: {columns:?} w/ body: {body:?}");
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
                                                params.push(col.sql_type());
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
                    trace!("WHERE CLAUSE: {where_clause:?}");
                    extract_param(schema, where_clause, &tables, &mut params);
                }
            }
            _ => {
                // do nothing, there can't be bound params here!
            }
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
        .query_one("SELECT t.id, t.text, t2.text as t2text FROM tests t LEFT JOIN tests2 t2 WHERE t.id = ? LIMIT ?", &[&2i64, &1i64])
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
