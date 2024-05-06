pub mod sql_state;
mod vtab;

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fmt,
    future::poll_fn,
    net::SocketAddr,
    str::{FromStr, Utf8Error},
    sync::Arc,
};

use bytes::Buf;
use chrono::NaiveDateTime;
use compact_str::CompactString;
use corro_types::{
    agent::{Agent, ChangeError},
    broadcast::broadcast_changes,
    change::{insert_local_changes, InsertChangesInfo},
    config::PgConfig,
    schema::{parse_sql, Column, Schema, SchemaError, SqliteType, Table},
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
            CommandComplete, EmptyQueryResponse, ErrorResponse, ReadyForQuery,
            READY_STATUS_FAILED_TRANSACTION_BLOCK, READY_STATUS_IDLE,
            READY_STATUS_TRANSACTION_BLOCK,
        },
        startup::{ParameterStatus, SslRequest},
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::PgWireMessageServerCodec,
};
use postgres_types::{FromSql, Type};
use rusqlite::{
    functions::FunctionFlags, types::ValueRef, vtab::eponymous_only_module, Connection, Statement,
};
use spawn::spawn_counted;
use sqlite3_parser::ast::{
    As, Cmd, ColumnDefinition, CreateTableBody, Expr, FromClause, Id, InsertBody, Limit, Name,
    OneSelect, ResultColumn, Select, SelectTable, Stmt, With,
};
use sqlparser::ast::Statement as PgStatement;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Sender},
        AcquireError, OwnedSemaphorePermit,
    },
    task::block_in_place,
};
use tokio_util::{codec::Framed, sync::CancellationToken};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

use crate::{
    sql_state::SqlState,
    vtab::{
        pg_class::PgClassTable,
        pg_database::{PgDatabase, PgDatabaseTable},
        pg_namespace::PgNamespaceTable,
        pg_range::PgRangeTable,
        pg_type::PgTypeTable,
    },
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
    fn into_command_complete(self, rows: usize, changes: usize) -> CommandComplete {
        if self.returns_num_rows() {
            self.tag(Some(rows)).into()
        } else if self.returns_rows_affected() {
            if matches!(self, StmtTag::Insert) {
                CommandComplete::new(format!("INSERT 0 {changes}"))
            } else {
                self.tag(Some(changes)).into()
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
        cmd: ParsedCmd,
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
        cmd: ParsedCmd,
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
enum ParsedCmd {
    Sqlite(Cmd),
    Postgres(PgStatement),
}

impl ParsedCmd {
    pub fn is_begin(&self) -> bool {
        matches!(
            self,
            ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Begin(_, _)))
                | ParsedCmd::Postgres(PgStatement::StartTransaction { .. })
        )
    }

    pub fn is_commit(&self) -> bool {
        matches!(
            self,
            ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Commit(_)))
                | ParsedCmd::Postgres(PgStatement::Commit { .. })
        )
    }

    pub fn is_rollback(&self) -> bool {
        matches!(self, ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Rollback { .. })))
    }

    pub fn is_pg(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(_))
    }

    pub fn is_show(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(PgStatement::ShowVariable { .. }))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(PgStatement::SetVariable { .. }))
    }

    fn tag(&self) -> StmtTag {
        match &self {
            ParsedCmd::Sqlite(Cmd::Stmt(stmt)) => match stmt {
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
            ParsedCmd::Postgres(stmt) => match stmt {
                PgStatement::StartTransaction { .. } => StmtTag::Begin,
                PgStatement::Commit { .. } => StmtTag::Commit,
                _ => StmtTag::Other,
            },
            _ => StmtTag::Other,
        }
    }
}

impl fmt::Display for ParsedCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParsedCmd::Sqlite(cmd) => cmd.fmt(f),
            ParsedCmd::Postgres(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("pg: {0}")]
    Postgres(#[from] sqlparser::parser::ParserError),
}

fn parse_query(sql: &str) -> Result<VecDeque<ParsedCmd>, ParseError> {
    let mut cmds = VecDeque::new();

    let normalized = sql.trim_matches(';').trim();
    if normalized.is_empty() {
        return Ok(cmds);
    }

    let mut parser = sqlite3_parser::lexer::sql::Parser::new(normalized.as_bytes());
    loop {
        match parser.next() {
            Ok(Some(cmd)) => {
                cmds.push_back(ParsedCmd::Sqlite(cmd));
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                debug!("could not parse as sqlite: {e}");
                let stmts = sqlparser::parser::Parser::parse_sql(
                    &sqlparser::dialect::PostgreSqlDialect {},
                    normalized,
                )?;
                for stmt in stmts {
                    cmds.push_back(ParsedCmd::Postgres(stmt));
                }
                break;
            }
        }
    }

    Ok(cmds)
}

#[derive(Debug, Default)]
enum TxState {
    Started {
        kind: OpenTxKind,
        write_permit: Option<OwnedSemaphorePermit>,
    },
    #[default]
    Ended,
}

impl TxState {
    fn implicit() -> Self {
        Self::Started {
            kind: OpenTxKind::Implicit,
            write_permit: None,
        }
    }
    fn explicit() -> Self {
        Self::Started {
            kind: OpenTxKind::Explicit,
            write_permit: None,
        }
    }

    fn is_writing(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                write_permit: Some(_),
                ..
            }
        )
    }

    fn set_write_permit(&mut self, permit: OwnedSemaphorePermit) {
        match self {
            TxState::Started { write_permit, .. } => *write_permit = Some(permit),
            TxState::Ended => {
                // do nothing, maybe bomb?
            }
        }
    }

    fn is_implicit(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                kind: OpenTxKind::Implicit,
                ..
            }
        )
    }
    fn is_explicit(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                kind: OpenTxKind::Explicit,
                ..
            }
        )
    }
    fn is_ended(&self) -> bool {
        matches!(self, TxState::Ended)
    }

    fn start_implicit(&mut self) {
        *self = Self::implicit()
    }

    fn start_explicit(&mut self) {
        *self = Self::explicit()
    }

    fn end(&mut self) -> Option<OwnedSemaphorePermit> {
        let permit = match self {
            TxState::Started { write_permit, .. } => write_permit.take(),
            TxState::Ended => None,
        };
        *self = TxState::Ended;
        permit
    }
}

#[derive(Debug)]
enum OpenTxKind {
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

    tokio::spawn(async move {
        loop {
            let (mut conn, remote_addr) = match server.accept().preemptible(&mut tripwire).await {
                Outcome::Completed(res) => res?,
                Outcome::Preempted(_) => break,
            };
            debug!("Accepted a PostgreSQL connection (from: {remote_addr})");

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
                        debug!("received startup message: {startup:?}");
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

                let conn = agent.pool().client_dedicated_write().unwrap();
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
                                    if let PgWireBackendMessage::ErrorResponse(e) = &message {
                                        warn!("sending: {e:?}");
                                    } else {
                                        debug!("sending: {message:?}");
                                    }
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

                let res = block_in_place(|| {
                    conn.execute_batch("ATTACH ':memory:' AS pg_catalog;")?;

                    let dbs = Arc::new(vec![PgDatabase::new("state".into())]);

                    conn.create_module(
                        "pg_database",
                        eponymous_only_module::<PgDatabaseTable>(),
                        Some(dbs),
                    )?;
                    conn.create_module("pg_type", eponymous_only_module::<PgTypeTable>(), None)?;
                    conn.create_module("pg_range", eponymous_only_module::<PgRangeTable>(), None)?;
                    conn.create_module(
                        "pg_namespace",
                        eponymous_only_module::<PgNamespaceTable>(),
                        None,
                    )?;
                    conn.create_module("pg_class", eponymous_only_module::<PgClassTable>(), None)?;

                    conn.create_scalar_function(
                        "version",
                        0,
                        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                        |_ctx| Ok("PostgreSQL 14.9"),
                    )?;

                    conn.create_scalar_function(
                        "pg_my_temp_schema",
                        0,
                        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                        |_ctx| Ok(0i64),
                    )?;

                    conn.create_scalar_function(
                        "pg_is_other_temp_schema",
                        1,
                        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                        |_ctx| Ok(false),
                    )?;

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

                    let mut discard_until_sync = false;

                    let mut session = Session {
                        agent,
                        tx_state: TxState::default(),
                    };

                    'outer: while let Some(msg) = front_rx.blocking_recv() {
                        debug!("msg: {msg:?}");

                        if discard_until_sync
                            && !matches!(
                                msg,
                                PgWireFrontendMessage::Sync(_) | PgWireFrontendMessage::Flush(_)
                            )
                        {
                            debug!("discarding message due to previous error");
                            continue;
                        }

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
                                        discard_until_sync = true;
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
                                            discard_until_sync = true;
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
                                                discard_until_sync = true;
                                                continue;
                                            }
                                        };

                                        let mut param_types: Vec<Type> = parse
                                            .type_oids()
                                            .iter()
                                            .filter_map(|oid| Type::from_oid(*oid))
                                            .collect();

                                        debug!("params types {param_types:?}");

                                        if param_types.len() != prepped.parameter_count() {
                                            param_types = parameter_types(&schema, &parsed_cmd)
                                                .params
                                                .into_iter()
                                                .map(|param| {
                                                    trace!("got param: {param:?}");
                                                    match (param.sqlite_type, param.source) {
                                                        (SqliteType::Null, _) => unreachable!(),
                                                        (SqliteType::Text, src) => match src {
                                                            Some("JSON") => Type::JSON,
                                                            _ => Type::TEXT,
                                                        },
                                                        (SqliteType::Numeric, Some(src)) => {
                                                            match src {
                                                                "BOOLEAN" | "BOOL" => Type::BOOL,
                                                                "DATETIME" => Type::TIMESTAMP,
                                                                _ => Type::FLOAT8,
                                                            }
                                                        }
                                                        (SqliteType::Numeric, None) => Type::FLOAT8,
                                                        (SqliteType::Integer, _src) => Type::INT8,
                                                        (SqliteType::Real, _src) => Type::FLOAT8,
                                                        (SqliteType::Blob, src) => match src {
                                                            Some("JSONB") => Type::JSONB,
                                                            _ => Type::BYTEA,
                                                        },
                                                    }
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
                                                    back_tx
                                                        .blocking_send((e.into(), true).into())?;
                                                    discard_until_sync = true;
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
                                                cmd: parsed_cmd,
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
                                            discard_until_sync = true;
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
                                            discard_until_sync = true;
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
                                        discard_until_sync = true;
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
                                        discard_until_sync = true;
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
                                        cmd,
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
                                                discard_until_sync = true;
                                                continue;
                                            }
                                        };

                                        trace!(
                                            "bind params count: {}, statement params count: {}",
                                            bind.parameters().len(),
                                            prepped.parameter_count()
                                        );

                                        debug!("bind param types: {param_types:?}");

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
                                                discard_until_sync = true;
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
                                                        discard_until_sync = true;
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
                                                    discard_until_sync = true;
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

                                                        t @ &Type::TEXT
                                                        | t @ &Type::VARCHAR
                                                        | t @ &Type::JSON => {
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

                                                        &Type::BYTEA | &Type::JSONB => {
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

                                                        t @ &Type::TIMESTAMP => {
                                                            let dt = match format_code {
                                                                FormatCode::Text => {
                                                                    let s =
                                                                        String::from_utf8_lossy(b);
                                                                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f").map_err(ToParamError::Parse)?
                                                                }
                                                                FormatCode::Binary => {
                                                                    NaiveDateTime::from_sql(t, b)
                                                                        .map_err(
                                                                            ToParamError::<
                                                                                chrono::format::ParseError
                                                                            >::FromSql,
                                                                        )?
                                                                }
                                                            };
                                                            prepped.raw_bind_parameter(idx, dt)?;
                                                        }

                                                        // t @ &Type::TIMESTAMP => {
                                                        //     let value: time::OffsetDateTime =
                                                        //         from_type_and_format(
                                                        //             t,
                                                        //             b,
                                                        //             format_code,
                                                        //         )?;

                                                        //     trace!("binding idx {idx} w/ value: {value}");
                                                        //     prepped
                                                        //         .raw_bind_parameter(idx, value)?;
                                                        // }
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
                                                                ).into(),
                                                            )?;
                                                            discard_until_sync = true;
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
                                                cmd: cmd.clone(),
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
                                send_ready(&mut session, &conn, discard_until_sync, &back_tx)?;

                                // reset this
                                discard_until_sync = false;
                            }
                            PgWireFrontendMessage::Execute(execute) => {
                                let name = execute.name().as_deref().unwrap_or("");
                                let (prepped, result_formats, cmd) = match portals.get_mut(name) {
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
                                        cmd,
                                        ..
                                    }) => (stmt, result_formats, cmd),
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
                                        discard_until_sync = true;
                                        continue;
                                    }
                                };

                                trace!("non-empty portal!");

                                let max_rows = *execute.max_rows();
                                let max_rows = if max_rows == 0 {
                                    usize::MAX
                                } else {
                                    max_rows as usize
                                };

                                if let Err(e) = session.handle_execute(
                                    &conn,
                                    prepped,
                                    result_formats,
                                    cmd,
                                    max_rows,
                                    &back_tx,
                                ) {
                                    back_tx.blocking_send(BackendResponse::Message {
                                        message: e.try_into()?,
                                        flush: true,
                                    })?;

                                    discard_until_sync = true;

                                    send_ready(&mut session, &conn, discard_until_sync, &back_tx)?;
                                    continue;
                                }
                            }
                            PgWireFrontendMessage::Query(query) => {
                                let parsed_query = match parse_query(query.query()) {
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
                                        send_ready(
                                            &mut session,
                                            &conn,
                                            discard_until_sync,
                                            &back_tx,
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

                                    send_ready(&mut session, &conn, discard_until_sync, &back_tx)?;
                                    continue;
                                }

                                for cmd in parsed_query.into_iter() {
                                    if let Err(e) =
                                        session.handle_query(&conn, &cmd, &back_tx, true)
                                    {
                                        back_tx.blocking_send(BackendResponse::Message {
                                            message: e.try_into()?,
                                            flush: true,
                                        })?;
                                        send_ready(
                                            &mut session,
                                            &conn,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue 'outer;
                                    }
                                }

                                // automatically commit an implicit tx
                                if session.tx_state.is_implicit() {
                                    trace!("committing IMPLICIT tx");
                                    let _permit = session.tx_state.end();

                                    if let Err(e) = session.handle_commit(&conn) {
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
                                        send_ready(
                                            &mut session,
                                            &conn,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue;
                                    }
                                    trace!("committed IMPLICIT tx");
                                }

                                send_ready(&mut session, &conn, discard_until_sync, &back_tx)?;
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
                                        discard_until_sync = true;
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
                });

                if let Err(e) = res {
                    error!("connection failed: {e}");
                    _ = back_tx
                        .send(
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
                        )
                        .await;
                }

                Ok::<_, BoxError>(())
            });
        }

        info!("postgres server done");

        Ok::<_, BoxError>(())
    });

    Ok(PgServer { local_addr })
}

struct Session {
    agent: Agent,
    tx_state: TxState,
}

impl Session {
    fn handle_query(
        &mut self,
        conn: &Connection,
        cmd: &ParsedCmd,
        back_tx: &Sender<BackendResponse>,
        send_row_desc: bool,
    ) -> Result<(), QueryError> {
        if cmd.is_show() {
            back_tx
                .blocking_send(
                    (
                        PgWireBackendMessage::CommandComplete(CommandComplete::new("SHOW".into())),
                        true,
                    )
                        .into(),
                )
                .map_err(|_| QueryError::BackendResponseSendFailed)?;
            return Ok(());
        }

        if cmd.is_set() {
            back_tx
                .blocking_send(
                    (
                        PgWireBackendMessage::CommandComplete(CommandComplete::new("SET".into())),
                        true,
                    )
                        .into(),
                )
                .map_err(|_| QueryError::BackendResponseSendFailed)?;
            return Ok(());
        }

        // need to start an implicit transaction
        if self.tx_state.is_ended() && !cmd.is_begin() {
            conn.execute_batch("BEGIN")?;
            trace!("started IMPLICIT tx");
            self.tx_state.start_implicit();
        } else if self.tx_state.is_implicit() && cmd.is_begin() {
            trace!("committing IMPLICIT tx");
            let _permit = self.tx_state.end();

            self.handle_commit(conn)?;
            trace!("committed IMPLICIT tx");
        }

        let tag = cmd.tag();

        let mut changes = 0usize;

        let count = if cmd.is_begin() {
            conn.execute_batch("BEGIN")?;
            self.tx_state.start_explicit();
            0
        } else if cmd.is_commit() {
            let _permit = self.tx_state.end();
            self.handle_commit(conn)?;
            0
        } else if cmd.is_rollback() {
            let _permit = self.tx_state.end();
            conn.execute_batch("ROLLBACK")?;
            0
        } else {
            let mut prepped = if cmd.is_pg() {
                return Err(QueryError::NotSqlite);
            } else {
                conn.prepare(&cmd.to_string())?
            };

            let mut fields = vec![];
            for col in prepped.columns() {
                let col_type = name_to_type(col.decl_type().unwrap_or("text"))?;
                fields.push(FieldInfo::new(
                    col.name().to_string(),
                    None,
                    None,
                    col_type,
                    FieldFormat::Text,
                ));
            }

            if send_row_desc {
                back_tx
                    .blocking_send(
                        (
                            PgWireBackendMessage::RowDescription(RowDescription::new(
                                fields.iter().map(Into::into).collect(),
                            )),
                            true,
                        )
                            .into(),
                    )
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            let schema = Arc::new(fields);

            if !self.tx_state.is_writing() && !prepped.readonly() {
                trace!("query statement writes, acquiring permit...");
                self.tx_state
                    .set_write_permit(self.agent.write_permit_blocking()?);
            }

            let mut rows = prepped.raw_query();
            let ncols = schema.len();

            let mut count = 0;
            while let Some(row) = rows.next()? {
                count += 1;
                let mut encoder = DataRowEncoder::new(schema.clone());
                for idx in 0..ncols {
                    let data = row.get_ref_unwrap::<usize>(idx);
                    match data {
                        ValueRef::Null => encoder.encode_field(&None::<i8>).unwrap(),
                        ValueRef::Integer(i) => {
                            encoder.encode_field(&i).unwrap();
                        }
                        ValueRef::Real(f) => {
                            encoder.encode_field(&f).unwrap();
                        }
                        ValueRef::Text(t) => {
                            encoder
                                .encode_field(&String::from_utf8_lossy(t).as_ref())
                                .unwrap();
                        }
                        ValueRef::Blob(b) => {
                            encoder.encode_field(&b).unwrap();
                        }
                    }
                }
                let data_row = encoder.finish()?;
                back_tx
                    .blocking_send((PgWireBackendMessage::DataRow(data_row), false).into())
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            if tag.returns_rows_affected() {
                changes = conn.changes() as usize;
            }

            count
        };

        back_tx
            .blocking_send(
                (
                    PgWireBackendMessage::CommandComplete(
                        tag.into_command_complete(count, changes),
                    ),
                    true,
                )
                    .into(),
            )
            .map_err(|_| QueryError::BackendResponseSendFailed)?;

        if cmd.is_begin() {
            trace!("setting EXPLICIT tx");
            // explicit tx
            self.tx_state.start_explicit();
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_execute<'conn>(
        &mut self,
        conn: &'conn Connection,
        prepped: &mut Statement<'conn>,
        result_formats: &[FieldFormat],
        cmd: &ParsedCmd,
        max_rows: usize,
        back_tx: &Sender<BackendResponse>,
    ) -> Result<(), QueryError> {
        // TODO: maybe we don't need to recompute this...
        let mut fields = vec![];
        for (i, col) in prepped.columns().into_iter().enumerate() {
            trace!("col decl_type: {:?}", col.decl_type());
            let col_type = name_to_type(col.decl_type().unwrap_or("any"))?;

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

        // we need to know because we'll commit it right away
        let mut opened_implicit_tx = false;

        if self.tx_state.is_ended() {
            if !cmd.is_begin() && !prepped.readonly() {
                // NOT in a tx and statement mutates DB...
                conn.execute_batch("BEGIN")?;

                self.tx_state.start_implicit();
                opened_implicit_tx = true;
            } else if cmd.is_begin() {
                conn.execute_batch("BEGIN")?;
                self.tx_state.start_explicit();
            }
        }

        let tag = cmd.tag();

        let mut count = 0;
        let mut changes = 0usize;

        if cmd.is_commit() {
            let _permit = self.tx_state.end();
            self.handle_commit(conn)?;
        } else {
            if !self.tx_state.is_writing() && !prepped.readonly() {
                trace!("statement writes, acquiring permit...");
                self.tx_state
                    .set_write_permit(self.agent.write_permit_blocking()?);
            }
            let mut rows = prepped.raw_query();
            loop {
                if count >= max_rows {
                    trace!("attained max rows");
                    // forget the Rows iterator here so as to not reset the statement!
                    std::mem::forget(rows);
                    back_tx
                        .blocking_send(
                            (
                                PgWireBackendMessage::PortalSuspended(PortalSuspended::new()),
                                true,
                            )
                                .into(),
                        )
                        .map_err(|_| QueryError::BackendResponseSendFailed)?;
                    return Ok(());
                }

                let row = match rows.next()? {
                    Some(row) => {
                        trace!("got a row: {row:?}");
                        row
                    }
                    None => {
                        trace!("done w/ rows");
                        break;
                    }
                };

                count += 1;

                let mut encoder = DataRowEncoder::new(schema.clone());
                for (idx, field) in schema.iter().enumerate() {
                    trace!("processing field: {field:?}");
                    let format = *field.format();
                    match field.datatype() {
                        &Type::ANY => {
                            let data = row.get_ref_unwrap(idx);
                            match data {
                                ValueRef::Null => encoder
                                    .encode_field_with_type_and_format(
                                        &None::<i8>,
                                        &Type::ANY,
                                        format,
                                    )
                                    .unwrap(),
                                ValueRef::Integer(i) => {
                                    encoder
                                        .encode_field_with_type_and_format(&i, &Type::INT8, format)
                                        .unwrap();
                                }
                                ValueRef::Real(f) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &f,
                                            &Type::FLOAT8,
                                            format,
                                        )
                                        .unwrap();
                                }
                                ValueRef::Text(t) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &String::from_utf8_lossy(t).as_ref(),
                                            &Type::TEXT,
                                            format,
                                        )
                                        .unwrap();
                                }
                                ValueRef::Blob(b) => {
                                    encoder
                                        .encode_field_with_type_and_format(&b, &Type::BYTEA, format)
                                        .unwrap();
                                }
                            }
                        }
                        t @ &Type::INT8 => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<i64>>(idx)?,
                                    t,
                                    format,
                                )
                                .unwrap();
                        }
                        t @ &Type::TIMESTAMP => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<NaiveDateTime>>(idx)?,
                                    t,
                                    format,
                                )
                                .unwrap();
                        }
                        t @ &Type::VARCHAR | t @ &Type::TEXT | t @ &Type::JSON => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<String>>(idx)?,
                                    t,
                                    format,
                                )
                                .unwrap();
                        }
                        t @ &Type::BYTEA | t @ &Type::JSONB => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<Vec<u8>>>(idx)?,
                                    t,
                                    format,
                                )
                                .unwrap();
                        }
                        t @ &Type::FLOAT8 => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<f64>>(idx)?,
                                    t,
                                    format,
                                )
                                .unwrap();
                        }
                        _ => {
                            return Err(UnsupportedSqliteToPostgresType(field.name().clone()).into())
                        }
                    }
                }

                let data_row = encoder.finish()?;
                back_tx
                    .blocking_send((PgWireBackendMessage::DataRow(data_row), false).into())
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            if tag.returns_rows_affected() {
                changes = conn.changes() as usize;
            }

            if opened_implicit_tx {
                let _permit = self.tx_state.end();
                self.handle_commit(conn)?;
            }
        }

        trace!("done w/ rows, computing tag: {tag:?}");

        // done!
        back_tx
            .blocking_send(
                (
                    PgWireBackendMessage::CommandComplete(
                        tag.into_command_complete(count, changes),
                    ),
                    true,
                )
                    .into(),
            )
            .map_err(|_| QueryError::BackendResponseSendFailed)?;

        Ok(())
    }

    fn handle_commit(&self, conn: &Connection) -> Result<(), ChangeError> {
        trace!("HANDLE COMMIT");

        let mut book_writer = self
            .agent
            .booked()
            .blocking_write("handle_write_tx(book_writer)");

        let actor_id = self.agent.actor_id();

        let insert_info = insert_local_changes(&self.agent, conn, &mut book_writer)?;
        conn.execute_batch("COMMIT")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        if let Some(InsertChangesInfo {
            version,
            db_version,
            last_seq,
            ts,
            snap,
        }) = insert_info
        {
            trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

            book_writer.commit_snapshot(snap);

            let agent = self.agent.clone();

            spawn_counted(async move {
                broadcast_changes(agent, db_version, last_seq, version, ts).await
            });
        }

        Ok(())
    }
}

fn send_ready(
    session: &mut Session,
    conn: &Connection,
    discard_until_sync: bool,
    back_tx: &Sender<BackendResponse>,
) -> Result<(), BoxError> {
    let ready_status = if session.tx_state.is_implicit() {
        let _permit = session.tx_state.end(); // do this first, in case of failure
        if discard_until_sync {
            // an error occured, rollback implicit tx!
            warn!("receive Sync message w/ an error to send, rolling back implicit tx");
            conn.execute_batch("ROLLBACK")?;
        } else {
            // no error, commit implicit tx
            warn!("receive Sync message, committing implicit tx");
            session.handle_commit(conn)?;
        }

        READY_STATUS_IDLE
    } else if session.tx_state.is_explicit() {
        if discard_until_sync {
            READY_STATUS_FAILED_TRANSACTION_BLOCK
        } else {
            READY_STATUS_TRANSACTION_BLOCK
        }
    } else {
        READY_STATUS_IDLE
    };

    back_tx.blocking_send(
        (
            PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(ready_status)),
            true,
        )
            .into(),
    )?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum QueryError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Unsupported(#[from] UnsupportedSqliteToPostgresType),
    #[error("statement is not parsable as SQLite-flavored SQL")]
    NotSqlite,
    #[error(transparent)]
    PgWire(#[from] PgWireError),
    #[error("backend response channel is closed")]
    BackendResponseSendFailed,
    #[error("could not acquire write permit")]
    PermitAcquire(#[from] AcquireError),
    #[error(transparent)]
    Change(#[from] ChangeError),
}

#[derive(Debug, thiserror::Error)]
#[error("channel is closed")]
struct ChannelClosed;

impl TryFrom<QueryError> for PgWireBackendMessage {
    type Error = ChannelClosed;

    fn try_from(value: QueryError) -> Result<Self, Self::Error> {
        Ok(PgWireBackendMessage::ErrorResponse(match value {
            QueryError::Rusqlite(e) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            QueryError::Unsupported(e) => e.into(),
            e @ QueryError::NotSqlite => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            QueryError::PgWire(e) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            e @ QueryError::PermitAcquire(_) => {
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            QueryError::BackendResponseSendFailed => return Err(ChannelClosed),
            QueryError::Change(e) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
        }))
    }
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

#[derive(Debug, thiserror::Error)]
#[error("Unsupported data type: {0}")]
struct UnsupportedSqliteToPostgresType(String);

impl From<UnsupportedSqliteToPostgresType> for PgWireBackendMessage {
    fn from(value: UnsupportedSqliteToPostgresType) -> Self {
        PgWireBackendMessage::ErrorResponse(value.into())
    }
}

impl From<UnsupportedSqliteToPostgresType> for ErrorResponse {
    fn from(value: UnsupportedSqliteToPostgresType) -> Self {
        ErrorInfo::new("ERROR".to_owned(), "42846".to_owned(), value.to_string()).into()
    }
}

#[allow(clippy::result_large_err)]
fn name_to_type(name: &str) -> Result<Type, UnsupportedSqliteToPostgresType> {
    Ok(match name.to_uppercase().as_ref() {
        "ANY" => Type::ANY,
        "INT" | "INTEGER" | "BIGINT" => Type::INT8,
        "DATETIME" => Type::TIMESTAMP,
        "VARCHAR" => Type::VARCHAR,
        "TEXT" => Type::TEXT,
        "BINARY" | "BLOB" => Type::BYTEA,
        "JSONB" => Type::JSONB,
        "JSON" => Type::JSON,
        "FLOAT" => Type::FLOAT8,
        _ => return Err(UnsupportedSqliteToPostgresType(name.to_string())),
    })
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

#[derive(Debug)]
enum ParamKind<'a> {
    Named(&'a str),
    Positional,
}

fn as_param(expr: &Expr) -> Option<ParamKind> {
    if let Expr::Variable(name) = expr {
        if name.is_empty() {
            Some(ParamKind::Positional)
        } else {
            Some(ParamKind::Named(name.as_str()))
        }
    } else {
        None
    }
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

fn handle_lhs_rhs<'stmt>(
    lhs: &'stmt Expr,
    rhs: &'stmt Expr,
) -> Option<(SqliteName, ParamKind<'stmt>)> {
    match (
        (expr_to_name(lhs), as_param(lhs)),
        (expr_to_name(rhs), as_param(rhs)),
    ) {
        ((Some(name), _), (_, Some(kind))) | ((_, Some(kind)), (Some(name), _)) => {
            Some((name.to_owned(), kind))
        }
        _ => None,
    }
}

fn extract_params<'schema, 'stmt>(
    schema: &'schema Schema,
    expr: &'stmt Expr,
    tables: &HashMap<String, &'schema Table>,
    params: &mut ParamsList<'stmt, 'schema>,
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
            if let Some((name, kind)) = handle_lhs_rhs(lhs, rhs) {
                match name {
                    // not aliased!
                    SqliteName::Id(id) => {
                        // find the first one to match
                        for (_, table) in tables.iter() {
                            if let Some(col) = table.columns.get(&id.0) {
                                let (sqlite_type, source) = col.sql_type();
                                params.insert(Param {
                                    kind,
                                    sqlite_type,
                                    source,
                                });
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
                                let (sqlite_type, source) = col.sql_type();
                                params.insert(Param {
                                    kind,
                                    sqlite_type,
                                    source,
                                });
                            }
                        }
                    }
                }
            } else {
                extract_params(schema, lhs, tables, params);
                extract_params(schema, rhs, tables, params);
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
            order_by: _,
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
                    if let Some((name, kind)) = handle_lhs_rhs(lhs, expr) {
                        trace!("HANDLED LHS RHS: {name:?}");
                        match name {
                            // not aliased!
                            SqliteName::Id(id) => {
                                // find the first one to match
                                for (_, table) in tables.iter() {
                                    if let Some(col) = table.columns.get(&id.0) {
                                        let (sqlite_type, source) = col.sql_type();
                                        params.insert(Param {
                                            kind,
                                            sqlite_type,
                                            source,
                                        });
                                        break;
                                    }
                                }
                            }
                            SqliteName::Name(_) => {}
                            SqliteName::Qualified(tbl_name, col_name)
                            | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                                let col_name = if col_name.0.starts_with('"') {
                                    rem_first_and_last(&col_name.0)
                                } else {
                                    &col_name.0
                                };
                                if let Some(table) = tables.get(&tbl_name.0) {
                                    if let Some(col) = table.columns.get(col_name) {
                                        let (sqlite_type, source) = col.sql_type();
                                        params.insert(Param {
                                            kind,
                                            sqlite_type,
                                            source,
                                        });
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
                extract_params(schema, expr, tables, params)
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

fn handle_select<'schema, 'stmt>(
    schema: &'schema Schema,
    select: &'stmt Select,
    params: &mut ParamsList<'stmt, 'schema>,
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
                    extract_params(schema, where_clause, &tables, params);
                }
                tables
            } else {
                HashMap::new()
            };
            for col in columns.iter() {
                if let ResultColumn::Expr(expr, _) = col {
                    // TODO: check against table if we can...
                    if let Some(kind) = as_param(expr) {
                        params.insert(Param {
                            kind,
                            sqlite_type: SqliteType::Text,
                            source: None,
                        });
                    }
                }
            }
            tables
        }
        OneSelect::Values(values_values) => {
            for values in values_values.iter() {
                for value in values.iter() {
                    if let Some(kind) = as_param(value) {
                        params.insert(Param {
                            kind,
                            sqlite_type: SqliteType::Text,
                            source: None,
                        });
                    }
                }
            }
            HashMap::new()
        }
    };
    if let Some(limit) = &select.limit {
        handle_limit(schema, limit, &tables, params);
    }
}

fn handle_from<'schema, 'stmt>(
    schema: &'schema Schema,
    from: &'stmt FromClause,
    params: &mut ParamsList<'stmt, 'schema>,
) -> HashMap<String, &'schema Table> {
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

#[derive(Debug)]
struct Param<'stmt, 'schema> {
    kind: ParamKind<'stmt>,
    sqlite_type: SqliteType,
    source: Option<&'schema str>,
}

#[derive(Default, Debug)]
struct ParamsList<'stmt, 'schema> {
    params: Vec<Param<'stmt, 'schema>>,
    named: BTreeSet<&'stmt str>,
}

impl<'stmt, 'schema> ParamsList<'stmt, 'schema> {
    pub fn insert(&mut self, param: Param<'stmt, 'schema>) {
        let should_push = if let ParamKind::Named(name) = &param.kind {
            self.named.insert(*name)
        } else {
            true
        };
        if should_push {
            self.params.push(param);
        }
    }
}

fn handle_limit<'schema, 'stmt>(
    schema: &'schema Schema,
    limit: &'stmt Limit,
    tables: &HashMap<String, &'schema Table>,
    params: &mut ParamsList<'stmt, 'schema>,
) {
    if let Some(kind) = as_param(&limit.expr) {
        trace!("limit was a param (variable), pushing Integer type");
        params.insert(Param {
            kind,
            sqlite_type: SqliteType::Integer,
            source: None,
        });
    } else {
        extract_params(schema, &limit.expr, tables, params);
    }
    if let Some(offset) = &limit.offset {
        if let Some(kind) = as_param(offset) {
            trace!("offset was a param (variable), pushing Integer type");
            params.insert(Param {
                kind,
                sqlite_type: SqliteType::Integer,
                source: None,
            });
        } else {
            extract_params(schema, offset, tables, params);
        }
    }
}

fn handle_with<'schema, 'stmt>(
    schema: &'schema Schema,
    with: &'stmt With,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Vec<Table> {
    let mut tables = vec![];
    for cte in with.ctes.iter() {
        handle_select(schema, &cte.select, params);
        tables.push(Table {
            name: cte.tbl_name.0.clone(),
            pk: Default::default(),
            columns: cte
                .columns
                .as_ref()
                .map(|columns| {
                    columns
                        .iter()
                        .map(|col| {
                            (
                                col.col_name.0.clone(),
                                Column {
                                    name: col.col_name.0.clone(),
                                    sql_type: (SqliteType::Text, None), // no idea!
                                    nullable: false,
                                    default_value: None,
                                    generated: None,
                                    primary_key: false,
                                    raw: ColumnDefinition {
                                        col_name: col.col_name.clone(),
                                        col_type: None,
                                        constraints: vec![],
                                    },
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            indexes: Default::default(),
            raw: CreateTableBody::AsSelect(cte.select.clone()),
        })
    }
    tables
}

fn parameter_types<'schema, 'stmt>(
    schema: &'schema Schema,
    cmd: &'stmt ParsedCmd,
) -> ParamsList<'stmt, 'schema> {
    let mut params = ParamsList::default();

    if let ParsedCmd::Sqlite(Cmd::Stmt(stmt)) = cmd {
        match stmt {
            Stmt::Select(select) => handle_select(schema, select, &mut params),
            Stmt::Delete {
                with,
                tbl_name,
                where_clause,
                limit,
                ..
            } => {
                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params);
                }

                let mut tables = HashMap::new();
                if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                    tables.insert(tbl_name.name.0.clone(), tbl);
                }
                if let Some(where_clause) = where_clause {
                    extract_params(schema, where_clause, &tables, &mut params);
                }

                if let Some(limit) = limit {
                    handle_limit(schema, limit, &tables, &mut params);
                }
            }
            Stmt::Insert {
                with,
                tbl_name,
                columns,
                body,
                ..
            } => {
                trace!("GOT AN INSERT TO {tbl_name:?} on columns: {columns:?} w/ body: {body:?}");

                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params);
                }

                if let Some(table) = schema.tables.get(&tbl_name.name.0) {
                    match body {
                        InsertBody::Select(select, _) => {
                            if let OneSelect::Values(values_values) = &select.body.select {
                                for values in values_values.iter() {
                                    for (i, expr) in values.iter().enumerate() {
                                        if let Some(kind) = as_param(expr) {
                                            // specified columns
                                            let col = if let Some(columns) = columns {
                                                columns
                                                    .get(i)
                                                    .and_then(|name| table.columns.get(&name.0))
                                            } else {
                                                table.columns.get_index(i).map(|(_name, col)| col)
                                            };
                                            if let Some(col) = col {
                                                let (sqlite_type, source) = col.sql_type();
                                                params.insert(Param {
                                                    kind,
                                                    sqlite_type,
                                                    source,
                                                });
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
                with,
                or_conflict: _,
                tbl_name,
                indexed: _,
                sets,
                from,
                where_clause,
                returning: _,
                order_by: _,
                limit,
            } => {
                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params);
                }

                let mut tables: HashMap<String, &'schema Table> = Default::default();

                let table = if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                    tables.insert(tbl_name.name.0.clone(), tbl);
                    Some(tbl)
                } else {
                    None
                };

                for set in sets.iter() {
                    if let Some(kind) = as_param(&set.expr) {
                        let (sqlite_type, source) = if let Some(col) =
                            set.col_names.first().and_then(|first_col_name| {
                                table.and_then(|table| table.columns.get(&first_col_name.0))
                            }) {
                            col.sql_type()
                        } else {
                            (SqliteType::Text, None)
                        };
                        params.insert(Param {
                            kind,
                            sqlite_type,
                            source,
                        });
                    }
                }

                if let Some(from) = from {
                    let from_tables = handle_from(schema, from, &mut params);

                    tables.extend(from_tables);
                }

                if let Some(where_clause) = where_clause {
                    trace!("WHERE CLAUSE: {where_clause:?}");
                    extract_params(schema, where_clause, &tables, &mut params);
                }
                if let Some(limit) = limit {
                    handle_limit(schema, limit, &tables, &mut params);
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
    use std::time::{Duration, Instant};

    use chrono::{DateTime, Utc};
    use corro_tests::launch_test_agent;
    use spawn::wait_for_all_pending_handles;
    use tokio_postgres::NoTls;
    use tripwire::Tripwire;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pg() -> Result<(), BoxError> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let tmpdir = tempfile::tempdir()?;

        tokio::fs::write(
            tmpdir.path().join("kitchensink.sql"),
            "
            CREATE TABLE kitchensink (
                id BIGINT PRIMARY KEY NOT NULL,
                other_ts DATETIME,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ",
        )
        .await?;

        let ta = launch_test_agent(
            |builder| {
                builder
                    .add_schema_path(tmpdir.path().display().to_string())
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        let sema = ta.agent.write_sema().clone();

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

            let _permit = sema.acquire().await;

            println!("before prepare");
            let stmt = client.prepare("SELECT 1").await?;
            println!(
                "after prepare: params: {:?}, columns: {:?}",
                stmt.params(),
                stmt.columns()
            );

            println!("before query");
            // add a timeout because the semaphore shouldn't block anything here
            // it will fail if the semaphore prevents this query.
            let rows = tokio::time::timeout(Duration::from_millis(100), client.query(&stmt, &[]))
                .await??;

            println!("rows count: {}", rows.len());
            for row in rows {
                println!("ROW!!! {row:?}");
            }

            println!("before execute");
            let start = Instant::now();
            let (affected_res, sema_elapsed) = tokio::join!(
                async {
                    let affected = client
                        .execute("INSERT INTO tests VALUES (1,2)", &[])
                        .await?;
                    Ok::<_, tokio_postgres::Error>((affected, start.elapsed()))
                },
                async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    drop(_permit);
                    start.elapsed()
                }
            );

            let (affected, exec_elapsed) = affected_res?;

            println!("after execute, affected: {affected}, sema elapsed: {sema_elapsed:?}, exec elapsed: {exec_elapsed:?}");

            assert_eq!(affected, 1);

            assert!(exec_elapsed > sema_elapsed);

            let row = client.query_one("SELECT * FROM crsql_changes", &[]).await?;
            println!("CHANGE ROW: {row:?}");

            let row = client
                .query_one("SELECT * FROM __corro_bookkeeping", &[])
                .await?;
            println!("BK ROW: {row:?}");

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

            let now: DateTime<Utc> = Utc::now();
            let now = NaiveDateTime::from_timestamp_micros(now.timestamp_micros()).unwrap();
            println!("NOW: {now:?}");

            let row = client
                .query_one(
                    "INSERT INTO kitchensink (other_ts, id, updated_at) VALUES (?1, ?2, ?1) RETURNING updated_at",
                    &[&now, &1i64],
                )
                .await?;

            println!("ROW: {row:?}");
            let updated_at = row.try_get::<_, NaiveDateTime>(0)?;
            println!("updated_at: {updated_at:?}");

            assert_eq!(now, updated_at);

            let future: DateTime<Utc> = Utc::now() + Duration::from_secs(1);
            let future = NaiveDateTime::from_timestamp_micros(future.timestamp_micros()).unwrap();
            println!("NOW: {future:?}");

            let row = client
                .query_one(
                    "UPDATE kitchensink SET other_ts = $ts, updated_at = $ts WHERE id = $id AND updated_at > ? RETURNING updated_at",
                    &[&future, &1i64, &(now - Duration::from_secs(1))],
                )
                .await?;

            println!("ROW: {row:?}");
            let updated_at = row.try_get::<_, NaiveDateTime>(0)?;
            println!("updated_at: {updated_at:?}");

            assert_eq!(future, updated_at);
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
