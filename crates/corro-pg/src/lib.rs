mod codec;
pub mod sql_state;
mod ssl;
pub mod utils;
mod vtab;

use eyre::WrapErr;
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fmt,
    net::SocketAddr,
    str::{FromStr, Utf8Error},
    sync::Arc,
    time::Duration,
};

use chrono::NaiveDateTime;
use codec::PgWireMessageServerCodec;
use compact_str::CompactString;
use corro_types::{
    agent::{Agent, ChangeError},
    broadcast::{broadcast_changes, Timestamp},
    change::{insert_local_changes, InsertChangesInfo},
    config::PgConfig,
    persistent_gauge,
    schema::{parse_sql, Column, Schema, SchemaError, SqliteType, Table},
    sqlite::CrConn,
};
use fallible_iterator::FallibleIterator;
use futures::{SinkExt, StreamExt};
use metrics::counter;
use pgwire::{
    api::results::{DataRowEncoder, FieldFormat, FieldInfo, Tag},
    error::{ErrorInfo, PgWireError},
    messages::{
        data::{NoData, ParameterDescription, RowDescription},
        extendedquery::{BindComplete, CloseComplete, ParseComplete, PortalSuspended},
        response::{
            CommandComplete, EmptyQueryResponse, ErrorResponse, ReadyForQuery, TransactionStatus,
        },
        startup::ParameterStatus,
        PgWireBackendMessage, PgWireFrontendMessage,
    },
};
use postgres_types::{FromSql, Type};
use rusqlite::{
    ffi::SQLITE_CONSTRAINT_UNIQUE, functions::FunctionFlags, types::ValueRef,
    vtab::eponymous_only_module, Connection, Statement,
};
use rustls::ServerConfig;
use socket2::{SockRef, TcpKeepalive};
use spawn::spawn_counted;
use sqlite3_parser::ast::{
    As, Cmd, ColumnDefinition, CreateTableBody, Expr, FromClause, Id, InsertBody, Limit, Literal,
    Name, OneSelect, ResultColumn, Select, SelectBody, SelectTable, Stmt, With,
};
use sqlparser::ast::Statement as PgStatement;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel, Sender},
        AcquireError, OwnedSemaphorePermit,
    },
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::{codec::Framed, either::Either, sync::CancellationToken};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, TimeoutFutureExt, Tripwire};

use crate::{
    sql_state::SqlState,
    utils::CountedTcpStream,
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
        let tag = match self {
            StmtTag::Select => Tag::new("SELECT"),
            StmtTag::InsertAsSelect | StmtTag::Insert => Tag::new("INSERT"),
            StmtTag::Update => Tag::new("UPDATE"),
            StmtTag::Delete => Tag::new("DELETE"),
            StmtTag::Alter => Tag::new("ALTER"),
            StmtTag::Analyze => Tag::new("ANALYZE"),
            StmtTag::Attach => Tag::new("ATTACH"),
            StmtTag::Begin => Tag::new("BEGIN"),
            StmtTag::Commit => Tag::new("COMMIT"),
            StmtTag::Create => Tag::new("CREATE"),
            StmtTag::Detach => Tag::new("DETACH"),
            StmtTag::Drop => Tag::new("DROP"),
            StmtTag::Pragma => Tag::new("PRAGMA"),
            StmtTag::Reindex => Tag::new("REINDEX"),
            StmtTag::Release => Tag::new("RELEASE"),
            StmtTag::Rollback => Tag::new("ROLLBACK"),
            StmtTag::Savepoint => Tag::new("SAVEPOINT"),
            StmtTag::Vacuum => Tag::new("VACUUM"),
            StmtTag::Other => Tag::new("OK"),
        };

        if let Some(r) = rows {
            tag.with_rows(r)
        } else {
            tag
        }
    }
}

enum Prepared {
    Empty,
    NonEmpty {
        sql: String,
        param_types: Vec<Type>,
        fields: Vec<FieldInfo>,
        cmd: Box<ParsedCmd>,
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
        cmd: Box<ParsedCmd>,
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
#[allow(clippy::large_enum_variant)]
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
        matches!(self, ParsedCmd::Postgres(PgStatement::Set { .. }))
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
                debug!("could not parse statement ({sql:?}) as sqlite: {e}");
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

#[derive(Debug, thiserror::Error)]
pub enum PgStartError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    PgTlsError(#[from] eyre::Error),
}

async fn setup_tls(pg: PgConfig) -> eyre::Result<(Option<TlsAcceptor>, bool)> {
    use eyre::ContextCompat as _;
    use rustls::pki_types::pem::PemObject as _;

    let tls = match pg.tls {
        Some(tls) => tls,
        None => {
            return Ok((None, false));
        }
    };

    let ssl_required = tls.verify_client;

    let key_data = tokio::fs::read(&tls.key_file).await?;
    let key = if tls.key_file.extension() == Some("der") {
        rustls::pki_types::PrivateKeyDer::try_from(key_data).map_err(|e| eyre::eyre!("{e}"))?
    } else {
        rustls::pki_types::PrivateKeyDer::from_pem_slice(&key_data)?
    };

    let certs = tokio::fs::read(&tls.cert_file).await?;
    let certs = if tls.cert_file.extension() == Some("der") {
        vec![rustls::pki_types::CertificateDer::from(certs)]
    } else {
        rustls::pki_types::CertificateDer::pem_slice_iter(&certs)
            .map(|res| res.wrap_err_with(|| format!("failed to read certs from {}", tls.key_file)))
            .collect::<eyre::Result<Vec<_>>>()?
    };

    let server_crypto = ServerConfig::builder();

    let server_crypto = if ssl_required {
        let ca_file = tls
            .ca_file
            .as_ref()
            .context("ca_file required in tls config for server client cert auth verification")?;

        let ca_certs = tokio::fs::read(&ca_file).await?;

        let mut root_store = rustls::RootCertStore::empty();

        if ca_file.extension() == Some("der") {
            root_store.add(rustls::pki_types::CertificateDer::from_slice(&ca_certs))?;
        } else {
            for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&ca_certs) {
                root_store
                    .add(cert.wrap_err_with(|| format!("failed to read certs from {ca_file}"))?)?;
            }
        }

        server_crypto.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store)).build()?,
        )
    } else {
        server_crypto.with_no_client_auth()
    };

    let config = server_crypto.with_single_cert(certs, key)?;
    Ok((Some(TlsAcceptor::from(Arc::new(config))), ssl_required))
}

pub async fn start(
    agent: Agent,
    pg: PgConfig,
    tripwire: Tripwire,
) -> Result<PgServer, PgStartError> {
    let readonly = pg.readonly;
    let server = TcpListener::bind(pg.bind_addr).await?;
    let (tls_acceptor, ssl_required) = setup_tls(pg).await?;
    let local_addr = server.local_addr()?;
    let conn_gauge = persistent_gauge!("corro.api.active.streams",
    "source" => "postgres",
    "protocol" => "pg",
    "readonly" => readonly.to_string(),
    );

    spawn_counted(async move {
        let mut conn_tripwire = tripwire.clone();
        loop {
            let (tcp_conn, remote_addr) =
                match server.accept().preemptible(&mut conn_tripwire).await {
                    Outcome::Completed(res) => res?,
                    Outcome::Preempted(_) => break,
                };
            let conn = CountedTcpStream::wrap(tcp_conn, conn_gauge.clone());
            let tls_acceptor = tls_acceptor.clone();
            debug!("Accepted a PostgreSQL connection (from: {remote_addr})");

            counter!("corro.api.connection.count", "protocol" => "pg", "readonly" => readonly.to_string()).increment(1);

            let agent = agent.clone();
            let tripwire = tripwire.clone();
            // Don't use spawn_counted here
            // Until the connection gets fully established we don't need to gracefully close it
            tokio::spawn(async move {
                conn.stream.set_nodelay(true)?;
                {
                    let sock = SockRef::from(&conn.stream);
                    let ka = TcpKeepalive::new()
                        .with_time(Duration::from_secs(10))
                        .with_interval(Duration::from_secs(10))
                        .with_retries(4);
                    sock.set_tcp_keepalive(&ka)?;
                }

                let mut tcp_socket = Framed::new(
                    tokio::io::BufStream::new(conn),
                    PgWireMessageServerCodec::new(codec::Client::new(local_addr, false)),
                );

                let negotiation =
                    ssl::negotiate_ssl(&mut tcp_socket, tls_acceptor.is_some()).await?;

                let (mut framed, secured) = if matches!(negotiation, ssl::SslNegotiationType::None)
                {
                    if ssl_required {
                        debug!("rejecting non-ssl connection");
                        return Ok(());
                    }

                    (Either::Left(tcp_socket), false)
                } else if let Some(tls) = tls_acceptor {
                    let tls_socket = tls.accept(tcp_socket.into_inner()).await?;

                    if matches!(negotiation, ssl::SslNegotiationType::Direct) {
                        ssl::check_alpn_for_direct_ssl(&tls_socket)?;
                    }

                    let framed = Framed::new(
                        tokio::io::BufStream::new(tls_socket),
                        PgWireMessageServerCodec::new(codec::Client::new(local_addr, true)),
                    );

                    (Either::Right(framed), true)
                } else {
                    trace!("received SSL connection attempt without a TLS acceptor configured");
                    return Ok(());
                };

                trace!("SSL ? {secured}");

                use crate::codec::SetState;
                framed.set_state(pgwire::api::PgWireConnectionState::AwaitingStartup);

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
                        TransactionStatus::Idle,
                    )))
                    .await?;

                framed.flush().await?;

                trace!("sent auth ok and ReadyForQuery");

                let (front_tx, mut front_rx) = channel(1024);
                let (back_tx, mut back_rx) = channel(1024);

                let (mut sink, mut stream) = framed.split();

                let cancel = CancellationToken::new();

                // If we're shutting down corrosion, both frontend and backend tasks will finish
                let mut frontend_task = spawn_counted({
                    // Use a weak sender here; it should NOT hold the backend channel (and half-connection) open
                    let back_tx = back_tx.clone().downgrade();
                    let cancel = cancel.clone();
                    let mut tripwire = tripwire.clone();
                    async move {
                        // cancel stuff if this loop breaks
                        let _drop_guard = cancel.drop_guard();

                        match async move {
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
                                        if let Some(back_tx) = back_tx.upgrade() {
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
                                        }
                                        break;
                                    }
                                };

                                front_tx.send(msg).await?;
                            }
                            Ok::<_, BoxError>(())
                        }
                        .preemptible(&mut tripwire)
                        .await
                        {
                            Outcome::Completed(res) => res?,
                            Outcome::Preempted(_) => {}
                        }
                        debug!("frontend stream is done");

                        Ok::<_, BoxError>(())
                    }
                });

                let mut backend_task = spawn_counted({
                    let cancel = cancel.clone();
                    let mut tripwire = tripwire.clone();
                    async move {
                        let _drop_guard = cancel.drop_guard();
                        match async {
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
                            Ok::<_, std::io::Error>(())
                        }
                        .preemptible(&mut tripwire)
                        .await
                        {
                            Outcome::Completed(res) => res?,
                            Outcome::Preempted(_) => {}
                        }
                        if tripwire.is_shutting_down() {
                            debug!("Closing connection due to corrosion shutdown");
                            // Give 1s for graceful shutdown of the connection
                            timeout(Duration::from_millis(1000), async move {
                                let _ = sink
                                    .feed(PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            sql_state::SqlState::ADMIN_SHUTDOWN.code().into(),
                                            "Corrosion is shutting down".into(),
                                        )
                                        .into(),
                                    ))
                                    .await;
                                let _ = sink.flush().await;
                                let _ = sink.close().await;
                            })
                            .await?;
                        } else {
                            debug!("Closing connection due to client disconnection");
                            // If we get here, we know that `back_rx` has been fully drained.
                            // Close the sink, this calls shutdown() on the underlying TCP socket
                            // If the other side behaves correctly, the frontend task will eventually receive an EOF
                            // and will also complete; by that point we know all messages have been sent successfully over TCP.
                            // However, if this is not handled correctly we time out later.
                            //
                            // If we are shutting down when the client disconnects, we just exit. Don't need to timeout here
                            let _ = sink.close().preemptible(&mut tripwire).await;
                        }
                        Ok::<_, std::io::Error>(())
                    }
                });

                let res = tokio::task::spawn_blocking({
                    let back_tx = back_tx.clone();
                    move || {
                        let conn = if readonly {
                            agent.pool().client_dedicated_readonly().unwrap()
                        } else {
                            agent.pool().client_dedicated().unwrap()
                        };
                        trace!("opened connection");

                        let int_handle = conn.get_interrupt_handle();
                        tokio::spawn(async move {
                            cancel.cancelled().await;
                            int_handle.interrupt();
                        });

                        conn.execute_batch("ATTACH ':memory:' AS pg_catalog;")?;

                        let dbs = Arc::new(vec![PgDatabase::new("state".into())]);

                        conn.create_module(
                            "pg_database",
                            eponymous_only_module::<PgDatabaseTable>(),
                            Some(dbs),
                        )?;
                        conn.create_module(
                            "pg_type",
                            eponymous_only_module::<PgTypeTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_range",
                            eponymous_only_module::<PgRangeTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_namespace",
                            eponymous_only_module::<PgNamespaceTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_class",
                            eponymous_only_module::<PgClassTable>(),
                            None,
                        )?;

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

                        let mut session = Session {
                            agent,
                            conn: &conn,
                            tx_state: TxState::default(),
                        };

                        let mut prepared: HashMap<CompactString, Prepared> = HashMap::new();

                        let mut portals: HashMap<CompactString, Portal> = HashMap::new();

                        let mut discard_until_sync = false;

                        'outer: while let Some(msg) = front_rx.blocking_recv() {
                            debug!("msg: {msg:?}");

                            if discard_until_sync
                                && !matches!(
                                    msg,
                                    PgWireFrontendMessage::Sync(_)
                                        | PgWireFrontendMessage::Flush(_)
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
                                    let name: &str = parse.name.as_deref().unwrap_or_default();
                                    let mut cmds = match parse_query(&parse.query) {
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

                                            let prepped = match session.conn.prepare(&parse.query) {
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
                                                .type_oids
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
                                                                    "BOOLEAN" | "BOOL" => {
                                                                        Type::BOOL
                                                                    }
                                                                    "DATETIME" => Type::TIMESTAMP,
                                                                    _ => Type::FLOAT8,
                                                                }
                                                            }
                                                            (SqliteType::Numeric, None) => {
                                                                Type::FLOAT8
                                                            }
                                                            (SqliteType::Integer, _src) => {
                                                                Type::INT8
                                                            }
                                                            (SqliteType::Real, _src) => {
                                                                Type::FLOAT8
                                                            }
                                                            (SqliteType::Blob, src) => match src {
                                                                Some("JSONB") => Type::JSONB,
                                                                _ => Type::BYTEA,
                                                            },
                                                        }
                                                    })
                                                    .collect();
                                            }

                                            let fields = match field_types(
                                                &prepped,
                                                &parsed_cmd,
                                                FieldFormats::All(FieldFormat::Text),
                                            ) {
                                                Ok(fields) => fields,
                                                Err(e) => {
                                                    back_tx
                                                        .blocking_send((e.into(), true).into())?;
                                                    discard_until_sync = true;
                                                    continue 'outer;
                                                }
                                            };

                                            prepared.insert(
                                                name.into(),
                                                Prepared::NonEmpty {
                                                    sql: parse.query.clone(),
                                                    param_types,
                                                    fields,
                                                    cmd: Box::new(parsed_cmd),
                                                },
                                            );
                                        }
                                    }

                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ParseComplete(
                                                ParseComplete::new(),
                                            ),
                                            false,
                                        )
                                            .into(),
                                    )?;
                                }
                                PgWireFrontendMessage::Describe(desc) => {
                                    let name = desc.name.as_deref().unwrap_or_default();
                                    match desc.target_type {
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
                                                                fields
                                                                    .iter()
                                                                    .map(Into::into)
                                                                    .collect(),
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
                                                cmd,
                                                ..
                                            }) => {
                                                let fields = match field_types(
                                                    stmt,
                                                    cmd,
                                                    FieldFormats::Each(result_formats),
                                                ) {
                                                    Ok(fields) => fields,
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

                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::RowDescription(
                                                            RowDescription::new(
                                                                fields
                                                                    .iter()
                                                                    .map(Into::into)
                                                                    .collect(),
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
                                                            SqlState::PROTOCOL_VIOLATION
                                                                .code()
                                                                .into(),
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
                                        .portal_name
                                        .as_deref()
                                        .map(CompactString::from)
                                        .unwrap_or_default();

                                    let stmt_name = bind.statement_name.as_deref().unwrap_or_default();

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
                                            let mut prepped = match session.conn.prepare(sql) {
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
                                                bind.parameters.len(),
                                                prepped.parameter_count()
                                            );

                                            debug!("bind param types: {param_types:?}");

                                            let mut format_codes = match bind
                                            .parameter_format_codes
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
                                                    vec![FormatCode::Text; bind.parameters.len()];
                                            } else if format_codes.len() == 1 {
                                                // single code means we should use it for all others
                                                format_codes =
                                                    vec![format_codes[0]; bind.parameters.len()];
                                            }

                                            for (i, param) in bind.parameters.iter().enumerate() {
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
                                                                        "missing parameter type"
                                                                            .into(),
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
                                                                let value: bool =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT2 => {
                                                                let value: i16 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT4 => {
                                                                let value: i32 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT8 => {
                                                                let value: i64 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }

                                                            t @ &Type::TEXT
                                                            | t @ &Type::VARCHAR
                                                            | t @ &Type::JSON => {
                                                                let value: &str = match format_code
                                                                {
                                                                    FormatCode::Text => {
                                                                        std::str::from_utf8(b)?
                                                                    }
                                                                    FormatCode::Binary => {
                                                                        FromSql::from_sql(t, b)?
                                                                    }
                                                                };
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::FLOAT4 => {
                                                                let value: f32 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::FLOAT8 => {
                                                                let value: f64 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
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
                                                                prepped
                                                                    .raw_bind_parameter(idx, dt)?;
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
                                                        .result_column_format_codes
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
                                    send_ready(&mut session, discard_until_sync, &back_tx)?;

                                    // reset this
                                    discard_until_sync = false;
                                }
                                PgWireFrontendMessage::Execute(execute) => {
                                    let name = execute.name.as_deref().unwrap_or_default();
                                    let (prepped, result_formats, cmd) = match portals.get_mut(name)
                                    {
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

                                    let max_rows = if execute.max_rows <= 0 {
                                        usize::MAX
                                    } else {
                                        execute.max_rows as usize
                                    };

                                    if let Err(e) = session.handle_execute(
                                        prepped,
                                        result_formats,
                                        cmd,
                                        max_rows,
                                        &back_tx,
                                    ) {
                                        debug!("error in execute: {e}");

                                        back_tx.blocking_send(BackendResponse::Message {
                                            message: e.try_into()?,
                                            flush: true,
                                        })?;

                                        discard_until_sync = true;

                                        send_ready(
                                            &mut session,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue;
                                    }
                                }
                                PgWireFrontendMessage::Query(query) => {
                                    let parsed_query = match parse_query(&query.query) {
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

                                        send_ready(
                                            &mut session,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue;
                                    }

                                    for cmd in parsed_query.into_iter() {
                                        if let Err(e) =
                                            session.handle_query(&cmd, &back_tx, true)
                                        {
                                            back_tx.blocking_send(BackendResponse::Message {
                                                message: e.try_into()?,
                                                flush: true,
                                            })?;
                                            send_ready(
                                                &mut session,
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

                                        if let Err(e) = session.handle_commit() {
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
                                                discard_until_sync,
                                                &back_tx,
                                            )?;
                                            continue;
                                        }
                                        trace!("committed IMPLICIT tx");
                                    }

                                    send_ready(&mut session, discard_until_sync, &back_tx)?;
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
                                    let name = close.name.as_deref().unwrap_or_default();
                                    match close.target_type {
                                        // statement
                                        b'S' => {
                                            if prepared.remove(name).is_some() {
                                                portals
                                                    .retain(|_, portal| portal.stmt_name() != name);
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
                                                            SqlState::PROTOCOL_VIOLATION
                                                                .code()
                                                                .into(),
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
                                PgWireFrontendMessage::CancelRequest(_) => {
                                    // cancel.cancel(); ?
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "Cancel is not implemented".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::GssEncRequest(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::GssEncResponse(pgwire::messages::response::GssEncResponse::Refuse), false
                                        ).into())?;
                                    continue;
                                }
                                PgWireFrontendMessage::SslRequest(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "SslRequest is not implemented".into(),
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
                    }
                }).await;

                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
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
                    Err(e) => {
                        error!("spawn_blocking failed: {e}");
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
                }

                // The message-handling loop has completed, make sure we also abort the tasks
                // handling the TCP connection
                // Firstly we attempt a graceful shutdown -- dropping back_tx will cause
                // backend_task to complete once it writes all content to the TCP socket
                // Then, frontend_task will eventually receive an EOF if clients behave properly
                // Note that this should be the only reference of back_tx at this point:
                // the one in frontend_task is weak, and the one cloned into the message-handling
                // thread should have been dropped.
                assert_eq!(back_tx.strong_count(), 1);
                drop(back_tx);

                // Now we wait for both front and back to complete; if however frontend_task never
                // receives an EOF, instead of relying on half-open timeout we just abort both tasks
                // after 1 minute.
                match async { tokio::join!(&mut frontend_task, &mut backend_task) }
                    .with_timeout(Duration::from_secs(60))
                    .await
                {
                    Outcome::Preempted(_) => {
                        frontend_task.abort();
                        backend_task.abort();
                    }
                    Outcome::Completed(_) => {}
                }

                Ok::<_, BoxError>(())
            });
        }

        info!("postgres server done");

        Ok::<_, BoxError>(())
    });

    Ok(PgServer { local_addr })
}

struct Session<'conn> {
    agent: Agent,
    conn: &'conn CrConn,
    tx_state: TxState,
}

impl<'conn> Session<'conn> {
    fn handle_query(
        &mut self,
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
            self.conn.execute_batch("BEGIN")?;
            trace!("started IMPLICIT tx");
            self.tx_state.start_implicit();
        } else if self.tx_state.is_implicit() && cmd.is_begin() {
            trace!("committing IMPLICIT tx");
            let _permit = self.tx_state.end();

            self.handle_commit()?;
            trace!("committed IMPLICIT tx");
        }

        let tag = cmd.tag();

        let mut changes = 0usize;

        let count = if cmd.is_begin() {
            self.conn.execute_batch("BEGIN")?;
            self.tx_state.start_explicit();
            0
        } else if cmd.is_commit() {
            let _permit = self.tx_state.end();
            self.handle_commit()?;
            0
        } else if cmd.is_rollback() {
            let _permit = self.tx_state.end();
            self.conn.execute_batch("ROLLBACK")?;
            0
        } else {
            let mut prepped = if cmd.is_pg() {
                return Err(QueryError::NotSqlite);
            } else {
                self.conn.prepare(&cmd.to_string())?
            };

            let fields = field_types(&prepped, cmd, FieldFormats::All(FieldFormat::Text))?;

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

                counter!("corro.acquired.write.permit.count", "protocol" => "pg").increment(1);
                self.set_ts()?;
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
                changes = self.conn.changes() as usize;
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
    fn handle_execute(
        &mut self,
        prepped: &mut Statement<'conn>,
        result_formats: &[FieldFormat],
        cmd: &ParsedCmd,
        max_rows: usize,
        back_tx: &Sender<BackendResponse>,
    ) -> Result<(), QueryError> {
        // TODO: maybe we don't need to recompute this...
        let fields = field_types(prepped, cmd, FieldFormats::Each(result_formats))?;

        trace!("fields: {fields:?}");

        let schema = Arc::new(fields);

        // we need to know because we'll commit it right away
        let mut opened_implicit_tx = false;

        if self.tx_state.is_ended() {
            debug!("tx is_ended");
            if !cmd.is_begin() && !prepped.readonly() {
                debug!("tx is_ended && !cmd.is_begin() && !prepped.readonly()");
                // NOT in a tx and statement mutates DB...
                self.conn.execute_batch("BEGIN")?;

                self.tx_state.start_implicit();
                opened_implicit_tx = true;
            } else if cmd.is_begin() {
                debug!("cmd is BEGIN");
                self.conn.execute_batch("BEGIN")?;
                self.tx_state.start_explicit();
                debug!("started EXPLICIT tx");
            }
        }

        let tag = cmd.tag();

        let mut count = 0;
        let mut changes = 0usize;

        if cmd.is_commit() {
            let _permit = self.tx_state.end();
            self.handle_commit()?;
        } else if cmd.is_begin() {
            // do nothing
            debug!("cmd is BEGIN");
        } else {
            if !self.tx_state.is_writing() && !prepped.readonly() {
                trace!("statement writes, acquiring permit...");
                self.tx_state
                    .set_write_permit(self.agent.write_permit_blocking()?);

                self.set_ts()?;
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
                    let format = field.format();
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
                            return Err(
                                UnsupportedSqliteToPostgresType(field.name().to_owned()).into()
                            )
                        }
                    }
                }

                let data_row = encoder.finish()?;
                back_tx
                    .blocking_send((PgWireBackendMessage::DataRow(data_row), false).into())
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            if tag.returns_rows_affected() {
                changes = self.conn.changes() as usize;
            }

            if opened_implicit_tx {
                let _permit = self.tx_state.end();
                self.handle_commit()?;
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

    fn handle_commit(&self) -> Result<(), ChangeError> {
        trace!("HANDLE COMMIT");

        let mut book_writer = self
            .agent
            .booked()
            .blocking_write::<&str, _>("handle_write_tx(book_writer)", None);

        let actor_id = self.agent.actor_id();

        let insert_info = insert_local_changes(&self.agent, self.conn, &mut book_writer)?;
        self.conn
            .execute_batch("COMMIT")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        if let Some(InsertChangesInfo {
            db_version,
            last_seq,
            ts,
            snap,
        }) = insert_info
        {
            trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

            book_writer.commit_snapshot(snap);

            let agent = self.agent.clone();

            spawn_counted(async move { broadcast_changes(agent, db_version, last_seq, ts).await });
        }

        Ok(())
    }

    fn set_ts(&self) -> Result<(), rusqlite::Error> {
        let ts = Timestamp::from(self.agent.clock().new_timestamp());

        let _ = self
            .conn
            .prepare_cached("SELECT crsql_set_ts(?)")?
            .query_row([&ts], |row| row.get::<_, String>(0))?;

        Ok(())
    }
}

impl<'conn> Drop for Session<'conn> {
    fn drop(&mut self) {
        if !self.tx_state.is_ended() {
            let _permit = self.tx_state.end();
            if let Err(e) = self.conn.execute_batch("ROLLBACK") {
                warn!("failed to rollback tx: {e}");
            } else {
                debug!("rolled back tx");
            }
        }
    }
}

fn send_ready(
    session: &mut Session,
    discard_until_sync: bool,
    back_tx: &Sender<BackendResponse>,
) -> Result<(), BoxError> {
    let ready_status = if session.tx_state.is_implicit() {
        let _permit = session.tx_state.end(); // do this first, in case of failure
        if discard_until_sync {
            // an error occured, rollback implicit tx!
            warn!("receive Sync message w/ an error to send, rolling back implicit tx");
            session.conn.execute_batch("ROLLBACK")?;
        } else {
            // no error, commit implicit tx
            warn!("receive Sync message, committing implicit tx");
            session.handle_commit()?;
        }

        TransactionStatus::Idle
    } else if session.tx_state.is_explicit() {
        if discard_until_sync {
            TransactionStatus::Error
        } else {
            TransactionStatus::Transaction
        }
    } else {
        TransactionStatus::Idle
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
            QueryError::Rusqlite(e) => match &e {
                rusqlite::Error::SqliteFailure(sqlite_error, _maybe_sqlite_message)
                    if sqlite_error.extended_code == SQLITE_CONSTRAINT_UNIQUE =>
                {
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        SqlState::UNIQUE_VIOLATION.code().into(),
                        e.to_string(),
                    )
                    .into()
                }
                _ => ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into(),
            },
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

fn compute_schema(conn: &Connection) -> Result<Schema, Box<SchemaError>> {
    fn dump_sql(conn: &Connection) -> Result<String, rusqlite::Error> {
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

        Ok(dump)
    }

    let dump = dump_sql(conn).map_err(|err| Box::new(SchemaError::from(err)))?;
    parse_sql(&dump)
}

#[derive(Debug)]
enum ParamKind<'a> {
    Named(&'a str),
    Positional,
}

fn as_param(expr: &Expr) -> Option<ParamKind<'_>> {
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
#[allow(dead_code)]
enum SqliteName {
    Id(Id),
    Name(Name),
    Qualified(Name, Name),
    DoublyQualified(Name, Name, Name),
}

fn expr_to_name(expr: &Expr) -> Option<SqliteNameRef<'_>> {
    match expr {
        Expr::Id(id) => Some(SqliteNameRef::Id(id)),
        Expr::Name(name) => Some(SqliteNameRef::Name(name)),
        Expr::Qualified(n0, n1) => Some(SqliteNameRef::Qualified(n0, n1)),
        Expr::DoublyQualified(n0, n1, n2) => Some(SqliteNameRef::DoublyQualified(n0, n1, n2)),
        _ => None,
    }
}

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

enum FieldFormats<'a> {
    All(FieldFormat),
    Each(&'a [FieldFormat]),
}

impl<'a> FieldFormats<'a> {
    fn get(&self, i: usize) -> FieldFormat {
        match self {
            FieldFormats::All(format) => *format,
            FieldFormats::Each(formats) => formats.get(i).copied().unwrap_or(FieldFormat::Text),
        }
    }
}

fn field_types(
    prepped: &Statement,
    parsed_cmd: &ParsedCmd,
    field_formats: FieldFormats<'_>,
) -> Result<Vec<FieldInfo>, UnsupportedSqliteToPostgresType> {
    let mut field_type_overrides = HashMap::new();

    match parsed_cmd {
        ParsedCmd::Sqlite(Cmd::Stmt(
            Stmt::Select(Select {
                body:
                    SelectBody {
                        select: OneSelect::Select { columns: cols, .. },
                        ..
                    },
                ..
            })
            | Stmt::Delete {
                returning: Some(cols),
                ..
            }
            | Stmt::Insert {
                returning: Some(cols),
                ..
            }
            | Stmt::Update {
                returning: Some(cols),
                ..
            },
        )) => {
            for (i, col) in cols.iter().enumerate() {
                if let ResultColumn::Expr(expr, _as) = col {
                    let type_override = match expr {
                        Expr::Cast { type_name, .. } => Some(name_to_type(&type_name.name)?),
                        Expr::FunctionCall { name, .. } | Expr::FunctionCallStar { name, .. } => {
                            match name.0.as_str().to_uppercase().as_ref() {
                                "COUNT" => Some(Type::INT8),
                                _ => None,
                            }
                        }
                        Expr::Literal(lit) => match lit {
                            Literal::Numeric(s) => Some(if s.contains('.') {
                                Type::FLOAT8
                            } else {
                                Type::INT8
                            }),
                            Literal::String(_) => Some(Type::TEXT),
                            Literal::Blob(_) => Some(Type::BYTEA),
                            Literal::Keyword(_) => None,
                            Literal::Null => None,
                            Literal::CurrentDate => Some(Type::DATE),
                            Literal::CurrentTime => Some(Type::TIME),
                            Literal::CurrentTimestamp => Some(Type::TIMESTAMP),
                        },
                        _ => None,
                    };
                    if let Some(type_override) = type_override {
                        match prepped.column_name(i) {
                            Ok(col_name) => {
                                field_type_overrides.insert(col_name, type_override);
                            }
                            Err(e) => {
                                error!("col index didn't exist at {i}, attempted to override type as: {type_override}: {e}");
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
        ParsedCmd::Postgres(_stmt) => {
            // TODO: handle type overrides here too
            // let cols = match stmt {
            //     PgStatement::Insert { returning, .. }
            //     | PgStatement::Update { returning, .. }
            //     | PgStatement::Delete { returning, .. } => {
            //         returning
            //     }
            //     PgStatement::Query(query) => {
            //         match *query.body {
            //             sqlparser::ast::SetExpr::Select(
            //                 select,
            //             ) => Some(select.projection),
            //             _ => None,
            //         }
            //     }
            //     _ => None,
            // };

            // if let Some(cols) = cols {

            // }
        }
        _ => {}
    }

    let mut fields = vec![];
    for (i, col) in prepped.columns().iter().enumerate() {
        let col_name = col.name();
        let col_type = match field_type_overrides.remove(col_name) {
            Some(t) => t,
            None => match col.decl_type() {
                None => Type::TEXT,
                Some(decl_type) => name_to_type(decl_type)?,
            },
        };
        fields.push(FieldInfo::new(
            col_name.to_string(),
            None,
            None,
            col_type,
            field_formats.get(i),
        ));
    }

    Ok(fields)
}
