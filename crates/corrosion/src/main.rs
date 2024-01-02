use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use admin::AdminConn;
use bytes::BytesMut;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use command::{
    tls::{generate_ca, generate_client_cert, generate_server_cert},
    tpl::TemplateFlags,
};
use corro_api_types::SqliteParam;
use corro_client::CorrosionApiClient;
use corro_types::{
    api::{ExecResult, QueryEvent, Statement},
    config::{default_admin_path, Config, ConfigError, LogFormat, OtelConfig},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use opentelemetry::{
    global,
    sdk::{
        propagation::TraceContextPropagator,
        trace::{self, BatchConfig},
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use rusqlite::{Connection, OptionalExtension};
use tokio_util::codec::{Decoder, LinesCodec};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{
    fmt::format::Format, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};
use uuid::Uuid;

pub mod admin;
pub mod command;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub static CONFIG: OnceCell<Config> = OnceCell::new();
pub static API_CLIENT: OnceCell<CorrosionApiClient> = OnceCell::new();

build_info::build_info!(pub fn version);

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn init_tracing(cli: &Cli) -> Result<(), ConfigError> {
    if matches!(cli.command, Command::Agent) {
        let config = cli.config()?;

        let directives = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
        let (filter, diags) = tracing_filter::legacy::Filter::parse(&directives);
        if let Some(diags) = diags {
            eprintln!("While parsing env filters: {diags}, using default");
        }

        global::set_text_map_propagator(TraceContextPropagator::new());

        // Tracing
        let (env_filter, _handle) = tracing_subscriber::reload::Layer::new(filter.layer());

        let sub = tracing_subscriber::registry::Registry::default().with(env_filter);

        if let Some(otel) = &config.telemetry.open_telemetry {
            let otlp_exporter = opentelemetry_otlp::new_exporter().tonic().with_env();
            let otlp_exporter = match otel {
                OtelConfig::FromEnv => otlp_exporter,
                OtelConfig::Exporter { endpoint } => otlp_exporter.with_endpoint(endpoint),
            };

            let batch_config = BatchConfig::default().with_max_queue_size(10240);

            let trace_config = trace::config().with_resource(Resource::new([
                KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    "corrosion",
                ),
                KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                    VERSION,
                ),
                KeyValue::new(
                    opentelemetry_semantic_conventions::resource::HOST_NAME,
                    hostname::get().unwrap().to_string_lossy().into_owned(),
                ),
            ]));

            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(otlp_exporter)
                .with_trace_config(trace_config)
                .with_batch_config(batch_config)
                .install_batch(opentelemetry::runtime::Tokio)
                .expect("Failed to initialize OpenTelemetry OTLP exporter.");

            let sub = sub.with(tracing_opentelemetry::layer().with_tracer(tracer));
            match config.log.format {
                LogFormat::Plaintext => {
                    sub.with(tracing_subscriber::fmt::Layer::new().with_ansi(config.log.colors))
                        .init();
                }
                LogFormat::Json => {
                    sub.with(
                        tracing_subscriber::fmt::Layer::new()
                            .json()
                            .with_span_list(false),
                    )
                    .init();
                }
            }
        } else {
            match config.log.format {
                LogFormat::Plaintext => {
                    sub.with(tracing_subscriber::fmt::Layer::new().with_ansi(config.log.colors))
                        .init();
                }
                LogFormat::Json => {
                    sub.with(
                        tracing_subscriber::fmt::Layer::new()
                            .json()
                            .with_span_list(false),
                    )
                    .init();
                }
            }
        }
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().event_format(Format::default().without_time()))
            .with(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("info"))
                    .unwrap(),
            )
            .init();
    }

    Ok(())
}

async fn process_cli(cli: Cli) -> eyre::Result<()> {
    init_tracing(&cli)?;

    match &cli.command {
        Command::Agent => command::agent::run(cli.config()?, &cli.config_path).await?,

        Command::Backup { path } => {
            let db_path = cli.db_path()?;

            {
                let conn = Connection::open(&db_path)?;
                conn.execute("VACUUM INTO ?;", [&path])?;
            }

            {
                let path = PathBuf::from(path);

                // make sure parent path exists
                if let Some(parent) = path.parent() {
                    _ = tokio::fs::create_dir_all(parent).await;
                }

                let conn = Connection::open(&path)?;

                let site_id: [u8; 16] = conn.query_row(
                    "DELETE FROM crsql_site_id WHERE ordinal = 0 RETURNING site_id;",
                    [],
                    |row| row.get(0),
                )?;

                let new_ordinal: i64 = conn.query_row(
                    "INSERT INTO crsql_site_id (site_id) VALUES (?) RETURNING ordinal;",
                    [&site_id],
                    |row| row.get(0),
                )?;

                let tables: Vec<String> = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'")?.query_map([], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;

                for table in tables {
                    let n = conn.execute(
                        &format!("UPDATE \"{table}\" SET site_id = ? WHERE site_id = 0"),
                        [new_ordinal],
                    )?;
                    debug!("updated {n} rows in {table}");
                }

                // clear __corro_members, this state is per actor
                conn.execute("DELETE FROM __corro_members;", [])?;

                // clear __corro_subs, this state is per actor
                if let Err(e) = conn.execute("DELETE FROM __corro_subs;", []) {
                    warn!(error = %e,
                        "could not clear __corro_subs table, possibly because it was never created"
                    );
                }

                if let Err(e) = conn.execute_batch(
                    "DROP TABLE __corro_consul_services; DROP TABLE __corro_consul_checks;",
                ) {
                    warn!(error = %e, "could not drop consul services and checks hash tables, probably because they were never created");
                }

                conn.execute_batch(
                    r#"
                    PRAGMA journal_mode = WAL; -- so the restore can be done online
                    PRAGMA wal_checkpoint(TRUNCATE);
                    "#,
                )?;
            }

            info!("Successfully cleaned for restoration and backed up database to {path}");
        }
        Command::Restore {
            path,
            self_actor_id,
            actor_id,
        } => {
            if AdminConn::connect(cli.admin_path()).await.is_ok() {
                eyre::bail!("corrosion is currently running, shut it down before restoring!");
            }

            if *self_actor_id || actor_id.is_some() {
                let site_id: [u8; 16] = {
                    if let Some(actor_id) = actor_id {
                        actor_id.to_bytes_le()
                    } else {
                        let db_path = match cli.db_path() {
                            Ok(db_path) => db_path,
                            Err(_e) => {
                                eyre::bail!(
                                    "path to current database is required when passing --self-actor-id"
                                );
                            }
                        };

                        let conn = Connection::open(db_path)?;
                        conn.query_row(
                            "SELECT site_id FROM crsql_site_id WHERE ordinal = 0;",
                            [],
                            |row| row.get(0),
                        )?
                    }
                };

                let conn = Connection::open(path)?;

                let ordinal: Option<i64> = conn
                    .query_row(
                        "DELETE FROM crsql_site_id WHERE site_id = ? RETURNING ordinal",
                        [site_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if ordinal.is_none() {
                    warn!("snapshot database did not know about the current actor id");
                }

                // FIXME: find the old site_id for ordinal 0 and fix crsql_clock tables
                let inserted = conn.execute(
                    "INSERT OR REPLACE INTO crsql_site_id (ordinal, site_id) VALUES (0, ?)",
                    [site_id],
                )?;
                if inserted != 1 {
                    eyre::bail!("could not insert old site id into crsql_site_id table");
                }

                if let Some(ordinal) = ordinal {
                    let tables: Vec<String> = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'")?.query_map([], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;

                    for table in tables {
                        let n = conn.execute(
                            &format!("UPDATE \"{table}\" SET site_id = 0 WHERE site_id = ?"),
                            [ordinal],
                        )?;
                        info!("Updated {n} rows in {table}");
                    }
                }
            }

            let restored =
                sqlite3_restore::restore(path, cli.config()?.db.path, Duration::from_secs(30))?;

            info!(
                "successfully restored! old size: {}, new size: {}",
                restored.old_len, restored.new_len
            );
        }
        Command::Cluster(ClusterCommand::MembershipStates) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Cluster(
                corro_admin::ClusterCommand::MembershipStates,
            ))
            .await?;
        }
        Command::Consul(cmd) => match cmd {
            ConsulCommand::Sync => match cli.config()?.consul.as_ref() {
                Some(consul) => {
                    command::consul::sync::run(consul, cli.api_addr()?, cli.db_path()?).await?
                }
                None => {
                    error!("missing `consul` block in corrosion config");
                }
            },
        },
        Command::Query {
            query,
            columns: show_columns,
            timer,
            param,
        } => {
            let stmt = if param.is_empty() {
                Statement::Simple(query.clone())
            } else {
                Statement::WithParams(
                    query.clone(),
                    param.iter().map(|p| SqliteParam::Text(p.into())).collect(),
                )
            };

            let mut body = cli.api_client()?.query(&stmt).await?;

            let mut lines = LinesCodec::new();

            let mut buf = BytesMut::new();

            loop {
                buf.extend_from_slice(&body.next().await.unwrap()?);
                let s = match lines.decode(&mut buf)? {
                    Some(s) => s,
                    None => break,
                };
                let res: QueryEvent = serde_json::from_str(&s)?;

                match res {
                    QueryEvent::Columns(cols) => {
                        if *show_columns {
                            println!("{}", cols.join("|"));
                        }
                    }
                    QueryEvent::Row(_, cells) => {
                        println!(
                            "{}",
                            cells
                                .iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join("|")
                        );
                    }
                    QueryEvent::EndOfQuery { time, .. } => {
                        if *timer {
                            println!("time: {time}s");
                        }
                    }
                    QueryEvent::Change(_, _, _, _) => {
                        break;
                    }
                    QueryEvent::Error(e) => {
                        eyre::bail!("{e}");
                    }
                }
            }
        }
        Command::Exec {
            query,
            param,
            timer,
        } => {
            let stmt = if param.is_empty() {
                Statement::Simple(query.clone())
            } else {
                Statement::WithParams(
                    query.clone(),
                    param.iter().map(|p| SqliteParam::Text(p.into())).collect(),
                )
            };

            let res = cli.api_client()?.execute(&[stmt]).await?;

            for res in res.results {
                match res {
                    ExecResult::Execute {
                        rows_affected,
                        time,
                    } => {
                        info!("Rows affected: {rows_affected}");
                        if *timer {
                            println!("Run Time: real {time}");
                        }
                    }
                    ExecResult::Error { error } => {
                        error!("{error}");
                    }
                }
            }
        }
        Command::Reload => {
            command::reload::run(cli.api_addr()?, &cli.config()?.db.schema_paths).await?
        }
        Command::Sync(SyncCommand::Generate) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Sync(
                corro_admin::SyncCommand::Generate,
            ))
            .await?;
        }
        Command::Locks { top } => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Locks { top: *top })
                .await?;
        }
        Command::Template { template, flags } => {
            command::tpl::run(cli.api_addr()?, template, flags).await?;
        }
        Command::Tls(tls) => match tls {
            TlsCommand::Ca(TlsCaCommand::Generate) => generate_ca(std::env::current_dir()?).await?,
            TlsCommand::Server(TlsServerCommand::Generate {
                ip,
                ca_key,
                ca_cert,
            }) => generate_server_cert(ca_cert, ca_key, *ip).await?,
            TlsCommand::Client(TlsClientCommand::Generate { ca_key, ca_cert }) => {
                generate_client_cert(ca_cert, ca_key).await?
            }
        },
    }

    Ok(())
}

fn main() {
    let cli: Cli = Cli::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("could not build tokio runtime");

    if let Err(e) = rt.block_on(process_cli(cli)) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

#[derive(Parser)]
#[clap(version = VERSION)]
struct Cli {
    /// Set the config file path
    #[clap(
        long = "config",
        short,
        global = true,
        default_value = "/etc/corrosion/config.toml"
    )]
    config_path: Utf8PathBuf,

    #[clap(long, global = true)]
    api_addr: Option<SocketAddr>,

    #[clap(long, global = true)]
    db_path: Option<Utf8PathBuf>,

    #[clap(long, global = true)]
    admin_path: Option<Utf8PathBuf>,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    fn api_client(&self) -> Result<CorrosionApiClient, ConfigError> {
        API_CLIENT
            .get_or_try_init(|| Ok(CorrosionApiClient::new(self.api_addr()?)))
            .cloned()
    }

    fn api_addr(&self) -> Result<SocketAddr, ConfigError> {
        Ok(if let Some(api_addr) = self.api_addr {
            api_addr
        } else {
            self.config()?.api.bind_addr
        })
    }

    fn db_path(&self) -> Result<Utf8PathBuf, ConfigError> {
        Ok(if let Some(ref db_path) = self.db_path {
            db_path.clone()
        } else {
            self.config()?.db.path
        })
    }

    fn admin_path(&self) -> Utf8PathBuf {
        if let Some(ref admin_path) = self.admin_path {
            admin_path.clone()
        } else if let Ok(config) = Config::load(self.config_path.as_str()) {
            config.admin.uds_path
        } else {
            default_admin_path()
        }
    }

    fn config(&self) -> Result<Config, ConfigError> {
        CONFIG
            .get_or_try_init(|| {
                let config_path = &self.config_path;
                Config::load(config_path.as_str())
            })
            .cloned()
    }
}

#[derive(Subcommand)]
enum Command {
    /// Launches the agent
    Agent,

    /// Backup the Corrosion DB
    Backup {
        path: String,
    },

    /// Restore the Corrosion DB from a backup
    Restore {
        path: PathBuf,
        #[arg(long, default_value = "false")]
        self_actor_id: bool,
        #[arg(long)]
        actor_id: Option<Uuid>,
    },

    /// Cluster interactions
    #[command(subcommand)]
    Cluster(ClusterCommand),

    /// Consul interactions
    #[command(subcommand)]
    Consul(ConsulCommand),

    /// Query data from Corrosion w/ a SQL statement
    Query {
        query: String,
        #[arg(long, default_value = "false")]
        columns: bool,
        #[arg(long, default_value = "false")]
        timer: bool,

        #[arg(long)]
        param: Vec<String>,
    },

    /// Execute a SQL statement that mutates the state of Corrosion
    Exec {
        query: String,
        #[arg(long)]
        param: Vec<String>,
        #[arg(long, default_value = "false")]
        timer: bool,
    },

    /// Reload the config
    Reload,

    /// Sync-related commands
    #[command(subcommand)]
    Sync(SyncCommand),

    Locks {
        top: usize,
    },

    Template {
        template: Vec<String>,
        #[command(flatten)]
        flags: TemplateFlags,
    },

    /// Tls-related commands
    #[command(subcommand)]
    Tls(TlsCommand),
}

#[derive(Subcommand)]
enum ClusterCommand {
    /// Dumps the current member states
    MembershipStates,
}

#[derive(Subcommand)]
enum ConsulCommand {
    /// Synchronizes the local consul agent with Corrosion
    Sync,
}

#[derive(Subcommand)]
enum SyncCommand {
    /// Generate a sync message from the current agent
    Generate,
}

#[derive(Subcommand)]
enum TlsCommand {
    /// TLS certificate authority commands
    #[command(subcommand)]
    Ca(TlsCaCommand),
    /// TLS server certificate commands
    #[command(subcommand)]
    Server(TlsServerCommand),
    /// TLS client certificate commands (for mutual TLS)
    #[command(subcommand)]
    Client(TlsClientCommand),
}

#[derive(Subcommand)]
enum TlsCaCommand {
    /// Generate a TLS certificate authority
    Generate,
}

#[derive(Subcommand)]
enum TlsServerCommand {
    /// Generate a TLS server certificate from a CA
    Generate {
        ip: IpAddr,
        #[arg(long)]
        ca_key: Utf8PathBuf,
        #[arg(long)]
        ca_cert: Utf8PathBuf,
    },
}

#[derive(Subcommand)]
enum TlsClientCommand {
    /// Generate a TLS certificate from a CA
    Generate {
        #[arg(long)]
        ca_key: Utf8PathBuf,
        #[arg(long)]
        ca_cert: Utf8PathBuf,
    },
}
