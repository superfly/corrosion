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
use corro_client::CorrosionApiClient;
use corro_types::{
    api::{QueryEvent, RqliteResult, Statement},
    config::{default_admin_path, Config, ConfigError, LogFormat},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use rusqlite::Connection;
use tokio_util::codec::{Decoder, LinesCodec};
use tracing::{debug, error, info};
use tracing_subscriber::{
    fmt::format::Format, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

pub mod admin;
pub mod command;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const CONFIG: OnceCell<Config> = OnceCell::new();
pub const API_CLIENT: OnceCell<CorrosionApiClient> = OnceCell::new();

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn init_tracing(cli: &Cli) -> Result<(), ConfigError> {
    if matches!(cli.command, Command::Agent) {
        let config = cli.config()?;

        let directives = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
        println!("tracing-filter directives: {directives}");
        let (filter, diags) = tracing_filter::legacy::Filter::parse(&directives);
        if let Some(diags) = diags {
            eprintln!("While parsing env filters: {diags}, using default");
        }

        // Tracing
        let (env_filter, _handle) = tracing_subscriber::reload::Layer::new(filter.layer());

        let sub = tracing_subscriber::registry::Registry::default().with(env_filter);

        match config.log.format {
            LogFormat::Plaintext => {
                sub.with(tracing_subscriber::fmt::Layer::new().with_ansi(config.log.colors))
                    .init();
            }
            LogFormat::Json => {
                sub.with(tracing_subscriber::fmt::Layer::new().json())
                    .init();
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

                let ordinal: i64 = conn.query_row(
                    "INSERT INTO crsql_site_id (site_id) VALUES (?) RETURNING ordinal;",
                    [&site_id],
                    |row| row.get(0),
                )?;

                let tables: Vec<String> = conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'")?.query_map([], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;

                for table in tables {
                    let n = conn.execute(&format!("UPDATE \"{table}\" SET __crsql_site_id = ? WHERE __crsql_site_id IS NULL"), [ordinal])?;
                    debug!("updated {n} rows in {table}");
                }

                conn.execute("DELETE FROM __corro_members;", [])?;

                conn.execute_batch(
                    r#"
                    PRAGMA journal_mode = WAL; -- so the restore can be done online
                    PRAGMA wal_checkpoint(TRUNCATE);
                    "#,
                )?;
            }

            info!("successfully cleaned for restoration and backed up database to {path}");
        }
        Command::Restore { path } => {
            if AdminConn::connect(cli.admin_path()).await.is_ok() {
                eyre::bail!("corrosion is currently running, shut it down before restoring!");
            }

            let restored =
                sqlite3_restore::restore(&path, &cli.config()?.db.path, Duration::from_secs(30))?;

            info!(
                "successfully restored! old size: {}, new size: {}",
                restored.old_len, restored.new_len
            );
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
            ..
        } => {
            let mut body = cli
                .api_client()?
                .query(&Statement::Simple(query.clone()))
                .await?;

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
                    QueryEvent::Row { cells, .. } => {
                        println!(
                            "{}",
                            cells
                                .iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join("|")
                        );
                    }
                    QueryEvent::EndOfQuery => {
                        break;
                    }
                    QueryEvent::Error(e) => {
                        eyre::bail!("{e}");
                    }
                }
            }
        }
        Command::Exec { query, timer } => {
            let res = cli
                .api_client()?
                .execute(&[Statement::Simple(query.clone())])
                .await?;

            for res in res.results {
                match res {
                    RqliteResult::Execute {
                        rows_affected,
                        time,
                    } => {
                        info!("Rows affected: {rows_affected}");
                        if let Some(elapsed) = timer.then_some(time).flatten() {
                            println!("Run Time: real {elapsed}");
                        }
                    }
                    RqliteResult::Error { error } => {
                        error!("{error}");
                    }
                    _ => {}
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
        Command::Template { template, flags } => {
            command::tpl::run(cli.api_addr()?, template, flags).await?;
        }
        Command::Tls(tls) => match tls {
            TlsCommand::Ca(TlsCaCommand::Generate) => generate_ca().await?,
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

#[tokio::main]
async fn main() {
    let cli: Cli = Cli::parse();

    if let Err(e) = process_cli(cli).await {
        eprintln!("{e}");
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
        default_value = "corrosion.toml"
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
    Backup { path: String },

    /// Restore the Corrosion DB from a backup
    Restore { path: PathBuf },

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
    },

    /// Execute a SQL statement that mutates the state of Corrosion
    Exec {
        query: String,
        #[arg(long, default_value = "false")]
        timer: bool,
    },

    /// Reload the config
    Reload,

    /// Sync-related commands
    #[command(subcommand)]
    Sync(SyncCommand),

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
