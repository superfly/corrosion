use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant},
};

use admin::AdminConn;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use command::{
    tls::{generate_ca, generate_client_cert, generate_server_cert},
    tpl::TemplateFlags,
};
use corro_api_types::SqliteParam;
use corro_client::CorrosionApiClient;
use corro_types::{
    actor::{ActorId, ClusterId},
    api::{ExecResult, QueryEvent, Statement},
    base::Version,
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

            let config = match cli.config() {
                Ok(config) => config,
                Err(_e) => {
                    eyre::bail!(
                        "path to current database is required via the config file passed as --config"
                    );
                }
            };

            let db_path = &config.db.path;

            if *self_actor_id || actor_id.is_some() {
                let site_id: Uuid = {
                    if let Some(actor_id) = actor_id {
                        *actor_id
                    } else {
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

                let inserted = conn.execute(
                    "INSERT OR REPLACE INTO crsql_site_id (ordinal, site_id) VALUES (0, ?)",
                    [site_id],
                )?;
                if inserted != 1 {
                    eyre::bail!("could not insert old site id into crsql_site_id table");
                }

                if let Some(ordinal) = ordinal {
                    if ordinal == 0 {
                        warn!("skipping clock table site_id rewrite: ordinal was 0 and therefore did not change");
                    } else {
                        info!("rewriting clock tables site_id");
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
            }

            let subs_path = config.db.subscriptions_path();

            info!("removing subscriptions at {subs_path}");
            match tokio::fs::remove_dir_all(subs_path).await {
                Ok(_) => {
                    info!("cleared subscriptions");
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        info!("no subscriptions to delete.");
                    }
                    _ => {
                        eyre::bail!(
                            "aborting restore! could not delete previous subscriptions: {e}"
                        )
                    }
                },
            }

            if path == db_path {
                info!("reused the db path, not copying database into place");
            } else {
                let restored =
                    sqlite3_restore::restore(path, cli.config()?.db.path, Duration::from_secs(30))?;

                info!(
                    "successfully restored! old size: {}, new size: {}",
                    restored.old_len, restored.new_len
                );
            }
        }
        Command::Cluster(ClusterCommand::Rejoin) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Cluster(
                corro_admin::ClusterCommand::Rejoin,
            ))
            .await?;
        }
        Command::Cluster(ClusterCommand::Members) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Cluster(
                corro_admin::ClusterCommand::Members,
            ))
            .await?;
        }
        Command::Cluster(ClusterCommand::MembershipStates) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Cluster(
                corro_admin::ClusterCommand::MembershipStates,
            ))
            .await?;
        }
        Command::Cluster(ClusterCommand::SetId { cluster_id }) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Cluster(
                corro_admin::ClusterCommand::SetId(ClusterId(*cluster_id)),
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

            let mut query = cli.api_client()?.query(&stmt).await?;
            while let Some(res) = query.next().await {
                match res {
                    Ok(QueryEvent::Columns(cols)) => {
                        if *show_columns {
                            println!("{}", cols.join("|"));
                        }
                    }
                    Ok(QueryEvent::Row(_, cells)) => {
                        println!(
                            "{}",
                            cells
                                .iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                                .join("|")
                        );
                    }
                    Ok(QueryEvent::EndOfQuery { time, .. }) => {
                        if *timer {
                            println!("time: {time}s");
                        }
                    }
                    Ok(QueryEvent::Change(_, _, _, _)) => {
                        break;
                    }
                    Ok(QueryEvent::Error(e)) => {
                        eyre::bail!("{e}");
                    }
                    Err(e) => {
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
        Command::Sync(SyncCommand::ReconcileGaps) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Sync(
                corro_admin::SyncCommand::ReconcileGaps,
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
        Command::Actor(ActorCommand::Version { actor_id, version }) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Actor(
                corro_admin::ActorCommand::Version {
                    actor_id: ActorId(*actor_id),
                    version: Version(*version),
                },
            ))
            .await?;
        }
        Command::Db(DbCommand::Lock { cmd }) => {
            let config = match cli.config() {
                Ok(config) => config,
                Err(_e) => {
                    eyre::bail!(
                        "path to current database is required via the config file passed as --config"
                    );
                }
            };

            let db_path = &config.db.path;
            info!("Opening DB file at {db_path}");
            let mut db_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path)?;

            info!("Acquiring lock...");
            let start = Instant::now();
            let _lock = sqlite3_restore::lock_all(&mut db_file, db_path, Duration::from_secs(30))?;
            info!("Lock acquired after {:?}", start.elapsed());

            info!("Launching command {cmd}");
            let mut splitted_cmd = shell_words::split(cmd.as_str())?;
            let exit = std::process::Command::new(splitted_cmd.remove(0))
                .args(splitted_cmd)
                .spawn()?
                .wait()?;

            info!("Exited with code: {:?}", exit.code());
            std::process::exit(exit.code().unwrap_or(1));
        }
        Command::Subs(SubsCommand::Info { hash, id }) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Subs(corro_admin::SubsCommand::Info {
                hash: hash.clone(),
                id: *id,
            }))
            .await?;
        }
        Command::Subs(SubsCommand::List) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Subs(corro_admin::SubsCommand::List))
                .await?;
        }
        Command::Debug(DebugCommand::Follow {
            peer_addr,
            from,
            local_only,
        }) => {
            let mut conn = AdminConn::connect(cli.admin_path()).await?;
            conn.send_command(corro_admin::Command::Debug(
                corro_admin::DebugCommand::Follow {
                    peer_addr: *peer_addr,
                    from: *from,
                    local_only: *local_only,
                },
            ))
            .await?;
        }
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
#[command(version = VERSION)]
struct Cli {
    /// Set the config file path
    #[arg(
        long = "config",
        short,
        global = true,
        default_value = "/etc/corrosion/config.toml"
    )]
    config_path: Utf8PathBuf,

    #[arg(long, global = true)]
    api_addr: Option<SocketAddr>,

    #[arg(long, global = true)]
    db_path: Option<Utf8PathBuf>,

    #[arg(long, global = true)]
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
            *self.config()?.api.bind_addr.first().unwrap()
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
        path: Utf8PathBuf,
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

    /// Actor-related commands
    #[command(subcommand)]
    Actor(ActorCommand),

    Template {
        template: Vec<String>,
        #[command(flatten)]
        flags: TemplateFlags,
    },

    /// Tls-related commands
    #[command(subcommand)]
    Tls(TlsCommand),

    /// DB-related commands
    #[command(subcommand)]
    Db(DbCommand),

    /// Subscription related commands
    #[command(subcommand)]
    Subs(SubsCommand),

    /// Debug commands
    #[command(subcommand)]
    Debug(DebugCommand),
}

#[derive(Subcommand)]
enum ClusterCommand {
    // /// Dumps info about the current actor
    // Actor,
    /// Force a rejoin of the cluster
    Rejoin,
    /// Dumps the current members
    Members,
    /// Dumps the current member SWIM states
    MembershipStates,
    /// Set a new cluster ID for the node
    SetId { cluster_id: u16 },
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
    /// Reconcile gaps between memory and DB
    ReconcileGaps,
}

#[derive(Subcommand)]
enum ActorCommand {
    /// Get information about a known version
    Version { actor_id: Uuid, version: u64 },
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

#[derive(Subcommand)]
enum DbCommand {
    /// Acquires the lock on the DB
    Lock { cmd: String },
}

#[derive(Subcommand)]
enum SubsCommand {
    /// List all subscriptions on a node
    List,
    /// Get information on a subscription
    Info {
        #[arg(long)]
        hash: Option<String>,
        #[arg(long)]
        id: Option<Uuid>,
    },
}

#[derive(Subcommand)]
enum DebugCommand {
    /// Follow a node's changes
    Follow {
        peer_addr: SocketAddr,
        #[arg(long, default_value = None)]
        from: Option<u64>,
        #[arg(long, default_value_t = false)]
        local_only: bool,
    },
}
