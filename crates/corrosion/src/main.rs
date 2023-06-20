use std::net::SocketAddr;

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use corro_client::CorrosionApiClient;
use corro_types::{
    api::{RqliteResult, Statement},
    config::Config,
    pubsub::{SubscriptionEvent, SubscriptionMessage},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

pub mod command;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const CONFIG: OnceCell<Config> = OnceCell::new();
pub const API_CLIENT: OnceCell<CorrosionApiClient> = OnceCell::new();

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli: Cli = Cli::parse();

    match &cli.command {
        Command::Agent => command::agent::run(cli.config(), &cli.config_path).await?,
        Command::Consul(cmd) => match cmd {
            ConsulCommand::Sync => match cli.config().consul.as_ref() {
                Some(consul) => {
                    command::consul::sync::run(consul, cli.api_addr(), cli.db_path()).await?
                }
                None => {
                    eprintln!("missing `consul` block in corrosion config");
                }
            },
        },
        Command::Query {
            query,
            columns: show_columns,
            timer,
        } => {
            let res = cli
                .api_client()
                .query(&[Statement::Simple(query.clone())])
                .await?;

            for res in res.results {
                match res {
                    RqliteResult::QueryAssociative { rows, time, .. } => {
                        for row in rows {
                            println!(
                                "{}",
                                row.values()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join("|")
                            );
                        }
                        if let Some(elapsed) = timer.then_some(time).flatten() {
                            println!("Run Time: real {elapsed}");
                        }
                    }
                    RqliteResult::Query {
                        values,
                        time,
                        columns,
                        ..
                    } => {
                        if *show_columns {
                            println!("{}", columns.join("|"));
                        }
                        for row in values {
                            println!(
                                "{}",
                                row.iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join("|")
                            );
                        }
                        if let Some(elapsed) = timer.then_some(time).flatten() {
                            println!("Run Time: real {elapsed}");
                        }
                    }
                    RqliteResult::Error { error } => {
                        eprintln!("Error: {error}");
                    }
                    _ => {}
                }
            }
        }
        Command::Exec { query, timer } => {
            let res = cli
                .api_client()
                .execute(&[Statement::Simple(query.clone())])
                .await?;

            for res in res.results {
                match res {
                    RqliteResult::Execute {
                        rows_affected,
                        time,
                    } => {
                        println!("Rows affected: {rows_affected}");
                        if let Some(elapsed) = timer.then_some(time).flatten() {
                            println!("Run Time: real {elapsed}");
                        }
                    }
                    RqliteResult::Error { error } => {
                        eprintln!("Error: {error}");
                    }
                    _ => {}
                }
            }
        }
        Command::Sub { where_clause } => {
            let id = "testing-testing";
            let mut conn = cli.api_client().subscribe(id, where_clause.clone()).await?;

            while let Some(event) = conn.next().await {
                match event {
                    Ok(event) => match event {
                        SubscriptionMessage::Event { id, event } => match event {
                            SubscriptionEvent::Change(change) => {
                                print!(
                                    "({id}) [{} on '{}'] {{ ",
                                    change.evt_type.as_str(),
                                    change.table,
                                );
                                for (k, v) in change.pk {
                                    print!("{k}: {v}");
                                }
                                print!(" }} => {{ ");
                                for (k, v) in change.data {
                                    print!("{k}: {v}");
                                }
                                println!(" }}");
                            }
                            SubscriptionEvent::Error { error } => {
                                eprintln!("Error: {error}");
                            }
                        },
                    },
                    Err(e) => {
                        eprintln!("Error: {e}");
                    }
                }
            }
        }
        Command::Reload => command::reload::run(cli.admin_path()).await?,
    }

    Ok(())
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
    fn api_client(&self) -> CorrosionApiClient {
        API_CLIENT
            .get_or_init(|| CorrosionApiClient::new(self.api_addr()))
            .clone()
    }

    fn api_addr(&self) -> SocketAddr {
        if let Some(api_addr) = self.api_addr {
            api_addr
        } else {
            self.config().api_addr
        }
    }

    fn db_path(&self) -> Utf8PathBuf {
        if let Some(ref db_path) = self.db_path {
            db_path.clone()
        } else {
            self.config().db_path
        }
    }

    fn admin_path(&self) -> Utf8PathBuf {
        if let Some(ref admin_path) = self.admin_path {
            admin_path.clone()
        } else {
            self.config().admin_path
        }
    }

    fn config(&self) -> Config {
        CONFIG
            .get_or_init(|| {
                let config_path = &self.config_path;
                Config::load(config_path.as_str())
                    .expect("could not read config from file at {config_path}")
            })
            .clone()
    }
}

#[derive(Subcommand)]
enum Command {
    /// Launches the agent
    Agent,

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

    Sub {
        where_clause: Option<String>,
    },

    /// Reload the config
    Reload,
}

#[derive(Subcommand)]
enum ConsulCommand {
    /// Synchronizes the local consul agent with Corrosion
    Sync,
}
