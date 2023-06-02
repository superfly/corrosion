use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use corro_client::CorrosionClient;
use corro_types::{
    api::{RqliteResult, Statement},
    config::Config,
};

pub mod command;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli: Cli = Cli::parse();

    let config_path = cli.config;

    let config = Config::load(config_path.as_str())
        .expect("could not read config from file at {config_path}");

    match cli.command {
        None => {
            // No command provided
        }
        Some(cmd) => match cmd {
            Command::Agent => command::agent::run(config, config_path).await?,
            Command::Consul(cmd) => match cmd {
                ConsulCommand::Sync => match config.consul.as_ref() {
                    Some(consul) => {
                        command::consul::sync::run(consul, config.api_addr, &config.db_path).await?
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
                let client = CorrosionClient::new(config.api_addr, config.db_path);
                let res = client.query(&[Statement::Simple(query)]).await?;

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
                            if show_columns {
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
                let client = CorrosionClient::new(config.api_addr, config.db_path);
                let res = client.execute(&[Statement::Simple(query)]).await?;

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
            Command::Reload => command::reload::run(config).await?,
        },
    }

    Ok(())
}

#[derive(Parser)]
#[clap(version = VERSION)]
struct Cli {
    /// Set the config file path
    #[clap(long, short, global = true, default_value = "corrosion.toml")]
    config: Utf8PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
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

    /// Reload the config
    Reload,
}

#[derive(Subcommand)]
enum ConsulCommand {
    /// Synchronizes the local consul agent with Corrosion
    Sync,
}
