use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use corro_types::config::Config;

pub mod command;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli: Cli = Cli::parse();

    let config_path = cli.config;

    println!("Using config file: {}", config_path);
    let config = Config::load(config_path.as_str()).expect("could not read config from file");

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
            Command::Reload => command::reload::run(config).await?,
        },
    }

    Ok(())
}

#[derive(Parser)]
#[clap(version = VERSION)]
pub(crate) struct Cli {
    /// Set the config file path
    #[clap(long, short, global = true, default_value = "corrosion.toml")]
    pub(crate) config: Utf8PathBuf,

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

    /// Reload the config
    Reload,
}

#[derive(Subcommand)]
enum ConsulCommand {
    /// Synchronizes the local consul agent with Corrosion
    Sync,
}
