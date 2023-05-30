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

    /// Reload the config
    Reload,
}
