use std::net::SocketAddr;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

pub const DEFAULT_GOSSIP_PORT: u16 = 4001;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub base_path: Utf8PathBuf,
    pub gossip_addr: SocketAddr,
    #[serde(default)]
    pub api_addr: Option<SocketAddr>,
    pub metrics_addr: Option<SocketAddr>,
    #[serde(default)]
    pub bootstrap: Vec<String>,
    #[serde(default)]
    pub log_format: LogFormat,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Reads configuration from a TOML file, given its path. Environment
    /// variables can override whatever is set in the config file.
    pub fn read_from_file_and_env(config_path: &str) -> eyre::Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::new(config_path, config::FileFormat::Toml))
            .add_source(config::Environment::default().separator("__"))
            .build()?;
        Ok(config.try_deserialize()?)
    }
}

#[derive(Debug, Default)]
pub struct ConfigBuilder {
    base_path: Option<Utf8PathBuf>,
    gossip_addr: Option<SocketAddr>,
    api_addr: Option<SocketAddr>,
    metrics_addr: Option<SocketAddr>,
    bootstrap: Option<Vec<String>>,
    log_format: Option<LogFormat>,
}

impl ConfigBuilder {
    pub fn base_path<S: Into<Utf8PathBuf>>(mut self, db_path: S) -> Self {
        self.base_path = Some(db_path.into());
        self
    }

    pub fn gossip_addr(mut self, addr: SocketAddr) -> Self {
        self.gossip_addr = Some(addr);
        self
    }

    pub fn api_addr(mut self, addr: SocketAddr) -> Self {
        self.api_addr = Some(addr);
        self
    }

    pub fn metrics_addr(mut self, addr: SocketAddr) -> Self {
        self.metrics_addr = Some(addr);
        self
    }

    pub fn bootstrap<V: Into<Vec<String>>>(mut self, bootstrap: V) -> Self {
        self.bootstrap = Some(bootstrap.into());
        self
    }

    pub fn log_format(mut self, log_format: LogFormat) -> Self {
        self.log_format = Some(log_format);
        self
    }

    pub fn build(self) -> Result<Config, ConfigError> {
        Ok(Config {
            base_path: self.base_path.unwrap_or_else(default_base_path),
            gossip_addr: self.gossip_addr.ok_or(ConfigError::GossipAddrRequired)?,
            api_addr: self.api_addr,
            metrics_addr: self.metrics_addr,
            bootstrap: self.bootstrap.unwrap_or_default(),
            log_format: self.log_format.unwrap_or_default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("gossip_addr required")]
    GossipAddrRequired,
    #[error("api_addr required")]
    ApiAddrRequired,
}

fn default_base_path() -> Utf8PathBuf {
    "./".into()
}

/// Log format (JSON only)
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum LogFormat {
    Plaintext,
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::Plaintext
    }
}
