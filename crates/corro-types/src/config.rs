use std::net::SocketAddr;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

pub const DEFAULT_GOSSIP_PORT: u16 = 4001;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db_path: Utf8PathBuf,
    pub gossip_addr: SocketAddr,
    #[serde(default)]
    pub api_addr: Option<SocketAddr>,
    #[serde(default = "default_admin_path")]
    pub admin_path: Utf8PathBuf,
    pub metrics_addr: Option<SocketAddr>,
    #[serde(default)]
    pub bootstrap: Vec<String>,
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(default)]
    pub schema_paths: Vec<Utf8PathBuf>,
}

pub fn default_admin_path() -> Utf8PathBuf {
    "/var/run/corrosion/admin.sock".into()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Reads configuration from a TOML file, given its path. Environment
    /// variables can override whatever is set in the config file.
    pub fn load(config_path: &str) -> Result<Self, ConfigError> {
        let config = config::Config::builder()
            .add_source(config::File::new(config_path, config::FileFormat::Toml))
            .add_source(config::Environment::default().separator("__"))
            .build()?;
        Ok(config.try_deserialize()?)
    }
}

#[derive(Debug, Default)]
pub struct ConfigBuilder {
    pub db_path: Option<Utf8PathBuf>,
    gossip_addr: Option<SocketAddr>,
    api_addr: Option<SocketAddr>,
    admin_path: Option<Utf8PathBuf>,
    metrics_addr: Option<SocketAddr>,
    bootstrap: Option<Vec<String>>,
    log_format: Option<LogFormat>,
    schema_paths: Vec<Utf8PathBuf>,
}

impl ConfigBuilder {
    pub fn db_path<S: Into<Utf8PathBuf>>(mut self, db_path: S) -> Self {
        self.db_path = Some(db_path.into());
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

    pub fn add_schema_path<S: Into<Utf8PathBuf>>(mut self, path: S) -> Self {
        self.schema_paths.push(path.into());
        self
    }

    pub fn admin_path<S: Into<Utf8PathBuf>>(mut self, path: S) -> Self {
        self.admin_path = Some(path.into());
        self
    }

    pub fn build(self) -> Result<Config, ConfigBuilderError> {
        let db_path = self.db_path.unwrap_or_else(default_db_path);
        Ok(Config {
            db_path,
            gossip_addr: self
                .gossip_addr
                .ok_or(ConfigBuilderError::GossipAddrRequired)?,
            api_addr: self.api_addr,
            admin_path: self.admin_path.unwrap_or_else(default_admin_path),
            metrics_addr: self.metrics_addr,
            bootstrap: self.bootstrap.unwrap_or_default(),
            log_format: self.log_format.unwrap_or_default(),
            schema_paths: self.schema_paths,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigBuilderError {
    #[error("gossip_addr required")]
    GossipAddrRequired,
}

fn default_db_path() -> Utf8PathBuf {
    "./corro.db".into()
}

/// Log format (JSON only)
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
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
