use std::net::SocketAddr;

use camino::Utf8PathBuf;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

pub const DEFAULT_GOSSIP_PORT: u16 = 4001;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub api: ApiConfig,
    pub gossip: GossipConfig,

    #[serde(default)]
    pub admin: AdminConfig,

    #[serde(default)]
    pub telemetry: Option<TelemetryConfig>,

    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub consul: Option<ConsulConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryConfig {
    Prometheus {
        #[serde(alias = "addr")]
        bind_addr: SocketAddr,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
    pub uds_path: Utf8PathBuf,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            uds_path: default_admin_path(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub path: Utf8PathBuf,
    #[serde(default)]
    pub schema_paths: Vec<Utf8PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
    #[serde(alias = "authz", default)]
    pub authorization: Option<AuthzConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzConfig {
    BearerToken(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
    #[serde(default)]
    pub bootstrap: Vec<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub plaintext: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,
    /// CA (Certificate Authority) file
    pub ca_file: Option<Utf8PathBuf>,

    pub default_server_name: CompactString,

    #[serde(default)]
    pub insecure: bool,
}

pub fn default_admin_path() -> Utf8PathBuf {
    "/var/run/corrosion/admin.sock".into()
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default)]
    pub format: LogFormat,
    #[serde(default)]
    pub colors: bool,
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
    log: Option<LogConfig>,
    schema_paths: Vec<Utf8PathBuf>,
    max_change_size: Option<i64>,
    consul: Option<ConsulConfig>,
    tls: Option<TlsConfig>,
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

    pub fn log(mut self, log: LogConfig) -> Self {
        self.log = Some(log);
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

    pub fn max_change_size(mut self, size: i64) -> Self {
        self.max_change_size = Some(size);
        self
    }

    pub fn consul(mut self, config: ConsulConfig) -> Self {
        self.consul = Some(config);
        self
    }

    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    pub fn build(self) -> Result<Config, ConfigBuilderError> {
        let db_path = self.db_path.ok_or(ConfigBuilderError::DbPathRequired)?;
        Ok(Config {
            db: DbConfig {
                path: db_path,
                schema_paths: self.schema_paths,
            },
            api: ApiConfig {
                bind_addr: self.api_addr.ok_or(ConfigBuilderError::ApiAddrRequired)?,
                authorization: None,
            },
            gossip: GossipConfig {
                bind_addr: self
                    .gossip_addr
                    .ok_or(ConfigBuilderError::GossipAddrRequired)?,
                bootstrap: self.bootstrap.unwrap_or_default(),
                plaintext: self.tls.is_none(),
                tls: self.tls,
            },
            admin: AdminConfig {
                uds_path: self.admin_path.unwrap_or_else(default_admin_path),
            },
            telemetry: self
                .metrics_addr
                .map(|bind_addr| TelemetryConfig::Prometheus { bind_addr }),
            log: self.log.unwrap_or_default(),

            consul: self.consul,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigBuilderError {
    #[error("db.path required")]
    DbPathRequired,
    #[error("gossip.addr required")]
    GossipAddrRequired,
    #[error("api.addr required")]
    ApiAddrRequired,
}

/// Log format (JSON only)
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum LogFormat {
    #[default]
    Plaintext,
    Json,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ConsulConfig {
    #[serde(default)]
    pub extra_services_columns: Vec<String>,
    #[serde(default)]
    pub extra_statements: Vec<String>,

    pub client: consul_client::Config,
}
