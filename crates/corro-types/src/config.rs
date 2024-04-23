use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_with::{formats::PreferOne, serde_as, OneOrMany};

pub const DEFAULT_GOSSIP_PORT: u16 = 4001;
const DEFAULT_GOSSIP_IDLE_TIMEOUT: u32 = 30;

const fn default_apply_queue() -> usize {
    100
}

/// Used for the apply channel
const fn default_huge_channel() -> usize {
    2048
}

//
const fn default_big_channel() -> usize {
    1024
}

const fn default_mid_channel() -> usize {
    512
}

const fn default_small_channel() -> usize {
    256
}

const fn default_apply_timeout() -> usize {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub api: ApiConfig,
    pub gossip: GossipConfig,

    #[serde(default)]
    pub perf: PerfConfig,

    #[serde(default)]
    pub admin: AdminConfig,

    #[serde(default)]
    pub telemetry: TelemetryConfig,

    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub consul: Option<ConsulConfig>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TelemetryConfig {
    pub prometheus: Option<PrometheusConfig>,
    pub open_telemetry: Option<OtelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OtelConfig {
    FromEnv,
    Exporter { endpoint: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
    #[serde(alias = "path")]
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
    #[serde(default)]
    pub subscriptions_path: Option<Utf8PathBuf>,
    #[serde(default)]
    pub clear_overwritten_secs: Option<u64>,
}

impl DbConfig {
    pub fn subscriptions_path(&self) -> Utf8PathBuf {
        self.subscriptions_path
            .as_ref()
            .cloned()
            .unwrap_or_else(|| {
                self.path
                    .parent()
                    .map(|parent| parent.join("subscriptions"))
                    .unwrap_or_else(|| "/subscriptions".into())
            })
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(alias = "addr")]
    #[serde_as(deserialize_as = "OneOrMany<_, PreferOne>")]
    pub bind_addr: Vec<SocketAddr>,
    #[serde(alias = "authz", default)]
    pub authorization: Option<AuthzConfig>,
    #[serde(default)]
    pub pg: Option<PgConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthzConfig {
    #[serde(alias = "bearer")]
    BearerToken(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
    pub external_addr: Option<SocketAddr>,
    #[serde(default = "default_gossip_client_addr")]
    pub client_addr: SocketAddr,
    #[serde(default)]
    pub bootstrap: Vec<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub plaintext: bool,
    #[serde(default)]
    pub max_mtu: Option<u16>,
    #[serde(default = "default_gossip_idle_timeout")]
    pub idle_timeout_secs: u32,
    #[serde(default)]
    pub disable_gso: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfConfig {
    #[serde(default = "default_huge_channel")]
    pub apply_channel_len: usize,
    #[serde(default = "default_big_channel")]
    pub changes_channel_len: usize,
    #[serde(default = "default_big_channel")]
    pub empties_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub to_send_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub notifications_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub schedule_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub clearbuf_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub bcast_channel_len: usize,
    #[serde(default = "default_small_channel")]
    pub foca_channel_len: usize,
    #[serde(default = "default_apply_timeout")]
    pub apply_queue_timeout: usize,
    #[serde(default = "default_apply_queue")]
    pub apply_queue_len: usize,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            apply_channel_len: default_huge_channel(),
            changes_channel_len: default_big_channel(),
            empties_channel_len: default_big_channel(),
            to_send_channel_len: default_mid_channel(),
            notifications_channel_len: default_mid_channel(),
            schedule_channel_len: default_mid_channel(),
            clearbuf_channel_len: default_mid_channel(),
            bcast_channel_len: default_mid_channel(),
            foca_channel_len: default_small_channel(),
            apply_queue_timeout: default_apply_timeout(),
            apply_queue_len: default_apply_queue(),
        }
    }
}

fn default_gossip_idle_timeout() -> u32 {
    DEFAULT_GOSSIP_IDLE_TIMEOUT
}

pub const DEFAULT_GOSSIP_CLIENT_ADDR: SocketAddr =
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0u16, 0, 0));

fn default_gossip_client_addr() -> SocketAddr {
    DEFAULT_GOSSIP_CLIENT_ADDR
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,

    /// CA (Certificate Authority) file
    #[serde(default)]
    pub ca_file: Option<Utf8PathBuf>,

    #[serde(default)]
    pub insecure: bool,

    /// Mutual TLS configuration
    #[serde(default)]
    pub client: Option<TlsClientConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsClientConfig {
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,
}

pub fn default_admin_path() -> Utf8PathBuf {
    "/var/run/corrosion/admin.sock".into()
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default)]
    pub format: LogFormat,
    #[serde(default = "default_as_true")]
    pub colors: bool,
}

fn default_as_true() -> bool {
    true
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
    api_addr: Vec<SocketAddr>,
    external_addr: Option<SocketAddr>,
    admin_path: Option<Utf8PathBuf>,
    prometheus_addr: Option<SocketAddr>,
    bootstrap: Option<Vec<String>>,
    log: Option<LogConfig>,
    schema_paths: Vec<Utf8PathBuf>,
    max_change_size: Option<i64>,
    consul: Option<ConsulConfig>,
    tls: Option<TlsConfig>,
    perf: Option<PerfConfig>,
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
        self.api_addr.push(addr);
        self
    }

    pub fn prometheus_addr(mut self, addr: SocketAddr) -> Self {
        self.prometheus_addr = Some(addr);
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

        let telemetry = TelemetryConfig {
            prometheus: self
                .prometheus_addr
                .map(|bind_addr| PrometheusConfig { bind_addr }),
            open_telemetry: None,
        };

        if self.api_addr.is_empty() {
            return Err(ConfigBuilderError::ApiAddrRequired);
        }

        Ok(Config {
            db: DbConfig {
                path: db_path,
                schema_paths: self.schema_paths,
                subscriptions_path: None,
                clear_overwritten_secs: None,
            },
            api: ApiConfig {
                bind_addr: self.api_addr,
                authorization: None,
                pg: None,
            },
            gossip: GossipConfig {
                bind_addr: self
                    .gossip_addr
                    .ok_or(ConfigBuilderError::GossipAddrRequired)?,
                external_addr: self.external_addr,
                client_addr: default_gossip_client_addr(),
                bootstrap: self.bootstrap.unwrap_or_default(),
                plaintext: self.tls.is_none(),
                tls: self.tls,
                idle_timeout_secs: default_gossip_idle_timeout(),
                max_mtu: None, // TODO: add a builder function for it
                disable_gso: false,
            },
            perf: self.perf.unwrap_or_default(),
            admin: AdminConfig {
                uds_path: self.admin_path.unwrap_or_else(default_admin_path),
            },
            telemetry,
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
#[serde(rename_all = "kebab-case")]
pub struct ConsulConfig {
    pub client: consul_client::Config,
}
