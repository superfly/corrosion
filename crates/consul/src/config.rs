use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

/// HashiCorp Consul client configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Vault address
    #[serde(default = "default_consul_address")]
    pub address: String,

    /// True if TLS is used to speak to vault
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

fn default_consul_address() -> String {
    "127.0.0.1:8501".into()
}

/// HashiCorp Consul client's TLS configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
    /// CA (Certificate Authority) file
    pub ca_file: Utf8PathBuf,
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,
}
