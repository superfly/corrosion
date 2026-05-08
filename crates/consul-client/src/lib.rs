use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    fs::OpenOptions,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, NoneAsEmptyString};

use rusqlite::types::{FromSql, FromSqlError, ValueRef};

pub mod config;
pub use config::Config;

/// Convenience alias for results returned by this crate.
pub type ConsulResult<T> = std::result::Result<T, Error>;

/// HTTP client for the local Consul agent API.
#[derive(Debug, Clone)]
pub struct Client {
    client: reqwest::Client,
    addr: String,
}

impl Client {
    /// Build a new client from the given [`Config`].
    ///
    /// If [`Config::tls`] is set the corresponding CA, certificate and key
    /// files are read eagerly and used to configure rustls. Errors from
    /// reading or parsing those files are returned via [`Error::TlsSetup`]
    /// and the rustls/PEM error variants.
    pub fn new(config: Config) -> ConsulResult<Self> {
        use rustls::pki_types::pem::PemObject as _;

        let scheme = if config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let ctor = if let Some(tls) = config.tls {
            // HTTPS path
            let mut root_store = rustls::RootCertStore::empty();
            let cacert_iter = rustls::pki_types::CertificateDer::pem_reader_iter(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.ca_file)
                    .map_err(Error::TlsSetup)?,
            );
            for cacert in cacert_iter {
                let cert = cacert?;
                root_store.add(cert)?;
            }

            let cert_file = rustls::pki_types::CertificateDer::pem_reader_iter(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.cert_file)
                    .map_err(Error::TlsSetup)?,
            );
            let certs = cert_file.collect::<Result<Vec<_>, _>>()?;

            let key = rustls::pki_types::PrivateKeyDer::from_pem_reader(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.key_file)
                    .map_err(Error::TlsSetup)?,
            )?;

            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)?;

            reqwest::ClientBuilder::new()
                .use_preconfigured_tls(tls_config)
                .build()
        } else {
            reqwest::ClientBuilder::new().build()
        };

        Ok(Self {
            client: ctor?,
            addr: format!("{scheme}://{}", config.address),
        })
    }

    /// Fetch the services registered on the local Consul agent.
    ///
    /// Wraps the [`/v1/agent/services`](https://developer.hashicorp.com/consul/api-docs/agent/service#list-services)
    /// endpoint. The returned map is keyed by service ID.
    pub async fn agent_services(&self) -> ConsulResult<HashMap<String, AgentService>> {
        self.request("/v1/agent/services").await
    }

    /// Fetch the health checks registered on the local Consul agent.
    ///
    /// Wraps the [`/v1/agent/checks`](https://developer.hashicorp.com/consul/api-docs/agent/check#list-checks)
    /// endpoint. The returned map is keyed by check ID.
    pub async fn agent_checks(&self) -> ConsulResult<HashMap<String, AgentCheck>> {
        self.request("/v1/agent/checks").await
    }

    async fn request<P: Display, T: DeserializeOwned>(&self, path: P) -> ConsulResult<T> {
        let res = match self.client.get(format!("{}{path}", self.addr)).send().await {
            Ok(res) => res,
            Err(e) => {
                return Err(e.into());
            }
        };

        if res.status() != http::StatusCode::OK {
            return Err(Error::BadStatusCode(res.status()));
        }

        let bytes = res.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// Errors produced by the Consul client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP transport-level error from `reqwest`.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// The Consul agent returned a non-`200 OK` response.
    #[error("bad status code: {0}")]
    BadStatusCode(http::StatusCode),
    /// The configured agent address could not be parsed as a URI.
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    /// The response body could not be deserialized as JSON.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    /// One of the TLS material files (CA, cert, key) could not be opened.
    #[error("tls setup: {0}")]
    TlsSetup(std::io::Error),
    /// The rustls client configuration could not be built.
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    /// A PEM-encoded TLS file failed to parse.
    #[error(transparent)]
    Pem(#[from] rustls::pki_types::pem::Error),
    /// A certificate failed WebPKI validation.
    #[error(transparent)]
    Webpki(#[from] webpki::Error),
}

/// A service registered on the local Consul agent.
///
/// Mirrors the subset of fields that Corrosion cares about from Consul's
/// `AgentService` JSON object.
#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
#[serde(rename_all(deserialize = "PascalCase"))]
pub struct AgentService {
    #[serde(rename(deserialize = "ID"))]
    pub id: String,
    #[serde(rename(deserialize = "Service"))]
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
    pub port: u16,
    pub address: String,
}

/// A health check registered on the local Consul agent.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all(deserialize = "PascalCase"))]
pub struct AgentCheck {
    #[serde(rename(deserialize = "CheckID"))]
    pub id: String,
    pub name: String,
    pub status: ConsulCheckStatus,
    pub output: String,
    #[serde(rename(deserialize = "ServiceID"))]
    pub service_id: String,
    pub service_name: String,
    #[serde_as(as = "NoneAsEmptyString")]
    pub notes: Option<String>,
}

/// Status of a Consul health check.
#[derive(Debug, Copy, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsulCheckStatus {
    Passing,
    Warning,
    Critical,
}

impl ConsulCheckStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsulCheckStatus::Passing => "passing",
            ConsulCheckStatus::Warning => "warning",
            ConsulCheckStatus::Critical => "critical",
        }
    }
}

impl FromSql for ConsulCheckStatus {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => Ok(match String::from_utf8_lossy(s).as_ref() {
                "passing" => Self::Passing,
                "warning" => Self::Warning,
                "critical" => Self::Critical,
                _ => {
                    return Err(FromSqlError::InvalidType);
                }
            }),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl fmt::Display for ConsulCheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl Default for ConsulCheckStatus {
    fn default() -> Self {
        Self::Passing
    }
}
