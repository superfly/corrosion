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

pub type ConsulResult<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Client {
    client: reqwest::Client,
    addr: String,
}

impl Client {
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
                .http2_prior_knowledge()
                .build()
        } else {
            reqwest::ClientBuilder::new().http1_only().build()
        };

        Ok(Self {
            client: ctor?,
            addr: format!("{scheme}://{}", config.address),
        })
    }

    pub async fn agent_services(&self) -> ConsulResult<HashMap<String, AgentService>> {
        self.request("/v1/agent/services").await
    }

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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("bad status code: {0}")]
    BadStatusCode(http::StatusCode),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("tls setup: {0}")]
    TlsSetup(std::io::Error),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    Pem(#[from] rustls::pki_types::pem::Error),
    #[error(transparent)]
    Webpki(#[from] webpki::Error),
}

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
