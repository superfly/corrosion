use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    fs::OpenOptions,
    io::BufReader,
};

use hyper::{client::HttpConnector, http::uri::InvalidUri};
use hyper_rustls::HttpsConnector;
use rustls::{Certificate, PrivateKey, RootCertStore};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, NoneAsEmptyString};

pub mod config;
pub use config::Config;

pub type ConsulResult<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Client {
    client: hyper::Client<HttpsConnector<HttpConnector>>,
    addr: String,
}

impl Client {
    pub fn new(config: Config) -> ConsulResult<Self> {
        let scheme = if config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let ctor = if let Some(tls) = config.tls {
            // HTTPS path
            let mut root_store = RootCertStore::empty();
            let mut cacert_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.ca_file)
                    .map_err(Error::TlsSetup)?,
            );
            for cacert in rustls_pemfile::certs(&mut cacert_file).map_err(Error::TlsSetup)? {
                root_store.add(&Certificate(cacert))?;
            }

            let mut cert_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.cert_file)
                    .map_err(Error::TlsSetup)?,
            );
            let certs = rustls_pemfile::certs(&mut cert_file)
                .map_err(Error::TlsSetup)?
                .into_iter()
                .map(Certificate)
                .collect();

            let mut key_file = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&tls.key_file)
                    .map_err(Error::TlsSetup)?,
            );
            let key = rustls_pemfile::pkcs8_private_keys(&mut key_file)
                .map_err(Error::TlsSetup)?
                .into_iter()
                .map(PrivateKey)
                .next()
                .expect("could not find tls key");

            let tls_config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_single_cert(certs, key)?;

            hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(tls_config)
                .https_or_http()
                .enable_http2()
                .build()
        } else {
            // this is always gonna be HTTP, but we still need to build this, annoyingly.
            // TODO: build custom connector so we don't have to do this
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .build()
        };

        Ok(Self {
            client: hyper::Client::builder().build(ctor),
            addr: format!("{}://{}", scheme, config.address),
        })
    }

    pub async fn agent_services(&self) -> ConsulResult<HashMap<String, AgentService>> {
        self.request("/v1/agent/services").await
    }

    pub async fn agent_checks(&self) -> ConsulResult<HashMap<String, AgentCheck>> {
        self.request("/v1/agent/checks").await
    }

    async fn request<P: Display, T: DeserializeOwned>(&self, path: P) -> ConsulResult<T> {
        let res = match self
            .client
            .get(format!("{}{}", &self.addr, &path).parse()?)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                return Err(e.into());
            }
        };

        if res.status() != hyper::StatusCode::OK {
            return Err(Error::BadStatusCode(res.status()));
        }

        let bytes = hyper::body::to_bytes(res.into_body()).await?;

        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error("bad status code: {0}")]
    BadStatusCode(hyper::StatusCode),
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("tls setup: {0}")]
    TlsSetup(std::io::Error),
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
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
