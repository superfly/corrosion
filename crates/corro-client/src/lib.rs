pub mod sub;

use std::{net::SocketAddr, ops::Deref, path::Path};

use corro_api_types::{ChangeId, ExecResponse, ExecResult, Statement};
use http::uri::PathAndQuery;
use hyper::{client::HttpConnector, http::HeaderName, Body, StatusCode};
use sub::SubscriptionStream;
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct CorrosionApiClient {
    api_addr: SocketAddr,
    api_client: hyper::Client<HttpConnector, Body>,
}

impl CorrosionApiClient {
    pub fn new(api_addr: SocketAddr) -> Self {
        Self {
            api_addr,
            api_client: hyper::Client::builder().http2_only(true).build_http(),
        }
    }

    pub async fn query(&self, statement: &Statement) -> Result<hyper::Body, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/v1/queries", self.api_addr))
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(statement)?))?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            let status = res.status();
            match hyper::body::to_bytes(res.into_body()).await {
                Ok(b) => match serde_json::from_slice(&b) {
                    Ok(res) => match res {
                        ExecResult::Error { error } => return Err(Error::ResponseError(error)),
                        res => return Err(Error::UnexpectedResult(res)),
                    },
                    Err(e) => {
                        debug!(
                            error = %e,
                            "could not deserialize response body, sending generic error..."
                        );
                        return Err(Error::UnexpectedStatusCode(status));
                    }
                },
                Err(e) => {
                    debug!(
                        error = %e,
                        "could not aggregate response body bytes, sending generic error..."
                    );
                    return Err(Error::UnexpectedStatusCode(status));
                }
            }
        }

        Ok(res.into_body())
    }

    pub async fn subscribe(
        &self,
        statement: &Statement,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream, Error> {
        let p_and_q: PathAndQuery = if let Some(change_id) = from {
            format!(
                "/v1/subscriptions?skip_rows={}&from={}",
                skip_rows, change_id.0
            )
            .try_into()?
        } else {
            format!("/v1/subscriptions?skip_rows={}", skip_rows).try_into()?
        };
        let url = hyper::Uri::builder()
            .scheme("http")
            .authority(self.api_addr.to_string())
            .path_and_query(p_and_q)
            .build()?;

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(url)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(statement)?))?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        // TODO: make that header name a const in corro-types
        let id = res
            .headers()
            .get(HeaderName::from_static("corro-query-id"))
            .and_then(|v| v.to_str().ok().and_then(|v| v.parse().ok()))
            .ok_or(Error::ExpectedQueryId)?;

        Ok(SubscriptionStream::new(
            id,
            self.api_client.clone(),
            self.api_addr,
            res.into_body(),
        ))
    }

    pub async fn subscription(
        &self,
        id: Uuid,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream, Error> {
        let p_and_q: PathAndQuery = if let Some(change_id) = from {
            format!(
                "/v1/subscriptions/{id}?skip_rows={}&from={}",
                skip_rows, change_id.0
            )
            .try_into()?
        } else {
            format!("/v1/subscriptions/{id}?skip_rows={}", skip_rows).try_into()?
        };
        let url = hyper::Uri::builder()
            .scheme("http")
            .authority(self.api_addr.to_string())
            .path_and_query(p_and_q)
            .build()?;

        let req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(url)
            .header(hyper::header::ACCEPT, "application/json")
            .body(hyper::Body::empty())?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        Ok(SubscriptionStream::new(
            id,
            self.api_client.clone(),
            self.api_addr,
            res.into_body(),
        ))
    }

    pub async fn execute(&self, statements: &[Statement]) -> Result<ExecResponse, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/v1/transactions", self.api_addr))
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(statements)?))?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        let bytes = hyper::body::to_bytes(res.into_body()).await?;

        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn schema(&self, statements: &[Statement]) -> Result<ExecResponse, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/v1/migrations", self.api_addr))
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(statements)?))?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        let bytes = hyper::body::to_bytes(res.into_body()).await?;

        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn schema_from_paths<P: AsRef<Path>>(
        &self,
        schema_paths: &[P],
    ) -> Result<Option<ExecResponse>, Error> {
        let mut statements = vec![];

        for schema_path in schema_paths.iter() {
            match tokio::fs::metadata(schema_path).await {
                Ok(meta) => {
                    if meta.is_dir() {
                        match tokio::fs::read_dir(schema_path).await {
                            Ok(mut dir) => {
                                let mut entries = vec![];

                                while let Ok(Some(entry)) = dir.next_entry().await {
                                    entries.push(entry);
                                }

                                let mut entries: Vec<_> = entries
                                    .into_iter()
                                    .filter_map(|entry| {
                                        entry.path().extension().and_then(|ext| {
                                            if ext == "sql" {
                                                Some(entry)
                                            } else {
                                                None
                                            }
                                        })
                                    })
                                    .collect();

                                entries.sort_by_key(|entry| entry.path());

                                for entry in entries.iter() {
                                    match tokio::fs::read_to_string(entry.path()).await {
                                        Ok(s) => {
                                            statements.push(Statement::Simple(s));
                                        }
                                        Err(e) => {
                                            warn!(
                                                "could not read schema file '{}', error: {e}",
                                                entry.path().display()
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "could not read dir '{}', error: {e}",
                                    schema_path.as_ref().display()
                                );
                            }
                        }
                    } else if meta.is_file() {
                        match tokio::fs::read_to_string(schema_path).await {
                            Ok(s) => {
                                statements.push(Statement::Simple(s));
                                // pushed.push(schema_path.clone());
                            }
                            Err(e) => {
                                warn!(
                                    "could not read schema file '{}', error: {e}",
                                    schema_path.as_ref().display()
                                );
                            }
                        }
                    }
                }

                Err(e) => {
                    warn!(
                        "could not read schema file meta '{}', error: {e}",
                        schema_path.as_ref().display()
                    );
                }
            }
        }

        if statements.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.schema(&statements).await?))
    }
}

#[derive(Clone)]
pub struct CorrosionClient {
    api_client: CorrosionApiClient,
    pool: sqlite_pool::RusqlitePool,
}

impl CorrosionClient {
    pub fn new<P: AsRef<Path>>(api_addr: SocketAddr, db_path: P) -> Self {
        Self {
            api_client: CorrosionApiClient::new(api_addr),
            pool: sqlite_pool::Config::new(db_path.as_ref())
                .max_size(5)
                .create_pool()
                .expect("could not build pool, this can't fail because we specified a runtime"),
        }
    }

    pub fn pool(&self) -> &sqlite_pool::RusqlitePool {
        &self.pool
    }
}

impl Deref for CorrosionClient {
    type Target = CorrosionApiClient;

    fn deref(&self) -> &Self::Target {
        &self.api_client
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    Http(#[from] hyper::http::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("received unexpected response code: {0}")]
    UnexpectedStatusCode(StatusCode),

    #[error("{0}")]
    ResponseError(String),

    #[error("unexpected result: {0:?}")]
    UnexpectedResult(ExecResult),

    #[error("could not retrieve subscription id from headers")]
    ExpectedQueryId,
}
