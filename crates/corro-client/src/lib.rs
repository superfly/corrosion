use std::{net::SocketAddr, ops::Deref, path::Path, time::Duration};

use corro_types::{
    api::{RqliteResponse, Statement},
    sqlite::RusqliteConnManager,
};
use hyper::{client::HttpConnector, http::HeaderName, Body, StatusCode};
use tracing::warn;
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
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        Ok(res.into_body())
    }

    pub async fn watch(&self, statement: &Statement) -> Result<(Uuid, hyper::Body), Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/v1/watches", self.api_addr))
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

        Ok((id, res.into_body()))
    }

    pub async fn watched_query(&self, id: Uuid) -> Result<hyper::Body, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(format!("http://{}/v1/watches/{}", self.api_addr, id))
            .header(hyper::header::ACCEPT, "application/json")
            .body(hyper::Body::empty())?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        Ok(res.into_body())
    }

    pub async fn execute(&self, statements: &[Statement]) -> Result<RqliteResponse, Error> {
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

    pub async fn schema(&self, statements: &[Statement]) -> Result<RqliteResponse, Error> {
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
    ) -> Result<RqliteResponse, Error> {
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
                                            // pushed.push(
                                            //     entry.path().to_string_lossy().to_string().into(),
                                            // );
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

        self.schema(&statements).await
    }
}

#[derive(Clone)]
pub struct CorrosionClient {
    api_client: CorrosionApiClient,
    pool: bb8::Pool<RusqliteConnManager>,
}

impl CorrosionClient {
    pub fn new<P: AsRef<Path>>(api_addr: SocketAddr, db_path: P) -> Self {
        Self {
            api_client: CorrosionApiClient::new(api_addr),
            pool: bb8::Pool::builder()
                .max_size(5)
                .max_lifetime(Some(Duration::from_secs(30)))
                .build_unchecked(RusqliteConnManager::new(&db_path)),
        }
    }

    pub fn pool(&self) -> &bb8::Pool<RusqliteConnManager> {
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
    Http(#[from] hyper::http::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("received unexpected response code: {0}")]
    UnexpectedStatusCode(StatusCode),

    #[error("could not retrieve watch id from headers")]
    ExpectedQueryId,
}
