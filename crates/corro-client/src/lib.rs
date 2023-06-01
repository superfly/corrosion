use std::{net::SocketAddr, path::Path, time::Duration};

use bb8_rusqlite::RusqliteConnectionManager;
use corro_types::api::{RqliteResponse, Statement};
use hyper::{client::HttpConnector, Body, StatusCode};

#[derive(Clone)]
pub struct CorrosionClient {
    api_addr: SocketAddr,
    pool: bb8::Pool<bb8_rusqlite::RusqliteConnectionManager>,
    api_client: hyper::Client<HttpConnector, Body>,
}

impl CorrosionClient {
    pub fn new<P: AsRef<Path>>(api_addr: SocketAddr, db_path: P) -> Self {
        Self {
            api_addr,
            pool: bb8::Pool::builder()
                .max_size(5)
                .max_lifetime(Some(Duration::from_secs(30)))
                .build_unchecked(RusqliteConnectionManager::new(&db_path)),
            api_client: hyper::Client::builder().http2_only(true).build_http(),
        }
    }

    pub fn pool(&self) -> &bb8::Pool<RusqliteConnectionManager> {
        &self.pool
    }

    pub async fn execute(&self, statements: &[Statement]) -> Result<RqliteResponse, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/db/execute?transaction", self.api_addr))
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
            .uri(format!("http://{}/db/schema", self.api_addr))
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
}
