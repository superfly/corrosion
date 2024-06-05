pub mod sub;

use corro_api_types::{ChangeId, ExecResponse, ExecResult, SqliteValue, Statement};
use http::uri::PathAndQuery;
use hyper::{client::HttpConnector, http::HeaderName, Body, StatusCode};
use serde::de::DeserializeOwned;
use std::{
    io,
    net::SocketAddr,
    ops::Deref,
    path::Path,
    sync::Arc,
    time::{self, Instant},
};
use sub::{QueryStream, SubscriptionStream};
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::{RwLock, RwLockReadGuard},
};
use tracing::{debug, info, warn};
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

    pub async fn query_typed<T: DeserializeOwned + Unpin>(
        &self,
        statement: &Statement,
    ) -> Result<QueryStream<T>, Error> {
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

        Ok(QueryStream::new(res.into_body()))
    }

    pub async fn query(
        &self,
        statement: &Statement,
    ) -> Result<QueryStream<Vec<SqliteValue>>, Error> {
        self.query_typed(statement).await
    }

    pub async fn subscribe_typed<T: DeserializeOwned + Unpin>(
        &self,
        statement: &Statement,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<T>, Error> {
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

    pub async fn subscribe(
        &self,
        statement: &Statement,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<Vec<SqliteValue>>, Error> {
        self.subscribe_typed(statement, skip_rows, from).await
    }

    pub async fn subscription_typed<T: DeserializeOwned + Unpin>(
        &self,
        id: Uuid,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<T>, Error> {
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

    pub async fn subscription(
        &self,
        id: Uuid,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<Vec<SqliteValue>>, Error> {
        self.subscription_typed(id, skip_rows, from).await
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

#[derive(Clone)]
pub struct CorrosionPooledClient<A> {
    inner: Arc<RwLock<PooledClientInner<A>>>,
}

struct PooledClientInner<A> {
    picker: AddrPicker<A>,

    // For how long to stick with a chosen server
    stickiness_timeout: time::Duration,
    // Currently chosen client
    client: Option<CorrosionApiClient>,
    // Whether or not the chosen client has made a successful request
    had_success: bool,
    // Time when the first fail occured after at least one successful call
    first_fail_at: Option<Instant>,
    // Current client generation, incremented after each client change
    generation: u64,
}

impl<A: ToSocketAddrs> CorrosionPooledClient<A> {
    pub fn new(addrs: A, stickiness_timeout: time::Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PooledClientInner {
                picker: AddrPicker::new(addrs),

                stickiness_timeout,
                client: None,
                had_success: false,
                first_fail_at: None,
                generation: 0,
            })),
        }
    }

    pub async fn query_typed<T: DeserializeOwned + Unpin>(
        &self,
        statement: &Statement,
    ) -> Result<QueryStream<T>, Error> {
        let (response, generation) = {
            let (client, generation) = self.get_client().await?;
            let response = client.query_typed(statement).await;

            (response, generation)
        };

        if matches!(response, Err(Error::Hyper(_))) {
            // We only care about I/O related errors
            self.handle_error(generation).await;
        } else {
            // The rest are considered a success
            self.handle_success(generation).await;
        }

        response
    }

    pub async fn subscribe_typed<T: DeserializeOwned + Unpin>(
        &self,
        statement: &Statement,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<T>, Error> {
        let (response, generation) = {
            let (client, generation) = self.get_client().await?;
            let response = client.subscribe_typed(statement, skip_rows, from).await;

            (response, generation)
        };

        if matches!(response, Err(Error::Hyper(_))) {
            // We only care about I/O related errors
            self.handle_error(generation).await;
        } else {
            // The rest are considered a success
            self.handle_success(generation).await;
        }

        response
    }

    pub async fn subscription_typed<T: DeserializeOwned + Unpin>(
        &self,
        id: Uuid,
        skip_rows: bool,
        from: Option<ChangeId>,
    ) -> Result<SubscriptionStream<T>, Error> {
        let (response, generation) = {
            let (client, generation) = self.get_client().await?;
            let response = client.subscription_typed(id, skip_rows, from).await;

            (response, generation)
        };

        if matches!(response, Err(Error::Hyper(_))) {
            // We only care about I/O related errors
            self.handle_error(generation).await;
        } else {
            // The rest are considered a success
            self.handle_success(generation).await;
        }

        response
    }

    async fn get_client(&self) -> Result<(RwLockReadGuard<'_, CorrosionApiClient>, u64), Error> {
        let mut inner = self.inner.write().await;
        let generation = inner.generation;

        if inner.client.is_none() {
            let addr = inner.picker.next().await?;
            info!(
                "next Corrosion server to attempt: {}, generation: {}",
                addr, generation
            );
            inner.client = Some(CorrosionApiClient::new(addr))
        }

        Ok((
            RwLockReadGuard::map(inner.downgrade(), |inner| inner.client.as_ref().unwrap()),
            generation,
        ))
    }

    async fn handle_success(&self, generation: u64) {
        let mut inner = self.inner.write().await;

        // Even though the call was successul, another failed call has advanced the client already.
        if inner.generation != generation {
            return;
        }

        // Mark that this client was able to perform a successful call to Corrosion
        // and we should stick with it.
        inner.had_success = true;
        // And reset the time of the first fail.
        inner.first_fail_at = None;
    }

    async fn handle_error(&self, generation: u64) {
        let mut inner = self.inner.write().await;

        // Somebody else has already handled an error with this client
        if generation != inner.generation {
            return;
        }

        match inner.first_fail_at {
            // First fail after success
            None if inner.had_success => {
                inner.first_fail_at = Some(Instant::now());
            }

            // Still within stickiness timeout, try the same server again
            Some(first) if Instant::now().duration_since(first) < inner.stickiness_timeout => {}

            // Otherwise, pick a new server for the next attempt
            _ => {
                // If we had a successful call before, try to fallback to the first server, so the first
                // one is always preferred by all the clients.
                // Otherwise, continue iterating over the rest of the servers.
                if inner.had_success {
                    inner.picker.reset()
                }

                inner.client = None;
                inner.first_fail_at = None;
                inner.had_success = false;
                inner.generation += 1;
            }
        }
    }
}

struct AddrPicker<A> {
    addrs: A,

    last_addrs: Option<Vec<SocketAddr>>,
    cur_addr: usize,
}

impl<A: ToSocketAddrs> AddrPicker<A> {
    fn new(addrs: A) -> AddrPicker<A> {
        Self {
            addrs,
            last_addrs: None,
            cur_addr: 0,
        }
    }

    async fn next(&mut self) -> Result<SocketAddr, Error> {
        // Either we don't have any address or we tried them all, resolve again.
        if self.cur_addr
            >= self
                .last_addrs
                .as_ref()
                .map(|v| v.len())
                .unwrap_or_default()
        {
            let mut addrs = lookup_host(&self.addrs).await?.collect::<Vec<_>>();
            // Sort so all the nodes try the addresses in the same order
            addrs.sort();

            debug!("got the following Corrosion servers: {:?}", addrs);

            self.last_addrs = Some(addrs);
            self.cur_addr = 0;
        }

        if let Some(addr) = self
            .last_addrs
            .as_ref()
            .and_then(|a| a.get(self.cur_addr).copied())
        {
            self.cur_addr += 1;

            Ok(addr)
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "DNS didn't return any addresses").into())
        }
    }

    fn reset(&mut self) {
        self.cur_addr = 0;
        self.last_addrs = None;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dns(#[from] io::Error),
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
