pub mod sub;

use corro_api_types::{ChangeId, ExecResponse, ExecResult, SqliteValue, Statement};
use hickory_resolver::{
    name_server::TokioConnectionProvider, ResolveError, ResolveErrorKind, Resolver,
};
use http::uri::PathAndQuery;
use hyper::{client::HttpConnector, http::HeaderName, Body, StatusCode};
use serde::de::DeserializeOwned;
use std::{
    net::SocketAddr,
    ops::Deref,
    path::Path,
    sync::Arc,
    time::{self, Duration, Instant},
};
use sub::{QueryStream, SubscriptionStream, UpdatesStream};
use tokio::{
    sync::{RwLock, RwLockReadGuard},
    time::timeout,
};
use tracing::{debug, info};
use uuid::Uuid;

const HTTP2_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const DNS_RESOLVE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct CorrosionApiClient {
    api_addr: SocketAddr,
    api_client: hyper::Client<HttpConnector, Body>,
}

impl CorrosionApiClient {
    pub fn new(api_addr: SocketAddr) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(HTTP2_CONNECT_TIMEOUT));
        Self {
            api_addr,
            api_client: hyper::Client::builder()
                .http2_only(true)
                .http2_keep_alive_interval(Some(HTTP2_KEEP_ALIVE_INTERVAL))
                .http2_keep_alive_timeout(HTTP2_KEEP_ALIVE_INTERVAL / 2)
                .build(connector),
        }
    }

    pub async fn query_typed<T: DeserializeOwned + Unpin>(
        &self,
        statement: &Statement,
        timeout: Option<u64>,
    ) -> Result<QueryStream<T>, Error> {
        let params = timeout.map(|t| format!("?timeout={t}")).unwrap_or_default();
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/v1/queries{}", self.api_addr, params))
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
        timeout: Option<u64>,
    ) -> Result<QueryStream<Vec<SqliteValue>>, Error> {
        self.query_typed(statement, timeout).await
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
            format!("/v1/subscriptions?skip_rows={skip_rows}").try_into()?
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
        let hash = res
            .headers()
            .get(HeaderName::from_static("corro-query-hash"))
            .and_then(|v| v.to_str().map(ToOwned::to_owned).ok());

        Ok(SubscriptionStream::new(
            id,
            hash,
            self.api_client.clone(),
            self.api_addr,
            res.into_body(),
            from,
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
            format!("/v1/subscriptions/{id}?skip_rows={skip_rows}").try_into()?
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

        let hash = res
            .headers()
            .get(HeaderName::from_static("corro-query-hash"))
            .and_then(|v| v.to_str().map(ToOwned::to_owned).ok());

        Ok(SubscriptionStream::new(
            id,
            hash,
            self.api_client.clone(),
            self.api_addr,
            res.into_body(),
            from,
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

    pub async fn updates_typed<T: DeserializeOwned + Unpin>(
        &self,
        table: &str,
    ) -> Result<UpdatesStream<T>, Error> {
        let p_and_q: PathAndQuery = format!("/v1/updates/{table}").try_into()?;

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
            .body(hyper::Body::empty())?;

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

        Ok(UpdatesStream::new(id, res.into_body()))
    }

    pub async fn updates(&self, table: &str) -> Result<UpdatesStream<Vec<SqliteValue>>, Error> {
        self.updates_typed(table).await
    }

    pub async fn execute(
        &self,
        statements: &[Statement],
        timeout: Option<u64>,
    ) -> Result<ExecResponse, Error> {
        let uri = if let Some(timeout) = timeout {
            format!(
                "http://{}/v1/transactions?timeout={}",
                self.api_addr, timeout
            )
        } else {
            format!("http://{}/v1/transactions", self.api_addr)
        };
        // println!("uri: {:?}", uri);
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(statements)?))?;

        let res = self.api_client.request(req).await?;

        let status = res.status();
        if !status.is_success() {
            match hyper::body::to_bytes(res.into_body()).await {
                Ok(b) => match serde_json::from_slice(&b) {
                    Ok(ExecResponse { results, .. }) => {
                        if let Some(ExecResult::Error { error }) = results
                            .into_iter()
                            .find(|r| matches!(r, ExecResult::Error { .. }))
                        {
                            return Err(Error::ResponseError(error));
                        }
                        return Err(Error::UnexpectedStatusCode(status));
                    }
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
        let statements: Vec<Statement> = corro_utils::read_files_from_paths(schema_paths)
            .await
            .map_err(|e| Error::ResponseError(e.to_string()))?
            .into_iter()
            .map(Statement::Simple)
            .collect();

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

    pub fn with_sqlite_pool(api_addr: SocketAddr, pool: sqlite_pool::RusqlitePool) -> Self {
        Self {
            api_client: CorrosionApiClient::new(api_addr),
            pool,
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
pub struct CorrosionPooledClient {
    inner: Arc<RwLock<PooledClientInner>>,
}

struct PooledClientInner {
    picker: AddrPicker,

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

impl CorrosionPooledClient {
    pub fn new(
        addrs: Vec<String>,
        stickiness_timeout: time::Duration,
        resolver: Resolver<TokioConnectionProvider>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PooledClientInner {
                picker: AddrPicker::new(addrs, resolver),

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
        timeout: Option<u64>,
    ) -> Result<QueryStream<T>, Error> {
        let (response, generation) = {
            let (client, generation) = self.get_client().await?;
            let response = client.query_typed(statement, timeout).await;

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

struct AddrPicker {
    // Resolver used to resolve the addresses
    resolver: Resolver<TokioConnectionProvider>,
    // List of addresses/hostname to try in order
    addrs: Vec<String>,
    // Next address/hostname to try
    next_addr: usize,

    // List of addresses returned by the last resolve attempt
    last_resolved_addrs: Option<Vec<SocketAddr>>,
    // Next address to return
    next_resolved_addr: usize,
}

impl AddrPicker {
    fn new(addrs: Vec<String>, resolver: Resolver<TokioConnectionProvider>) -> AddrPicker {
        Self {
            resolver,
            addrs,
            next_addr: 0,

            last_resolved_addrs: None,
            next_resolved_addr: 0,
        }
    }

    async fn next(&mut self) -> Result<SocketAddr, Error> {
        // Either we don't have any address or we tried them all, resolve again.
        if self.next_resolved_addr
            >= self
                .last_resolved_addrs
                .as_ref()
                .map(|v| v.len())
                .unwrap_or_default()
        {
            let host_port = self
                .addrs
                .get(self.next_addr)
                .ok_or(ResolveError::from("No addresses available"))?;
            self.next_addr = (self.next_addr + 1) % self.addrs.len();

            let mut addrs = if let Ok(addr) = host_port.parse() {
                vec![addr]
            } else {
                // split host port
                let (host, port) = host_port
                    .rsplit_once(':')
                    .and_then(|(host, port)| Some((host, port.parse().ok()?)))
                    .ok_or(ResolveError::from("Invalid Corrosion server address"))?;

                timeout(DNS_RESOLVE_TIMEOUT, self.resolver.lookup_ip(host))
                    .await
                    .map_err(|_| ResolveError::from(ResolveErrorKind::Message("timeout")))??
                    .iter()
                    .map(|addr| (addr, port).into())
                    .collect::<Vec<_>>()
            };
            // Sort so all the nodes try the addresses in the same order
            addrs.sort();

            debug!("got the following Corrosion servers: {:?}", addrs);

            self.last_resolved_addrs = Some(addrs);
            self.next_resolved_addr = 0;
        }

        if let Some(addr) = self
            .last_resolved_addrs
            .as_ref()
            .and_then(|a| a.get(self.next_resolved_addr).copied())
        {
            self.next_resolved_addr += 1;

            Ok(addr)
        } else {
            Err(ResolveError::from("DNS didn't return any addresses").into())
        }
    }

    fn reset(&mut self) {
        self.next_addr = 0;
        self.last_resolved_addrs = None;
        self.next_resolved_addr = 0;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dns(#[from] ResolveError),
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

#[cfg(test)]
mod tests {
    use crate::{CorrosionPooledClient, Error};
    use corro_api_types::SqliteValue;
    use hickory_resolver::Resolver;
    use hyper::{header::HeaderValue, service::service_fn, Body, Request, Response};
    use std::{
        convert::Infallible,
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::{net::TcpListener, pin, sync::broadcast};
    use uuid::Uuid;

    struct Server {
        id: Uuid,
        addr: SocketAddr,
        refuse: Arc<AtomicBool>,
        drop_conn_tx: broadcast::Sender<()>,
    }

    impl Server {
        async fn new(id: Uuid) -> Self {
            let refuse = Arc::new(AtomicBool::new(false));
            let (drop_conn_tx, drop_conn_rx) = broadcast::channel::<()>(1);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            tokio::spawn({
                let refuse = refuse.clone();

                async move {
                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        if refuse.load(Ordering::Relaxed) {
                            drop(stream);
                            continue;
                        }

                        let mut drop_conn_rx = drop_conn_rx.resubscribe();
                        tokio::spawn(async move {
                            let http = hyper::server::conn::Http::new();
                            let conn = http.serve_connection(
                                stream,
                                service_fn(move |_: Request<Body>| async move {
                                    let mut res = Response::new(Body::empty());
                                    res.headers_mut().insert(
                                        "corro-query-id",
                                        HeaderValue::from_str(&id.to_string()).unwrap(),
                                    );
                                    Ok::<_, Infallible>(res)
                                }),
                            );
                            pin!(conn);

                            tokio::select! {
                                _ = conn.as_mut() => (),
                                _ = drop_conn_rx.recv() => (),
                            }
                        });
                    }
                }
            });

            Server {
                id,
                addr,
                refuse,
                drop_conn_tx,
            }
        }

        fn refuse_new_conns(&self, refuse: bool) {
            self.refuse.store(refuse, Ordering::Relaxed)
        }

        fn kill_existing_conns(&self) {
            _ = self.drop_conn_tx.send(())
        }
    }

    async fn gen_servers(num: usize) -> (Vec<Server>, Vec<String>) {
        let mut servers = Vec::new();

        for _ in 0..num {
            servers.push(Server::new(Uuid::new_v4()).await);
        }

        // sort the way the client is supposed to try them
        servers.sort_by(|a, b| a.addr.partial_cmp(&b.addr).unwrap());
        let addrs = servers.iter().map(|s| s.addr.to_string()).collect();

        (servers, addrs)
    }

    #[tokio::test]
    async fn test_single_address() {
        let statement = "".into();
        let (servers, addresses) = gen_servers(1).await;

        let resolver = Resolver::builder_tokio().unwrap().build();
        let client = CorrosionPooledClient::new(addresses, Duration::from_nanos(1), resolver);
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), servers[0].id);

        // Drop the connection, next attempt should error
        servers[0].kill_existing_conns();

        let res = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await;
        assert!(matches!(res, Result::Err(Error::Hyper(_))));

        // But the new one should succeed
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), servers[0].id);
    }

    #[tokio::test]
    async fn test_multiple_addresses() {
        let statement = "".into();
        let (servers, addresses) = gen_servers(3).await;

        let resolver = Resolver::builder_tokio().unwrap().build();
        let client = CorrosionPooledClient::new(addresses, Duration::from_nanos(1), resolver);

        // Refuse connections on the first server
        servers[0].refuse_new_conns(true);

        // First one should error
        let res = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await;
        assert!(matches!(res, Result::Err(Error::Hyper(_))));

        // Second one should succeed
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), servers[1].id);

        // Abort the second server, the client should fallback back to the first one after the first two attempts
        servers[1].kill_existing_conns();
        servers[1].refuse_new_conns(true);
        servers[0].refuse_new_conns(false);

        // First and second one should error
        for _ in 0..2 {
            let res = client
                .subscribe_typed::<SqliteValue>(&statement, false, None)
                .await;
            assert!(matches!(res, Result::Err(Error::Hyper(_))));
        }

        // The next one should succeed
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), servers[0].id);
    }

    #[tokio::test]
    async fn test_multiple_addresses_sticky() {
        let statement = "".into();
        let (servers, addresses) = gen_servers(3).await;

        let resolver = Resolver::builder_tokio().unwrap().build();
        let client = CorrosionPooledClient::new(addresses, Duration::from_millis(50), resolver);

        // Refuse connections on the first server
        servers[0].refuse_new_conns(true);

        // First one should error
        let res = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await;
        assert!(matches!(res, Result::Err(Error::Hyper(_))));

        // Second one should succeed
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), servers[1].id);

        // Abort the second server, the client should continue trying it until the timeout expires
        servers[1].kill_existing_conns();
        servers[1].refuse_new_conns(true);
        servers[0].refuse_new_conns(false);

        let mut attempts = 0;
        loop {
            let res = client
                .subscribe_typed::<SqliteValue>(&statement, false, None)
                .await;

            match res {
                Ok(sub) => {
                    assert_eq!(sub.id(), servers[0].id);
                    break;
                }
                Err(_) => attempts += 1,
            };
        }
        assert!(attempts > 2);
    }

    #[tokio::test]
    async fn test_more_servers() {
        let statement = "".into();
        let (pool1_servers, pool1_addresses) = gen_servers(2).await;
        let (pool2_servers, pool2_addresses) = gen_servers(2).await;

        let mut addresses = pool1_addresses;
        addresses.extend_from_slice(&pool2_addresses);

        let resolver = Resolver::builder_tokio().unwrap().build();
        let client = CorrosionPooledClient::new(addresses, Duration::from_nanos(1), resolver);

        // Refuse connections on all servers
        for i in 0..2 {
            pool1_servers[i].refuse_new_conns(true);
            pool2_servers[i].refuse_new_conns(true);
        }

        // Try to connect multiple times, all should fail
        for _ in 0..15 {
            let res = client
                .subscribe_typed::<SqliteValue>(&statement, false, None)
                .await;
            assert!(matches!(res, Result::Err(Error::Hyper(_))));
        }

        // Accept connections on first server in the backup pool
        pool2_servers[0].refuse_new_conns(false);
        for i in 0..4 {
            let res = client
                .subscribe_typed::<SqliteValue>(&statement, false, None)
                .await;
            match res {
                Result::Err(_) => (),
                Ok(sub) => {
                    assert_eq!(sub.id(), pool2_servers[0].id);
                    break;
                }
            }
            assert!(i != 3);
        }

        // Kill the connection, it should fallback to the first pool
        pool2_servers[0].kill_existing_conns();
        pool2_servers[0].refuse_new_conns(true);
        pool1_servers[0].refuse_new_conns(false);
        pool1_servers[1].refuse_new_conns(false);

        // First and second one should error
        for _ in 0..2 {
            let res = client
                .subscribe_typed::<SqliteValue>(&statement, false, None)
                .await;
            assert!(matches!(res, Result::Err(Error::Hyper(_))));
        }

        // Thirst one should succeed
        let sub = client
            .subscribe_typed::<SqliteValue>(&statement, false, None)
            .await
            .unwrap();
        assert_eq!(sub.id(), pool1_servers[0].id);
    }
}
