use std::{net::SocketAddr, ops::Deref, path::Path, pin::Pin, time::Duration};

use async_stream::try_stream;
use bb8_rusqlite::RusqliteConnectionManager;
use compact_str::CompactString;
use corro_types::{
    api::{RqliteResponse, Statement},
    pubsub::{Subscription, SubscriptionId, SubscriptionMessage},
};
use futures::{
    stream::{SplitSink, SplitStream},
    Sink, SinkExt, Stream, StreamExt,
};
use hyper::{client::HttpConnector, Body, StatusCode};
use pin_project_lite::pin_project;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite as ws, MaybeTlsStream, WebSocketStream};
use tracing::warn;

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

    pub async fn query(&self, statements: &[Statement]) -> Result<RqliteResponse, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/db/query", self.api_addr))
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

    pub async fn subscribe<C: Into<CompactString>>(
        &self,
        id: C,
        where_clause: Option<String>,
    ) -> Result<SubConn, Error> {
        let (ws, _ws_res) =
            tokio_tungstenite::connect_async(format!("ws://{}/v1/subscribe", self.api_addr))
                .await?;

        let (sink, stream) = ws.split();

        let mut sub = SubConn::new(stream, sink);

        sub.send(Subscription::Add {
            id: SubscriptionId(id.into()),
            where_clause,
            from_db_version: None,
            is_priority: false,
        })
        .await?;
        sub.flush().await?;

        Ok(sub)
    }
}

pin_project! {
    pub struct SubConn {
        #[pin]
        stream: Pin<Box<dyn Stream<Item=Result<SubscriptionMessage, SubRecvError>>>>,
        #[pin]
        sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, ws::Message>,
    }
}

impl SubConn {
    fn new(
        mut ws_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, ws::Message>,
    ) -> Self {
        let stream = try_stream! {
            while let Some(msg) = ws_stream.next().await {
                let event: SubscriptionMessage = match msg? {
                    ws::Message::Text(s) => {
                        serde_json::from_str(s.as_str())?
                    },
                    ws::Message::Binary(v) => {
                        serde_json::from_slice(v.as_slice())?
                    },
                    ws::Message::Close(_) => {
                        break;
                    },
                    _ => continue
                };

                yield event
            }
        };
        Self {
            sink,
            stream: Box::pin(stream),
        }
    }
}

impl Stream for SubConn {
    type Item = Result<SubscriptionMessage, SubRecvError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl Sink<Subscription> for SubConn {
    type Error = SubSendError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_ready(cx)
            .map_err(SubSendError::from)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: Subscription) -> Result<(), Self::Error> {
        let b = serde_json::to_vec(&item)?;
        self.project()
            .sink
            .start_send(ws::Message::Binary(b))
            .map_err(SubSendError::from)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_flush(cx)
            .map_err(SubSendError::from)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_close(cx)
            .map_err(SubSendError::from)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubRecvError {
    #[error(transparent)]
    Ws(#[from] ws::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SubSendError {
    #[error(transparent)]
    Ws(#[from] ws::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct CorrosionClient {
    api_client: CorrosionApiClient,
    pool: bb8::Pool<bb8_rusqlite::RusqliteConnectionManager>,
}

impl CorrosionClient {
    pub fn new<P: AsRef<Path>>(api_addr: SocketAddr, db_path: P) -> Self {
        Self {
            api_client: CorrosionApiClient::new(api_addr),
            pool: bb8::Pool::builder()
                .max_size(5)
                .max_lifetime(Some(Duration::from_secs(30)))
                .build_unchecked(RusqliteConnectionManager::new(&db_path)),
        }
    }

    pub fn pool(&self) -> &bb8::Pool<RusqliteConnectionManager> {
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
    #[error(transparent)]
    Ws(#[from] ws::Error),
    #[error(transparent)]
    WsSend(#[from] SubSendError),

    #[error("received unexpected response code: {0}")]
    UnexpectedStatusCode(StatusCode),
}
