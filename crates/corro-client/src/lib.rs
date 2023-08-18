use std::{error::Error as StdError, io, net::SocketAddr, ops::Deref, path::Path, time::Duration};

use bytes::{Buf, BytesMut};
use corro_api_types::{sqlite::RusqliteConnManager, QueryEvent, RqliteResponse, Statement};
use futures::{Stream, TryStreamExt};
use hyper::{client::HttpConnector, http::HeaderName, Body, StatusCode};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::{
    codec::{Decoder, FramedRead, LinesCodecError},
    io::StreamReader,
};
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

    pub async fn watch(
        &self,
        statement: &Statement,
    ) -> Result<
        (
            Uuid,
            impl Stream<Item = io::Result<QueryEvent>> + Send + Sync + 'static,
        ),
        Error,
    > {
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

        Ok((id, watch_stream(res.into_body())))
    }

    pub async fn watched_query(
        &self,
        id: Uuid,
    ) -> Result<impl Stream<Item = io::Result<QueryEvent>> + Send + Sync + 'static, Error> {
        let req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(format!("http://{}/v1/watches/{}", self.api_addr, id))
            .header(hyper::header::ACCEPT, "application/json")
            .body(hyper::Body::empty())?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(Error::UnexpectedStatusCode(res.status()));
        }

        Ok(watch_stream(res.into_body()))
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

fn watch_stream(body: hyper::Body) -> impl Stream<Item = io::Result<QueryEvent>> {
    let body = StreamReader::new(body.map_err(|e| {
        if let Some(io_error) = e
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
        {
            return io::Error::from(io_error.kind());
        }
        io::Error::new(io::ErrorKind::Other, e)
    }));

    let framed = FramedRead::new(body, LinesBytesCodec::new())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

    SymmetricallyFramed::new(framed, SymmetricalJson::<QueryEvent>::default())
}

struct LinesBytesCodec {
    // Stored index of the next index to examine for a `\n` character.
    // This is used to optimize searching.
    // For example, if `decode` was called with `abc`, it would hold `3`,
    // because that is the next index to examine.
    // The next time `decode` is called with `abcde\n`, the method will
    // only look at `de\n` before returning.
    next_index: usize,

    /// The maximum length for a given line. If `usize::MAX`, lines will be
    /// read until a `\n` character is reached.
    max_length: usize,

    /// Are we currently discarding the remainder of a line which was over
    /// the length limit?
    is_discarding: bool,
}

impl LinesBytesCodec {
    /// Returns a `LinesBytesCodec` for splitting up data into lines.
    ///
    /// # Note
    ///
    /// The returned `LinesBytesCodec` will not have an upper bound on the length
    /// of a buffered line. See the documentation for [`new_with_max_length`]
    /// for information on why this could be a potential security risk.
    ///
    /// [`new_with_max_length`]: crate::codec::LinesBytesCodec::new_with_max_length()
    pub fn new() -> LinesBytesCodec {
        LinesBytesCodec {
            next_index: 0,
            max_length: usize::MAX,
            is_discarding: false,
        }
    }
}

impl Decoder for LinesBytesCodec {
    type Item = BytesMut;
    type Error = LinesCodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, LinesCodecError> {
        loop {
            // Determine how far into the buffer we'll search for a newline. If
            // there's no max_length set, we'll read to the end of the buffer.
            let read_to = std::cmp::min(self.max_length.saturating_add(1), buf.len());

            let newline_offset = buf[self.next_index..read_to]
                .iter()
                .position(|b| *b == b'\n');

            match (self.is_discarding, newline_offset) {
                (true, Some(offset)) => {
                    // If we found a newline, discard up to that offset and
                    // then stop discarding. On the next iteration, we'll try
                    // to read a line normally.
                    buf.advance(offset + self.next_index + 1);
                    self.is_discarding = false;
                    self.next_index = 0;
                }
                (true, None) => {
                    // Otherwise, we didn't find a newline, so we'll discard
                    // everything we read. On the next iteration, we'll continue
                    // discarding up to max_len bytes unless we find a newline.
                    buf.advance(read_to);
                    self.next_index = 0;
                    if buf.is_empty() {
                        return Ok(None);
                    }
                }
                (false, Some(offset)) => {
                    // Found a line!
                    let newline_index = offset + self.next_index;
                    self.next_index = 0;
                    let mut line = buf.split_to(newline_index + 1);
                    line.truncate(line.len() - 1);
                    without_carriage_return(&mut line);
                    return Ok(Some(line));
                }
                (false, None) if buf.len() > self.max_length => {
                    // Reached the maximum length without finding a
                    // newline, return an error and start discarding on the
                    // next call.
                    self.is_discarding = true;
                    return Err(LinesCodecError::MaxLineLengthExceeded);
                }
                (false, None) => {
                    // We didn't find a line or reach the length limit, so the next
                    // call will resume searching at the current offset.
                    self.next_index = read_to;
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, LinesCodecError> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                // No terminating newline - return remaining data, if any
                if buf.is_empty() || buf == &b"\r"[..] {
                    None
                } else {
                    let mut line = buf.split_to(buf.len());
                    line.truncate(line.len() - 1);
                    without_carriage_return(&mut line);
                    self.next_index = 0;
                    Some(line)
                }
            }
        })
    }
}

fn without_carriage_return(s: &mut BytesMut) {
    if let Some(&b'\r') = s.last() {
        s.truncate(s.len() - 1);
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
