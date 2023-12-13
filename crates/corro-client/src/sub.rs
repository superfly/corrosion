use std::{
    error::Error,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use corro_api_types::ChangeId;
use futures::{ready, Future, Stream};
use hyper::{client::HttpConnector, Body};
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;
use tokio::time::{sleep, Sleep};
use tokio_util::{
    codec::{Decoder, FramedRead, LinesCodecError},
    io::StreamReader,
};
use tracing::error;
use uuid::Uuid;

use super::TypedQueryEvent;

pin_project! {
    pub struct IoBodyStream {
        #[pin]
        body: Body
    }
}

impl Stream for IoBodyStream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = ready!(this.body.poll_next(cx));
        match res {
            Some(Ok(b)) => Poll::Ready(Some(Ok(b))),
            Some(Err(e)) => {
                let io_err = match e
                    .source()
                    .and_then(|source| source.downcast_ref::<io::Error>())
                {
                    Some(io_err) => io::Error::from(io_err.kind()),
                    None => io::Error::new(io::ErrorKind::Other, e),
                };
                Poll::Ready(Some(Err(io_err)))
            }
            None => Poll::Ready(None),
        }
    }
}

type IoBodyStreamReader = StreamReader<IoBodyStream, Bytes>;
type FramedBody = FramedRead<IoBodyStreamReader, LinesBytesCodec>;

pub struct SubscriptionStream<T> {
    id: Uuid,
    client: hyper::Client<HttpConnector, Body>,
    api_addr: SocketAddr,
    observed_eoq: bool,
    last_change_id: Option<ChangeId>,
    stream: Option<FramedBody>,
    backoff: Option<Pin<Box<Sleep>>>,
    backoff_count: u32,
    response: Option<hyper::client::ResponseFuture>,
    _deser: std::marker::PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
    #[error("missed a change, inconsistent state")]
    MissedChange,
    #[error("max line length exceeded")]
    MaxLineLengthExceeded,
    #[error("initial query never finished")]
    UnfinishedQuery,
    #[error("max retry attempts exceeded")]
    MaxRetryAttempts,
}

impl<T> SubscriptionStream<T>
where
    T: DeserializeOwned + Unpin,
{
    pub fn new(
        id: Uuid,
        client: hyper::Client<HttpConnector, Body>,
        api_addr: SocketAddr,
        body: hyper::Body,
    ) -> Self {
        Self {
            id,
            client,
            api_addr,
            observed_eoq: false,
            last_change_id: None,
            stream: Some(FramedRead::new(
                StreamReader::new(IoBodyStream { body }),
                LinesBytesCodec::default(),
            )),
            backoff: None,
            backoff_count: 0,
            response: None,
            _deser: Default::default(),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    fn poll_stream(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<TypedQueryEvent<T>, SubscriptionError>>> {
        let stream = loop {
            match self.stream.as_mut() {
                None => match ready!(self.as_mut().poll_request(cx)) {
                    Ok(stream) => {
                        self.stream = Some(stream);
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Some(stream) => {
                    break stream;
                }
            }
        };

        let res = ready!(Pin::new(stream).poll_next(cx));
        match res {
            Some(Ok(b)) => match serde_json::from_slice(&b) {
                Ok(evt) => {
                    if let TypedQueryEvent::EndOfQuery { change_id, .. } = &evt {
                        self.observed_eoq = true;
                        self.last_change_id = *change_id;
                    }
                    if let TypedQueryEvent::Change(_, _, _, change_id) = &evt {
                        if matches!(self.last_change_id, Some(id) if id.0 + 1 != change_id.0) {
                            return Poll::Ready(Some(Err(SubscriptionError::MissedChange)));
                        }
                        self.last_change_id = Some(*change_id);
                    }
                    Poll::Ready(Some(Ok(evt)))
                }
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => match e {
                LinesCodecError::MaxLineLengthExceeded => {
                    Poll::Ready(Some(Err(SubscriptionError::MaxLineLengthExceeded)))
                }
                LinesCodecError::Io(io_err) => Poll::Ready(Some(Err(io_err.into()))),
            },
            None => Poll::Ready(None),
        }
    }

    fn poll_request(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<FramedBody, SubscriptionError>> {
        loop {
            if let Some(res_fut) = self.response.as_mut() {
                // return early w/ Poll::Pending if response is not ready
                let res = ready!(Pin::new(res_fut).poll(cx));

                // reset response
                self.response = None;

                return match res {
                    Ok(res) => Poll::Ready(Ok(FramedRead::new(
                        StreamReader::new(IoBodyStream {
                            body: res.into_body(),
                        }),
                        LinesBytesCodec::default(),
                    ))),
                    Err(e) => {
                        let io_err = match e
                            .source()
                            .and_then(|source| source.downcast_ref::<io::Error>())
                        {
                            Some(io_err) => io::Error::from(io_err.kind()),
                            None => io::Error::new(io::ErrorKind::Other, e),
                        };
                        Poll::Ready(Err(io_err.into()))
                    }
                };
            } else if self.observed_eoq {
                let req = hyper::Request::builder()
                    .method(hyper::Method::GET)
                    .uri(format!(
                        "http://{}/v1/subscriptions/{}?from={}",
                        self.api_addr,
                        self.id,
                        self.last_change_id.unwrap_or_default()
                    ))
                    .header(hyper::header::ACCEPT, "application/json")
                    .body(hyper::Body::empty())?;

                let response = self.client.request(req);
                self.response = Some(response);
                // loop around!
            } else {
                return Poll::Ready(Err(SubscriptionError::UnfinishedQuery));
            }
        }
    }
}

impl<T> Stream for SubscriptionStream<T>
where
    T: DeserializeOwned + Unpin,
{
    type Item = Result<TypedQueryEvent<T>, SubscriptionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // first, check if we need to wait for a backoff...
        if let Some(backoff) = self.backoff.as_mut() {
            ready!(backoff.as_mut().poll(cx));
            self.backoff = None;
            self.backoff_count = 0;
        }

        let io_err = match ready!(self.as_mut().poll_stream(cx)) {
            Some(Err(SubscriptionError::Io(io_err))) => io_err,
            other => return Poll::Ready(other),
        };

        // reset the stream
        self.stream = None;

        if self.backoff_count >= 30 {
            return Poll::Ready(Some(Err(SubscriptionError::MaxRetryAttempts)));
        }

        error!("encountered a stream IO error: {io_err}, retrying in a bit");

        let mut backoff = Box::pin(sleep(Duration::from_secs(1)));

        // register w/ waker
        _ = backoff.as_mut().poll(cx);

        // this can't return Ready, right?
        self.backoff = Some(backoff);

        self.backoff_count += 1;

        Poll::Pending
    }
}

pub struct LinesBytesCodec {
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

impl Default for LinesBytesCodec {
    /// Returns a `LinesBytesCodec` for splitting up data into lines.
    ///
    /// # Note
    ///
    /// The returned `LinesBytesCodec` will not have an upper bound on the length
    /// of a buffered line. See the documentation for [`new_with_max_length`]
    /// for information on why this could be a potential security risk.
    ///
    /// [`new_with_max_length`]: crate::codec::LinesBytesCodec::default_with_max_length()
    fn default() -> Self {
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
