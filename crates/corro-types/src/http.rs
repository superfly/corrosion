use std::{
    error::Error,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{ready, Stream};
use hyper::Body;
use pin_project_lite::pin_project;
use tokio_util::codec::{Decoder, Encoder, LinesCodecError};

pin_project! {
    pub struct IoBodyStream {
        #[pin]
        pub body: Body
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

// type IoBodyStreamReader = StreamReader<IoBodyStream, Bytes>;
// type FramedBody = FramedRead<IoBodyStreamReader, LinesBytesCodec>;

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

impl<T> Encoder<T> for LinesBytesCodec
where
    T: AsRef<[u8]>,
{
    type Error = LinesCodecError;

    fn encode(&mut self, line: T, buf: &mut BytesMut) -> Result<(), LinesCodecError> {
        let line = line.as_ref();
        buf.reserve(line.len() + 1);
        buf.put(line);
        buf.put_u8(b'\n');
        Ok(())
    }
}
