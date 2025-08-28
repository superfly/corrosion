use http_body::Frame;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync;

pin_project_lite::pin_project! {
    /// A body backed by a channel.
    pub struct Channel<D> {
        rx_frame: sync::mpsc::Receiver<Frame<D>>,
    }
}

impl<D> Channel<D> {
    /// Create a new channel body.
    ///
    /// The channel will buffer up to the provided number of messages. Once the buffer is full,
    /// attempts to send new messages will wait until a message is received from the channel. The
    /// provided buffer capacity must be at least 1.
    pub fn new(buffer: usize) -> (Sender<D>, Self) {
        let (tx_frame, rx_frame) = sync::mpsc::channel(buffer);
        (Sender { tx_frame }, Self { rx_frame })
    }
}

impl<D> http_body::Body for Channel<D>
where
    D: bytes::Buf,
{
    type Data = D;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();

        match this.rx_frame.poll_recv(cx) {
            Poll::Ready(frame @ Some(_)) => return Poll::Ready(frame.map(Ok)),
            Poll::Ready(None) | Poll::Pending => {}
        }

        Poll::Pending
    }
}

/// A sender half created through [`Channel::new`].
pub struct Sender<D> {
    tx_frame: sync::mpsc::Sender<Frame<D>>,
}

impl<D> Sender<D> {
    /// Send data on data channel.
    #[inline]
    pub async fn send_data(&mut self, buf: D) -> Result<(), SendError> {
        self.tx_frame
            .send(Frame::data(buf))
            .await
            .map_err(|_| SendError)
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.tx_frame.is_closed()
    }
}

pub struct SendError;

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to send frame")
    }
}
