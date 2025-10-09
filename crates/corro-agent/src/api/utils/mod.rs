use bytes::Bytes;
use corro_types::gauge::PersistentGauge;
use http_body::Frame;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pin_project! {
    pub struct CountedBody {
        #[pin]
        rx_frame: mpsc::Receiver<Frame<Bytes>>,
        gauge: PersistentGauge,
    }

    impl PinnedDrop for CountedBody {
        fn drop(this: Pin<&mut Self>) {
            this.gauge.decrement(1.0);
        }
    }
}

impl CountedBody {
    // Channel bodies need to be counted as they can be long lived
    pub fn channel(gauge: PersistentGauge) -> (BodySender, Self) {
        let (tx_frame, rx_frame) = mpsc::channel(16);
        gauge.increment(1.0);
        (BodySender { tx_frame }, Self { rx_frame, gauge })
    }
}

impl http_body::Body for CountedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();

        match this.rx_frame.poll_recv(cx) {
            Poll::Ready(frame @ Some(_)) => return Poll::Ready(frame.map(Ok)),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A sender half created through [`Channel::new`].
pub struct BodySender {
    tx_frame: mpsc::Sender<Frame<Bytes>>,
}

impl BodySender {
    /// Send data on data channel.
    #[inline]
    pub async fn send_data(&mut self, buf: Bytes) -> Result<(), BodySendError> {
        self.tx_frame
            .send(Frame::data(buf))
            .await
            .map_err(|_| BodySendError)
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.tx_frame.is_closed()
    }
}

pub struct BodySendError;

impl std::fmt::Display for BodySendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to send frame")
    }
}
