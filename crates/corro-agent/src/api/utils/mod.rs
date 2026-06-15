use bytes::Bytes;
use corro_types::channel::{bounded, CorroReceiver, CorroSender};
use corro_types::gauge::PersistentGauge;
use http_body::Frame;
use metrics::Counter;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    pub struct CountedBody {
        rx_frame: CorroReceiver<Frame<Bytes>>,
        gauge: PersistentGauge,
        poll_count: Counter,
    }

    impl PinnedDrop for CountedBody {
        fn drop(this: Pin<&mut Self>) {
            this.gauge.decrement(1.0);
        }
    }
}

impl CountedBody {
    // Channel bodies need to be counted as they can be long lived.
    //
    // `channel_name` is used as the `channel_name` label for the underlying
    // `CorroSender`/`CorroReceiver` metrics (send/recv counts, capacity, send
    // delay) as well as the `corro.api.body.poll_frame` counter. It accepts an
    // owned string so callers can pass a per-subscription identifier (e.g. the
    // query hash).
    pub fn channel(
        gauge: PersistentGauge,
        channel_name: impl Into<metrics::SharedString>,
    ) -> (BodySender, Self) {
        let channel_name = channel_name.into();
        let (tx_frame, rx_frame) = bounded(16, channel_name.clone());
        gauge.increment(1.0);
        let poll_count =
            metrics::counter!("corro.api.body.poll_frame", "channel_name" => channel_name);
        (
            BodySender { tx_frame },
            Self {
                rx_frame,
                gauge,
                poll_count,
            },
        )
    }
}

impl http_body::Body for CountedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        this.poll_count.increment(1);

        match this.rx_frame.poll_recv(cx) {
            Poll::Ready(frame @ Some(_)) => Poll::Ready(frame.map(Ok)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A sender half created through [`Channel::new`].
pub struct BodySender {
    tx_frame: CorroSender<Frame<Bytes>>,
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
