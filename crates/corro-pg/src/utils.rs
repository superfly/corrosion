use std::pin::Pin;
use std::task::{Context, Poll};

use corro_types::gauge::PersistentGauge;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pin_project! {
    pub struct CountedTcpStream {
        #[pin]
        pub stream: TcpStream,
        gauge: PersistentGauge,
    }

    impl PinnedDrop for CountedTcpStream {
        fn drop(this: Pin<&mut Self>) {
            this.gauge.decrement(1.0);
        }
    }
}

impl CountedTcpStream {
    pub fn wrap(stream: TcpStream, gauge: PersistentGauge) -> Self {
        gauge.increment(1.0);
        Self { stream, gauge }
    }
}

impl AsyncRead for CountedTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl AsyncWrite for CountedTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_shutdown(cx)
    }
}
