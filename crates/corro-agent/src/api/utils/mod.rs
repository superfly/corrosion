use corro_types::gauge::PersistentGauge;
use hyper::body::{Body, HttpBody, Sender, SizeHint};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    pub struct CountedBody<B: HttpBody> {
        #[pin]
        body: B,
        gauge: Option<PersistentGauge>,
    }

    impl<B: HttpBody> PinnedDrop for CountedBody<B> {
        fn drop(this: Pin<&mut Self>) {
            if let Some(gauge) = &this.gauge {
                gauge.decrement(1.0);
            }
        }
    }
}

impl<B: HttpBody> CountedBody<B> {
    fn new(body: B, gauge: Option<PersistentGauge>) -> Self {
        if let Some(gauge) = &gauge {
            gauge.increment(1.0);
        }
        Self { body, gauge }
    }
}

impl CountedBody<Body> {
    // Channel bodies need to be counted as they can be long lived
    pub fn channel(gauge: PersistentGauge) -> (Sender, Self) {
        let (tx, body) = hyper::Body::channel();
        (tx, Self::new(body, Some(gauge)))
    }
}

impl<B: HttpBody> HttpBody for CountedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let this = self.project();
        this.body.poll_data(cx)
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<hyper::HeaderMap>, Self::Error>> {
        let this = self.project();
        this.body.poll_trailers(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

// If the underlying body can be constructed from some simple type
// then we can implement From<T> for CountedBody<B>
// No need to track their count as they are short lived
pub trait SimpleBody {}
impl SimpleBody for Vec<u8> {}
impl SimpleBody for &'static [u8] {}
impl SimpleBody for String {}
impl SimpleBody for &'static str {}

impl<B, T> From<T> for CountedBody<B>
where
    B: HttpBody + From<T>,
    T: SimpleBody,
{
    fn from(value: T) -> Self {
        Self::new(value.into(), None)
    }
}
