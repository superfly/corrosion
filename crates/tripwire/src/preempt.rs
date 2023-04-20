use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Sleep;

/// Whether a preemptible future was preempted or completed
#[derive(Debug)]
pub enum Outcome<P, C> {
    /// The future was preempted by another future
    Preempted(P),
    /// The future completed uninterrupted
    Completed(C),
}

impl<P, C> Outcome<P, C> {
    /// True if the variant is Preempted
    pub fn is_preempted(&self) -> bool {
        matches!(self, Outcome::Preempted(_))
    }

    /// True if the variant is Completed
    pub fn is_completed(&self) -> bool {
        matches!(self, Outcome::Completed(_))
    }
}

pin_project! {
    /// A future that can be preempted by another one
    pub struct PreemptibleFuture<P, C> {
        #[pin]
        preempt: P,

        #[pin]
        complete: C,
    }
}

impl<P, C> Future for PreemptibleFuture<P, C>
where
    P: Future,
    C: Future,
{
    type Output = Outcome<P::Output, C::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(v) = this.preempt.poll(cx) {
            return Poll::Ready(Outcome::Preempted(v));
        }

        this.complete.poll(cx).map(Outcome::Completed)
    }
}

impl<P, C, T, E> PreemptibleFuture<P, C>
where
    P: Future,
    C: Future<Output = Result<T, E>>,
{
    /// Map a "preempted" outcome to an error via a closure
    pub async fn complete_or_else(self, f: impl FnOnce(P::Output) -> E) -> Result<T, E> {
        match self.await {
            Outcome::Preempted(v) => Err(f(v)),
            Outcome::Completed(res) => res,
        }
    }

    /// Map a "preempted" outcome to an error
    pub async fn complete_or(self, err: E) -> Result<T, E> {
        match self.await {
            Outcome::Preempted(_) => Err(err),
            Outcome::Completed(res) => res,
        }
    }
}

/// Extension trait to allow wrapping futures such that they can be preempted
pub trait PreemptibleFutureExt<P>: Sized {
    /// Wrap a future such that it can be preempted by another one
    fn preemptible(self, done: P) -> PreemptibleFuture<P, Self>;
}

/// Extension trait to allow wrapping futures such that they can time out
/// Kinda like `tokio::time::timeout` but postfix
pub trait TimeoutFutureExt: Sized {
    /// Wrap a future so that it can be preempted by a timeout
    fn with_timeout(self, delay: Duration) -> PreemptibleFuture<Sleep, Self>;
}

impl<P, C> PreemptibleFutureExt<P> for C
where
    P: Future,
    C: Future,
{
    fn preemptible(self, preempt: P) -> PreemptibleFuture<P, Self> {
        PreemptibleFuture { preempt, complete: self }
    }
}

impl<C> TimeoutFutureExt for C
where
    C: Future,
{
    fn with_timeout(self, delay: Duration) -> PreemptibleFuture<Sleep, Self> {
        self.preemptible(tokio::time::sleep(delay))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use crate::{Outcome, PreemptibleFutureExt, Tripwire};
    use assert2::let_assert;

    #[tokio::test]
    async fn preemptible_tripwire() {
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        {
            let signal: Arc<AtomicBool> = Default::default();
            let f = {
                let signal = signal.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    signal.store(true, Ordering::SeqCst);
                }
                .preemptible(tripwire.clone())
            };
            let_assert!(Outcome::Completed(_) = f.await);
            assert!(signal.load(Ordering::SeqCst))
        }

        {
            let signal: Arc<AtomicBool> = Default::default();
            let f = {
                let signal = signal.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    signal.store(true, Ordering::SeqCst);
                }
                .preemptible(tripwire.clone())
            };
            tripwire_tx.send(()).await.ok();
            tripwire_worker.await;
            let_assert!(Outcome::Preempted(()) = f.await);
            assert!(!signal.load(Ordering::SeqCst))
        }
    }
}
