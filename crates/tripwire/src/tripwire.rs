use futures::stream::Stream;
use futures_util::stream::{select, Select};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
#[cfg(windows)]
use tokio::signal::windows::ctrl_c;
use tokio::sync::{mpsc, watch};
#[cfg(windows)]
use tokio_stream::wrappers::CtrlCStream;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tracing::{debug, warn};

#[cfg(unix)]
use crate::signalstream::SignalStream;

/// A `Future` that completes once the program is requested to shutdown. This
/// gives a chance for other `Futures` to do additional cleanup before being
/// `drop`ped.
#[derive(Debug)]
pub struct Tripwire {
    subscription: WatchStream<TripwireState>,
    subscription_rx: watch::Receiver<TripwireState>,
}

impl Tripwire {
    /// Create a new [Tripwire]. The returned worker will trip the tripwire when
    /// dropped, and must be spawned on a runtime to listen for SIGTERM/SIGINT
    pub fn new<S>(stream: S) -> (Self, TripwireWorker<S>)
    where
        S: Stream<Item = ()>,
    {
        let (sender, receiver) = watch::channel(TripwireState::Running);
        let tripwire = Tripwire {
            subscription: WatchStream::new(receiver.clone()),
            subscription_rx: receiver,
        };
        let worker = TripwireWorker {
            subscription: sender,
            stream,
        };
        (tripwire, worker)
    }

    /// Create a new [Tripwire] listens for a `()` being sent to the returned sender.
    /// Graceful shutdown can then be awaited through the [TripwireWorker]
    pub fn new_simple() -> (Self, TripwireWorker<ReceiverStream<()>>, mpsc::Sender<()>) {
        let (tx, rx) = mpsc::channel::<()>(1);
        let (tw, w) = Self::new(ReceiverStream::new(rx));
        (tw, w, tx)
    }

    /// Listen for SIGTERM and SIGINT
    #[cfg(unix)]
    pub fn new_signals() -> (Self, TripwireWorker<Select<SignalStream, SignalStream>>) {
        // For non-Windows platforms, create signal streams for SIGTERM and SIGINT
        let sigterms = SignalStream::new(signal(SignalKind::terminate()).unwrap());
        let sigints = SignalStream::new(signal(SignalKind::interrupt()).unwrap());
        Self::new(select(sigterms, sigints))
    }

    #[cfg(windows)]
    pub fn new_signals() -> (Self, TripwireWorker<Select<CtrlCStream, CtrlCStream>>) {
        // For Windows platforms, create two Ctrl-C signal streams to meet the requirement of `select`
        let ctrl_c1 = CtrlCStream::new(ctrl_c().unwrap());
        let ctrl_c2 = CtrlCStream::new(ctrl_c().unwrap());
        Self::new(select(ctrl_c1, ctrl_c2))
    }

    /// Returns an Arc of the current [TripwireState]
    pub fn state(&self) -> TripwireState {
        *self.subscription_rx.borrow()
    }

    /// Returns true if we're shutting down
    pub fn is_shutting_down(&self) -> bool {
        matches!(self.state(), TripwireState::ShuttingDown)
    }

    /// Returns a [Future] that completes when this wire is tripped
    pub fn tripwired<F>(self, inner: F) -> Tripwired<F> {
        Tripwired {
            tripwire: self,
            inner,
        }
    }
}

impl Clone for Tripwire {
    fn clone(&self) -> Self {
        Tripwire {
            subscription: WatchStream::new(self.subscription_rx.clone()),
            subscription_rx: self.subscription_rx.clone(),
        }
    }
}

impl Future for Tripwire {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match futures::ready!(Pin::new(&mut self.subscription).poll_next(cx)) {
                Some(TripwireState::Running) => {}
                Some(TripwireState::ShuttingDown) => return Poll::Ready(()),
                None => return Poll::Ready(()),
            }
        }
    }
}

/// A [Future] that completes when the program is requested to shutdown.
pub struct Tripwired<F> {
    tripwire: Tripwire,
    inner: F,
}

impl<F> Future for Tripwired<F>
where
    F: Future<Output = ()> + Unpin,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Poll::Ready(()) = Pin::new(&mut self.tripwire).poll(cx) {
            return Poll::Ready(());
        }

        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<S> Stream for Tripwired<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(()) = Pin::new(&mut self.tripwire).poll(cx) {
            return Poll::Ready(None);
        }

        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Whether this trip wire has been activated
#[derive(Copy, Clone, Debug)]
#[allow(missing_docs)]
pub enum TripwireState {
    Running,
    ShuttingDown,
}

/// Trips the [Tripwire] when receiving anything from a stream
/// (used for signals), or when dropping.
pub struct TripwireWorker<S> {
    subscription: watch::Sender<TripwireState>,
    stream: S,
}

impl<S> Future for TripwireWorker<S>
where
    S: Stream + Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match futures::ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            Some(_) => "either SIGTERM or SIGINT",
            None => return Poll::Ready(()),
        };

        println!(); // cleaner logs! (new line after ^C)
        debug!("TripwireWorker tripped");

        if let Err(error) = self.subscription.send(TripwireState::ShuttingDown) {
            warn!("all subscription handles have been cancelled?: {:?}", error);
        }
        debug!("tripwire worker's subscription has been updated");
        Poll::Ready(())
    }
}
