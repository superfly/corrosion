//! Keep track of how many futures of a certain kind are running (in an [AtomicUsize])

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Future;
use pin_project_lite::pin_project;
use tracing::{info, trace};

/// Global counter for [spawn_counted] and [spawn_counted_w_handle]
pub static PENDING_HANDLES: AtomicUsize = AtomicUsize::new(0);

/// Spawn `fut` as a [CountedFut] (increments/decrements an [AtomicUsize])
#[track_caller]
pub fn spawn_counted<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    tokio::spawn(CountedFut::new(fut, &PENDING_HANDLES))
}

/// Spawn `fut` as a [CountedFut] (increments/decrements an [AtomicUsize])
/// on the given [tokio::runtime::Handle].
#[track_caller]
pub fn spawn_counted_w_handle<F>(
    fut: F,
    h: &tokio::runtime::Handle,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    h.spawn(CountedFut::new(fut, &PENDING_HANDLES))
}

/// Spawn blocking `fut` as a [CountedFut] (increments/decrements an [AtomicUsize])
/// on the given [tokio::runtime::Handle].
#[track_caller]
pub fn spawn_blocking_counted_w_handle<F, R>(
    func: F,
    h: &tokio::runtime::Handle,
) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    h.spawn_blocking(move || {
        let _guard = CountedGuard::new(&PENDING_HANDLES);
        func()
    })
}

pub struct CountedGuard {
    pendings: &'static AtomicUsize,
}

impl CountedGuard {
    pub fn new(pendings: &'static AtomicUsize) -> Self {
        pendings.fetch_add(1, Ordering::SeqCst);
        Self { pendings }
    }
}

impl Drop for CountedGuard {
    fn drop(&mut self) {
        let count = self.pendings.fetch_sub(1, Ordering::SeqCst);
        trace!("dropping counted guard, count: {}", count - 1);
    }
}

pin_project! {
    /// An [AtomicUsize] is incremented when building this future, and decremented
    /// again when the future completes.
    #[project = CountedProj]
    pub struct CountedFut<F> {
        #[pin]
        fut: F,
        pendings: &'static AtomicUsize,
    }

    impl<F> PinnedDrop for CountedFut<F> {
        fn drop(this: Pin<&mut Self>) {
            let count = this.pendings.fetch_sub(1, Ordering::SeqCst);
            trace!("dropping counted future, count: {}", count - 1);
        }
    }
}

impl<F> CountedFut<F> {
    /// Create a new [CountedFut], immediately incrementing `pendings`
    pub fn new(fut: F, pendings: &'static AtomicUsize) -> Self {
        pendings.fetch_add(1, Ordering::SeqCst);
        Self { fut, pendings }
    }
}

impl<F> Future for CountedFut<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        trace!("polling counted future");
        let this = self.project();
        this.fut.poll(cx)
    }
}

/// Waits for [PENDING_HANDLES] to reach zero. All counted futures must be
/// [Tripwire]-aware, and the tripwire must've been tripped, otherwise this
/// is just going to sleep for 600 seconds.
pub async fn wait_for_all_pending_handles() {
    let mut rounds = 0;

    for _ in 0..600 {
        match PENDING_HANDLES.load(Ordering::SeqCst) {
            0 => {
                info!("All spawned futures done!");
                break;
            }
            n => {
                rounds += 1;
                if rounds % 10 == 0 {
                    info!("Waiting on {n} spawned futures before exiting")
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
