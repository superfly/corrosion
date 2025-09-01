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

#[cfg(all(unix, debug_assertions))]
static BACKTRACES: parking_lot::Mutex<Vec<Option<std::backtrace::Backtrace>>> =
    parking_lot::Mutex::new(Vec::new());

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
        index: usize,
    }

    impl<F> PinnedDrop for CountedFut<F> {
        fn drop(this: Pin<&mut Self>) {
            let count = this.pendings.fetch_sub(1, Ordering::SeqCst);
            trace!("dropping counted future, count: {}", count - 1);

            #[cfg(all(unix, debug_assertions))]
            {
                let mut lock = BACKTRACES.lock();
                lock[this.index] = None;
            }
        }
    }
}

impl<F> CountedFut<F> {
    /// Create a new [CountedFut], immediately incrementing `pendings`
    pub fn new(fut: F, pendings: &'static AtomicUsize) -> Self {
        pendings.fetch_add(1, Ordering::SeqCst);

        #[cfg(all(unix, debug_assertions))]
        let index = {
            let mut lock = BACKTRACES.lock();
            let i = lock.len();
            lock.push(Some(std::backtrace::Backtrace::capture()));
            i
        };

        #[cfg(not(all(unix, debug_assertions)))]
        let index = 0;

        Self {
            fut,
            pendings,
            index,
        }
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
    #[cfg(all(unix, debug_assertions))]
    {
        let signals = signal_hook_tokio::Signals::new([signal_hook::consts::SIGTERM])
            .expect("unable to create signals");
        let handle = signals.handle();

        let mut sig = tokio::spawn(handle_sigterm(signals));
        let mut wait = tokio::spawn(wait_handles());

        tokio::select! {
            _ = &mut sig => {
                eprintln!("received SIGTERM");
            }
            _ = &mut wait => {
                handle.close();
                let _ = sig.await;
                return;
            }
        }

        let _ = wait.await;
    }

    #[cfg(not(all(unix, debug_assertions)))]
    {
        wait_handles().await;
    }
}

async fn wait_handles() {
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

/// cargo nextest sends a `SIGTERM` when a test times out, which gives us a
/// chance to print the backtraces of the counted futures that are still running
#[cfg(all(unix, debug_assertions))]
async fn handle_sigterm(mut signals: signal_hook_tokio::Signals) {
    use futures::StreamExt;
    while let Some(signal) = signals.next().await {
        if signal != signal_hook::consts::SIGTERM {
            continue;
        }

        let lock = BACKTRACES.lock();

        eprintln!("active counted futures:");
        for (i, bt) in lock.iter().enumerate() {
            if let Some(bt) = bt {
                eprintln!("#{i}: {bt}");
            }
        }
    }
}
