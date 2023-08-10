//! Provides [Tripwire], which completes when the program is requested to shutdown.
//! It allows for graceful shutdown of various asynchronous tasks.

#![warn(missing_docs)]
#![deny(clippy::await_holding_lock)]

mod preempt;
mod signalstream;
mod tripwire;

pub use crate::tripwire::{Tripwire, TripwireWorker};
pub use preempt::{Outcome, PreemptibleFuture, PreemptibleFutureExt, TimeoutFutureExt};
