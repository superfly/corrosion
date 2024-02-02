//! Agent module
//!
//! The agent runs input and output streams (with other agents and
//! clients), manages cluster memberships, and applies propagated
//! changesets to local data.

mod bi;
mod bootstrap;
mod error;
mod handlers;
mod metrics;
mod run_root;
mod setup;
mod uni;
pub(crate) mod util;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use corro_types::api::QueryEventMeta;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{broadcast::Sender, RwLock};
use uuid::Uuid;

// Public exports
pub use error::{SyncClientError, SyncRecvError};
pub use run_root::start_with_config;
pub use setup::{setup, AgentOptions};

pub const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(300);
pub const COMPACT_BOOKED_INTERVAL: Duration = Duration::from_secs(300);
pub const MAX_SYNC_BACKOFF: Duration = Duration::from_secs(15); // 1 minute oughta be enough, we're constantly
                                                                // getting broadcasts randomly + targetted
pub const RANDOM_NODES_CHOICES: usize = 10;

pub const CHECK_EMPTIES_TO_INSERT_AFTER: Duration = Duration::from_secs(120);
pub const TO_CLEAR_COUNT: usize = 1000;

pub type BcastCache = Arc<RwLock<HashMap<Uuid, Sender<(Bytes, QueryEventMeta)>>>>;

#[derive(Clone)]
pub struct CountedExecutor;

impl<F> hyper::rt::Executor<F> for CountedExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, fut: F) {
        spawn::spawn_counted(fut);
    }
}
