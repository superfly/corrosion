use corro_tests::{launch_test_agent, TestAgent};
use corro_types::api::Statement;
use corro_types::{
    actor::ActorId,
    api::{ExecResponse, ExecResult},
    sync::generate_sync,
};
use futures::{StreamExt, TryStreamExt};
use hyper::StatusCode;
use rand::{prelude::*, rngs::StdRng, SeedableRng};
use serde_json::json;
use spawn::wait_for_all_pending_handles;
use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info_span};
use tripwire::Tripwire;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let a = launch_test_agent(
        |conf| {
            conf.gossip_addr(gossip_addr)
                .bootstrap(
                    bootstrap
                        .iter()
                        .map(SocketAddr::to_string)
                        .collect::<Vec<String>>(),
                )
                .build()
        },
        tripwire.clone(),
    )
    .await
    .unwrap();

    let b = launch_test_agent(
        |conf| {
            conf.gossip_addr(gossip_addr)
                .bootstrap(
                    bootstrap
                        .iter()
                        .map(SocketAddr::to_string)
                        .collect::<Vec<String>>(),
                )
                .build()
        },
        tripwire.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;


    Ok(())
}
