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

    let gossip_a = "127.0.0.1:9999".parse().unwrap();
    let gossip_b = "127.0.0.1:9090".parse().unwrap();

    let a = launch_test_agent(|conf| conf.gossip_addr(gossip_a).build(), tripwire.clone())
        .await
        .unwrap();

    let b = launch_test_agent(
        |conf| {
            conf.gossip_addr(gossip_b)
                .bootstrap(vec!["127.0.0.1:9999".into()])
                .build()
        },
        tripwire.clone(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client: hyper::Client<_, hyper::Body> = hyper::Client::builder().build_http();

    let test_statement = serde_json::from_value::<Vec<Statement>>(json!([[
        "INSERT INTO tests (id,text) VALUES (?,?)",
        [0, format!("hello chill world :)")]
    ],]))
    .unwrap();

    let res = client
        .request(
            hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri(format!("http://{}/v1/transactions", a.agent.api_addr()))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&test_statement)?.into())?,
        )
        .await?;

    if res.status() != StatusCode::OK {
        eyre::bail!("unexpected status code: {}", res.status());
    }

    let start = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let conn = b.agent.pool().read().await?;
        let counts: HashMap<ActorId, i64> = conn
            .prepare_cached("SELECT site_id, count(*) FROM crsql_changes GROUP BY site_id;")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;

        let count = counts.values().sum::<i64>();

        let sync = generate_sync(&b.bookie, b.agent.actor_id()).await;
        let needed = sync.need_len();

        if count == 1 && needed == 0 {
            break;
        } else {
            println!("Waiting for data");
        }
    }

    println!("fully disseminated in {}s", start.elapsed().as_secs_f32());

    let body: ExecResponse =
        serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

    if !matches!(
        body.results[0],
        ExecResult::Execute {
            rows_affected: 1,
            ..
        }
    ) {
        eyre::bail!("unexpected exec result for statement: {test_statement:?}");
    }

    Ok(())
}
