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

    let agents =
        futures::stream::iter((0..10).map(|n| "127.0.0.1:0".parse().map(move |addr| (n, addr))))
            .try_chunks(50)
            .try_fold(vec![], {
                let tripwire = tripwire.clone();
                move |mut agents: Vec<TestAgent>, to_launch| {
                    let tripwire = tripwire.clone();
                    async move {
                        for (n, gossip_addr) in to_launch {
                            println!("LAUNCHING AGENT #{n}");
                            let mut rng = StdRng::from_entropy();
                            let bootstrap = agents
                                .iter()
                                .map(|ta| ta.agent.gossip_addr())
                                .choose_multiple(&mut rng, 10);
                            agents.push(
                                launch_test_agent(
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
                                .unwrap(),
                            );
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Ok(agents)
                    }
                }
            })
            .await?;

    let client: hyper::Client<_, hyper::Body> = hyper::Client::builder().build_http();

    let addrs: Vec<SocketAddr> = agents.iter().map(|ta| ta.agent.api_addr()).collect();

    let count = 200;

    let iter = (0..count).flat_map(|n| {
        serde_json::from_value::<Vec<Statement>>(json!([
            [
                "INSERT INTO tests (id,text) VALUES (?,?)",
                [n, format!("hello world {n}")]
            ],
            [
                "INSERT INTO tests2 (id,text) VALUES (?,?)",
                [n, format!("hello world {n}")]
            ],
            [
                "INSERT INTO tests (id,text) VALUES (?,?)",
                [n + 10000, format!("hello world {n}")]
            ],
            [
                "INSERT INTO tests2 (id,text) VALUES (?,?)",
                [n + 10000, format!("hello world {n}")]
            ]
        ]))
        .unwrap()
    });

    tokio::spawn(async move {
        tokio_stream::StreamExt::map(futures::stream::iter(iter).chunks(20), {
            let addrs = addrs.clone();
            let client = client.clone();
            move |statements| {
                let addrs = addrs.clone();
                let client = client.clone();
                Ok(async move {
                    let mut rng = StdRng::from_entropy();
                    let chosen = addrs.iter().choose(&mut rng).unwrap();

                    let res = client
                        .request(
                            hyper::Request::builder()
                                .method(hyper::Method::POST)
                                .uri(format!("http://{chosen}/v1/transactions"))
                                .header(hyper::header::CONTENT_TYPE, "application/json")
                                .body(serde_json::to_vec(&statements)?.into())?,
                        )
                        .await?;

                    if res.status() != StatusCode::OK {
                        eyre::bail!("unexpected status code: {}", res.status());
                    }

                    let body: ExecResponse =
                        serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

                    for (i, statement) in statements.iter().enumerate() {
                        if !matches!(
                            body.results[i],
                            ExecResult::Execute {
                                rows_affected: 1,
                                ..
                            }
                        ) {
                            eyre::bail!("unexpected exec result for statement {i}: {statement:?}");
                        }
                    }

                    Ok::<_, eyre::Report>(())
                })
            }
        })
        .try_buffer_unordered(10)
        .try_collect::<Vec<()>>()
        .await?;
        Ok::<_, eyre::Report>(())
    });

    let changes_count = 4 * count;

    println!("expecting {changes_count} ops");

    let start = Instant::now();

    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        println!("checking status after {}s", start.elapsed().as_secs_f32());
        let mut v = vec![];
        for ta in agents.iter() {
            let span = info_span!("consistency", actor_id = %ta.agent.actor_id().0);
            let _entered = span.enter();

            let conn = ta.agent.pool().read().await?;
            let counts: HashMap<ActorId, i64> = conn
                .prepare_cached("SELECT site_id, count(*) FROM crsql_changes GROUP BY site_id;")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;

            debug!("versions count: {counts:?}");

            let actual_count: i64 =
                conn.query_row("SELECT count(*) FROM crsql_changes;", (), |row| row.get(0))?;
            debug!("actual count: {actual_count}");

            debug!(
                "last version: {:?}",
                ta.bookie
                    .write("test")
                    .await
                    .ensure(ta.agent.actor_id())
                    .read("test")
                    .await
                    .last()
            );

            let sync = generate_sync(&ta.bookie, ta.agent.actor_id()).await;
            let needed = sync.need_len();

            debug!("generated sync: {sync:?}");

            v.push((counts.values().sum::<i64>(), needed));
        }
        if v.len() != agents.len() {
            println!("got {} actors, expecting {}", v.len(), agents.len());
        }
        if v.len() == agents.len()
            && v.iter()
                .all(|(n, needed)| *n == changes_count && *needed == 0)
        {
            break;
        }

        if start.elapsed() > Duration::from_secs(30) {
            let conn = agents[0].agent.pool().read().await?;
            let mut prepped = conn.prepare("SELECT * FROM crsql_changes;")?;
            let mut rows = prepped.query(())?;

            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }

            panic!(
                "failed to disseminate all updates to all nodes in {}s",
                start.elapsed().as_secs_f32()
            );
        }
    }
    println!("fully disseminated in {}s", start.elapsed().as_secs_f32());

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}
