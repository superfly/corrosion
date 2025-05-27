use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    ops::{Deref, RangeInclusive},
    time::{Duration, Instant},
};

use axum::Extension;
use futures::{future, stream::FuturesUnordered, StreamExt, TryStreamExt};
use hyper::StatusCode;
use rand::{
    distributions::Uniform, prelude::Distribution, rngs::StdRng, seq::IteratorRandom, SeedableRng,
};
use rangemap::RangeInclusiveSet;
use serde::Deserialize;
use serde_json::json;
use spawn::wait_for_all_pending_handles;
use tokio::{
    sync::mpsc,
    time::{sleep, timeout, MissedTickBehavior},
};
use tracing::{debug, info_span};
use tripwire::Tripwire;
use uuid::Uuid;

use crate::{
    agent::process_multiple_changes,
    api::{
        peer::parallel_sync,
        public::{api_v1_db_schema, api_v1_transactions, TimeoutParams},
    },
    transport::Transport,
};
use corro_tests::*;
use corro_types::change::Change;
use corro_types::{
    actor::ActorId,
    agent::migrate,
    api::{row_to_change, ExecResponse, ExecResult, Statement},
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{ChangeSource, ChangeV1, Changeset},
    sync::generate_sync,
};
use corro_types::{
    agent::Agent,
    api::{ColumnName, TableName},
    change::row_to_change,
    pubsub::pack_columns,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]

async fn insert_rows_and_gossip() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta2 = launch_test_agent(
        |conf| {
            conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                .build()
        },
        tripwire.clone(),
    )
    .await?;

    let client = hyper::Client::builder()
        .pool_max_idle_per_host(5)
        .pool_idle_timeout(Duration::from_secs(300))
        .build_http::<hyper::Body>();

    let req_body: Vec<Statement> = serde_json::from_value(json!([[
        "INSERT INTO tests (id,text) VALUES (?,?)",
        [1, "hello world 1"]
    ],]))?;

    let res = timeout(
        Duration::from_secs(5),
        client.request(
            hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&req_body)?.into())?,
        ),
    )
    .await??;

    let body: ExecResponse =
        serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

    let db_version: CrsqlDbVersion =
        ta1.agent
            .pool()
            .read()
            .await?
            .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
    assert_eq!(db_version, CrsqlDbVersion(1));

    println!("body: {body:?}");

    let svc: TestRecord = ta1.agent.pool().read().await?.query_row(
        "SELECT id, text FROM tests WHERE id = 1;",
        [],
        |row| {
            Ok(TestRecord {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        },
    )?;

    assert_eq!(svc.id, 1);
    assert_eq!(svc.text, "hello world 1");

    sleep(Duration::from_secs(1)).await;

    let svc: TestRecord = ta2.agent.pool().read().await?.query_row(
        "SELECT id, text FROM tests WHERE id = 1;",
        [],
        |row| {
            Ok(TestRecord {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        },
    )?;

    assert_eq!(svc.id, 1);
    assert_eq!(svc.text, "hello world 1");

    let req_body: Vec<Statement> = serde_json::from_value(json!([[
        "INSERT INTO tests (id,text) VALUES (?,?)",
        [2, "hello world 2"]
    ]]))?;

    let res = client
        .request(
            hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&req_body)?.into())?,
        )
        .await?;

    let body: ExecResponse =
        serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

    println!("body: {body:?}");

    println!("checking crsql_changes");

    #[allow(clippy::type_complexity)]
    let bk: Vec<(ActorId, CrsqlDbVersion, Option<CrsqlSeq>)> = ta1
        .agent
        .pool()
        .read()
        .await?
        .prepare("SELECT site_id, db_version, max(seq) FROM crsql_changes group by db_version")?
        .query_map((), |row| {
            Ok((
                row.get::<_, ActorId>(0)?,
                row.get::<_, CrsqlDbVersion>(1)?,
                row.get::<_, Option<CrsqlSeq>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;

    assert_eq!(
        bk,
        vec![
            (ta1.agent.actor_id(), CrsqlDbVersion(1), Some(CrsqlSeq(0))),
            (ta1.agent.actor_id(), CrsqlDbVersion(2), Some(CrsqlSeq(0)))
        ]
    );

    let svc: TestRecord = ta1.agent.pool().read().await?.query_row(
        "SELECT id, text FROM tests WHERE id = 2;",
        [],
        |row| {
            Ok(TestRecord {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        },
    )?;

    assert_eq!(svc.id, 2);
    assert_eq!(svc.text, "hello world 2");

    sleep(Duration::from_secs(1)).await;

    let svc: TestRecord = ta2.agent.pool().read().await?.query_row(
        "SELECT id, text FROM tests WHERE id = 2;",
        [],
        |row| {
            Ok(TestRecord {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        },
    )?;

    assert_eq!(svc.id, 2);
    assert_eq!(svc.text, "hello world 2");

    let values: Vec<serde_json::Value> = (3..1000)
        .map(|id| {
            serde_json::json!([
                "INSERT INTO tests (id,text) VALUES (?,?)",
                [id, format!("hello world #{id}")],
            ])
        })
        .collect();

    let req_body: Vec<Statement> = serde_json::from_value(json!(values))?;

    timeout(
        Duration::from_secs(5),
        client.request(
            hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&req_body)?.into())?,
        ),
    )
    .await??;

    let db_version: CrsqlDbVersion =
        ta1.agent
            .pool()
            .read()
            .await?
            .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
    assert_eq!(db_version, CrsqlDbVersion(3));

    let expected_count: i64 =
        ta1.agent
            .pool()
            .read()
            .await?
            .query_row("SELECT COUNT(*) FROM tests", (), |row| row.get(0))?;

    sleep(Duration::from_secs(5)).await;

    let got_count: i64 =
        ta2.agent
            .pool()
            .read()
            .await?
            .query_row("SELECT COUNT(*) FROM tests", (), |row| row.get(0))?;

    assert_eq!(expected_count, got_count);

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn chill_test() -> eyre::Result<()> {
    configurable_stress_test(2, 1, 4).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn stress_test() -> eyre::Result<()> {
    configurable_stress_test(30, 10, 200).await
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn stresser_test() -> eyre::Result<()> {
    configurable_stress_test(45, 15, 1500).await
}

/// Default parameters:
///
/// - num_nodes: 30
/// - connectivity: 10
/// - input_count: 200
///
///
#[allow(unused)]
pub async fn configurable_stress_test(
    num_nodes: usize,
    connectivity: usize,
    input_count: usize,
) -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let agents = futures::stream::iter(
        (0..num_nodes).map(|n| "127.0.0.1:0".parse().map(move |addr| (n, addr))),
    )
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
                        .choose_multiple(&mut rng, connectivity);
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

    let addrs: Vec<(ActorId, SocketAddr)> = agents
        .iter()
        .map(|ta| (ta.agent.actor_id(), ta.agent.api_addr()))
        .collect();

    let iter = (0..input_count).flat_map(|n| {
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

    let actor_versions = {
        let client: hyper::Client<_, hyper::Body> = hyper::Client::builder().build_http();

        tokio_stream::StreamExt::map(futures::stream::iter(iter).chunks(20), {
            let addrs = addrs.clone();
            let client = client.clone();
            move |statements| {
                let addrs = addrs.clone();
                let client = client.clone();
                Ok(async move {
                    let mut rng = StdRng::from_entropy();
                    let (actor_id, chosen) = addrs.iter().choose(&mut rng).unwrap();

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

                    Ok::<_, eyre::Report>((*actor_id, 1))
                })
            }
        })
        .try_buffer_unordered(10)
        .try_fold(BTreeMap::new(), |mut acc, item| {
            {
                *acc.entry(item.0).or_insert(0) += item.1
            }
            future::ready(Ok(acc))
        })
        .await?
    };

    let changes_count: i64 = 4 * input_count as i64;

    println!("expecting {changes_count} ops");

    // tokio::spawn({
    //     let bookies = agents
    //         .iter()
    //         .map(|a| (a.agent.actor_id(), a.bookie.clone()))
    //         .collect::<Vec<_>>();
    //     async move {
    //         loop {
    //             tokio::time::sleep(Duration::from_secs(1)).await;
    //             for (actor_id, bookie) in bookies.iter() {
    //                 let registry = bookie.registry();
    //                 let r = registry.map.read();

    //                 for v in r.values() {
    //                     debug!(%actor_id, "GOT A LOCK {v:?}");
    //                 }
    //             }
    //         }
    //     }
    // });

    let start = Instant::now();

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        debug!("looping");
        for ta in agents.iter() {
            let registry = ta.bookie.registry();
            let r = registry.map.read();

            for v in r.values() {
                println!(
                    "{}: GOT A LOCK: {} has been locked for {:?}",
                    ta.agent.actor_id(),
                    v.label,
                    v.started_at.elapsed()
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
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
                    .write::<&str, _>("test", None)
                    .await
                    .ensure(ta.agent.actor_id())
                    .read::<&str, _>("test", None)
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

        println!("we're not done yet...");

        if start.elapsed() > Duration::from_secs(30) {
            for ta in agents.iter() {
                let conn = ta.agent.pool().read().await?;
                let mut per_actor: BTreeMap<ActorId, RangeInclusiveSet<CrsqlDbVersion>> =
                    BTreeMap::new();
                let mut prepped =
                    conn.prepare("SELECT DISTINCT site_id, db_version FROM crsql_changes;")?;
                let mut rows = prepped.query(())?;

                while let Ok(Some(row)) = rows.next() {
                    per_actor
                        .entry(row.get(0)?)
                        .or_default()
                        .insert(row.get(1)?..=row.get(1)?);
                }

                let actual_count: i64 =
                    conn.query_row("SELECT count(*) FROM crsql_changes;", (), |row| row.get(0))?;
                debug!("actual count: {actual_count}");
                if actual_count != changes_count {
                    println!(
                        "{}: still missing {} rows in crsql_changes",
                        ta.agent.actor_id(),
                        changes_count - actual_count
                    );
                }

                for (actor_id, versions) in per_actor {
                    if let Some(versions_len) = actor_versions.get(&actor_id) {
                        let full_range = CrsqlDbVersion(1)..=CrsqlDbVersion(*versions_len as u64);
                        let gaps = versions.gaps(&full_range);
                        for gap in gaps {
                            println!("{} db gap! {actor_id} => {gap:?}", ta.agent.actor_id());
                        }
                    }
                }

                let sync = generate_sync(&ta.bookie, ta.agent.actor_id()).await;
                for (actor, versions) in sync.need {
                    println!(
                        "{}: in-memory gap: {actor:?} from {versions:?}",
                        ta.agent.actor_id()
                    );
                }

                let recorded_gaps = conn
                    .prepare("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps")?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect::<Result<Vec<(ActorId, CrsqlDbVersion, CrsqlDbVersion)>, _>>()?;

                for (actor_id, start, end) in recorded_gaps {
                    println!(
                        "{} recorded gap: {actor_id} => {start}..={end}",
                        ta.agent.actor_id()
                    );
                }
            }

            panic!(
                "failed to disseminate all updates to all nodes in {}s",
                start.elapsed().as_secs_f32()
            );
        }
    }
    println!("fully disseminated in {}s", start.elapsed().as_secs_f32());

    println!("checking gaps in db...");
    for ta in agents {
        let conn = ta.agent.pool().read().await?;
        let gaps_count: u64 =
            conn.query_row("SELECT count(*) FROM __corro_bookkeeping_gaps", [], |row| {
                row.get(0)
            })?;
        assert_eq!(
            gaps_count,
            0,
            "expected {} to have 0 gaps in DB",
            ta.agent.actor_id()
        );
    }

    println!("waiting for things to shut down");
    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn large_tx_sync() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

    let client = hyper::Client::builder()
        .pool_max_idle_per_host(5)
        .pool_idle_timeout(Duration::from_secs(300))
        .build_http::<hyper::Body>();

    let counts = [
        10000, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500,
        400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700,
        600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900,
        800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100,
        1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300,
        200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100,
    ];

    for n in counts.iter() {
        let req_body: Vec<Statement> = serde_json::from_value(json!([format!("INSERT INTO testsbool (id) WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT {n}  ) SELECT id FROM cte;")]))?;

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let body: ExecResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");
    }

    let expected_count = counts.into_iter().sum::<usize>();

    let db_version: CrsqlDbVersion =
        ta1.agent
            .pool()
            .read()
            .await?
            .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
    assert_eq!(db_version, CrsqlDbVersion(counts.len() as u64));

    println!("expected count: {expected_count}");

    let ta2 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta3 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta4 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

    let (rtt_tx, _rtt_rx) = mpsc::channel(1024);
    let ta2_transport = Transport::new(&ta2.agent.config().gossip, rtt_tx.clone()).await?;
    let ta3_transport = Transport::new(&ta3.agent.config().gossip, rtt_tx.clone()).await?;
    let ta4_transport = Transport::new(&ta4.agent.config().gossip, rtt_tx.clone()).await?;

    println!("starting sync!?");
    for _ in 0..7 {
        let res = parallel_sync(
            &ta2.agent,
            &ta2_transport,
            vec![(ta1.agent.actor_id(), ta1.agent.gossip_addr())],
            generate_sync(&ta2.bookie, ta2.agent.actor_id()).await,
        )
        .await?;

        println!("ta2 synced {res}");

        let res = parallel_sync(
            &ta3.agent,
            &ta3_transport,
            vec![
                (ta1.agent.actor_id(), ta1.agent.gossip_addr()),
                (ta2.agent.actor_id(), ta2.agent.gossip_addr()),
            ],
            generate_sync(&ta3.bookie, ta3.agent.actor_id()).await,
        )
        .await?;

        println!("ta3 synced {res}");

        let res = parallel_sync(
            &ta4.agent,
            &ta4_transport,
            vec![
                (ta3.agent.actor_id(), ta3.agent.gossip_addr()),
                (ta2.agent.actor_id(), ta2.agent.gossip_addr()),
            ],
            generate_sync(&ta4.bookie, ta4.agent.actor_id()).await,
        )
        .await?;

        println!("ta4 synced {res}");

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut ta_counts = vec![];

    for (name, ta) in [("ta2", &ta2), ("ta3", &ta3), ("ta4", &ta4)] {
        let agent = &ta.agent;
        let conn = agent.pool().read().await?;

        let count: u64 = conn
            .prepare_cached("SELECT COUNT(*) FROM testsbool;")?
            .query_row((), |row| row.get(0))?;

        println!(
            "{name}: {:#?}",
            generate_sync(&ta.bookie, agent.actor_id()).await
        );

        println!(
            "{name}: bookie: {:?}",
            ta.bookie
                .read::<&str, _>("test", None)
                .await
                .get(&ta1.agent.actor_id())
                .unwrap()
                .read::<&str, _>("test", None)
                .await
                .deref()
        );

        if count as usize != expected_count {
            let buf_count: Vec<(CrsqlDbVersion, u64)> = conn
                .prepare(
                    "select db_version,count(*) from __corro_buffered_changes group by db_version",
                )?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            println!(
                "{name}: BUFFERED COUNT: {buf_count:?} (actor_id: {})",
                agent.actor_id()
            );

            let ranges = conn
                .prepare("select start_seq, end_seq from __corro_seq_bookkeeping")?
                .query_map([], |row| Ok(row.get::<_, u64>(0)?..=row.get::<_, u64>(1)?))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            println!("{name}: ranges: {ranges:?}");
        }

        ta_counts.push((name, agent.actor_id(), count as usize));
    }

    for (name, actor_id, count) in ta_counts {
        assert_eq!(
            count, expected_count,
            "{name}: actor {actor_id} did not reach {expected_count} rows",
        );
    }

    println!("now waiting for all futures to end");

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct TestRecord {
    id: i64,
    text: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clear_empty_versions() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta2 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

    let tx_timeout = Duration::from_secs(60);

    let (rtt_tx, _rtt_rx) = mpsc::channel(1024);
    let ta2_transport = Transport::new(&ta2.agent.config().gossip, rtt_tx.clone()).await?;
    // setup the schema, for both nodes
    let (status_code, _body) = api_v1_db_schema(
        Extension(ta1.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;

    assert_eq!(status_code, StatusCode::OK);

    let (status_code, _body) = api_v1_db_schema(
        Extension(ta2.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;
    assert_eq!(status_code, StatusCode::OK);

    // make about 50 transactions to ta1
    insert_rows(ta1.agent.clone(), 1, 50).await;
    // send them all
    let rows = get_rows(
        ta1.agent.clone(),
        vec![(CrsqlDbVersion(1)..=CrsqlDbVersion(50), None)],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;

    // overwrite different version ranges
    insert_rows(ta1.agent.clone(), 1, 5).await;
    insert_rows(ta1.agent.clone(), 10, 10).await;
    insert_rows(ta1.agent.clone(), 23, 25).await;
    insert_rows(ta1.agent.clone(), 30, 31).await;

    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (CrsqlDbVersion(51)..=CrsqlDbVersion(55), None),
            (CrsqlDbVersion(56)..=CrsqlDbVersion(56), None),
            (CrsqlDbVersion(57)..=CrsqlDbVersion(59), None),
            (CrsqlDbVersion(60)..=CrsqlDbVersion(60), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![CrsqlDbVersion(1)..=CrsqlDbVersion(50)],
        vec![],
        vec![],
        vec![],
    )
    .await?;

    // // initiate sync with ta1 to get cleared
    let res = parallel_sync(
        &ta2.agent,
        &ta2_transport,
        vec![(ta1.agent.actor_id(), ta1.agent.gossip_addr())],
        generate_sync(&ta2.bookie, ta2.agent.actor_id()).await,
    )
    .await?;

    println!("ta2 synced {res}");

    sleep(Duration::from_secs(2)).await;

    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![],
        vec![],
        vec![
            CrsqlDbVersion(1)..=CrsqlDbVersion(5),
            CrsqlDbVersion(10)..=CrsqlDbVersion(10),
            CrsqlDbVersion(23)..=CrsqlDbVersion(25),
            CrsqlDbVersion(30)..=CrsqlDbVersion(31),
        ],
    )
    .await?;

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn process_failed_changes() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let uuid = Uuid::parse_str("00000000-0000-0000-a716-446655440000")?;
    let actor_id = ActorId(uuid);
    // setup the schema, for both nodes
    let (status_code, _body) = api_v1_db_schema(
        Extension(ta1.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;
    assert_eq!(status_code, StatusCode::OK);

    let ta2 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let (status_code, _body) = api_v1_db_schema(
        Extension(ta2.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;
    assert_eq!(status_code, StatusCode::OK);

    for i in 1..=5_i64 {
        let (status_code, _) = api_v1_transactions(
            Extension(ta2.agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "INSERT OR REPLACE INTO tests (id,text) VALUES (?,?)".into(),
                vec![i.into(), "service-text".into()],
            )]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK);
    }
    let mut good_changes = get_rows(
        ta2.agent.clone(),
        vec![(CrsqlDbVersion(1)..=CrsqlDbVersion(5), None)],
    )
    .await?;

    let change6 = Change {
        table: TableName("tests".into()),
        pk: pack_columns(&vec![6i64.into()])?,
        cid: ColumnName("text".into()),
        val: "six".into(),
        col_version: 1,
        db_version: CrsqlDbVersion(6),
        seq: CrsqlSeq(0),
        site_id: actor_id.to_bytes(),
        cl: 1,
    };

    let bad_change = Change {
        table: TableName("tests".into()),
        pk: pack_columns(&vec![6i64.into()])?,
        cid: ColumnName("nonexistent".into()),
        val: "six".into(),
        col_version: 1,
        db_version: CrsqlDbVersion(6),
        seq: CrsqlSeq(1),
        site_id: actor_id.to_bytes(),
        cl: 1,
    };

    let mut rows = vec![(
        ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(1),
                changes: vec![change6.clone(), bad_change],
                seqs: CrsqlSeq(0)..=CrsqlSeq(1),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        },
        ChangeSource::Sync,
        Instant::now(),
    )];

    rows.append(&mut good_changes);

    let res = process_multiple_changes(
        ta1.agent.clone(),
        ta1.bookie.clone(),
        rows,
        Duration::from_secs(60),
    )
    .await;

    assert!(res.is_ok());

    // verify that correct versions were inserted
    let conn = ta1.agent.pool().read().await?;

    for i in 1..=5_i64 {
        let pk = pack_columns(&[i.into()])?;
        let crsql_dbv = conn
            .prepare_cached(
                r#"SELECT db_version from crsql_changes where "table" = "tests" and pk = ? and site_id = ?"#,
            )?
            .query_row((pk, ta2.agent.actor_id()), |row| row.get::<_, CrsqlDbVersion>(0))?;

        assert_eq!(crsql_dbv, CrsqlDbVersion(i as u64));

        let conn = ta1.agent.pool().read().await?;
        conn.prepare_cached("SELECT text from tests where id = ?")?
            .query_row([i], |row| row.get::<_, String>(0))?;
    }

    let res = conn
        .prepare_cached("SELECT text from tests where id = 6")?
        .query_row([], |row| row.get::<_, String>(0));
    assert!(res.is_err());
    assert_eq!(res, Err(rusqlite::Error::QueryReturnedNoRows));

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_process_multiple_changes() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta2 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let tx_timeout = Duration::from_secs(60);

    // setup the schema, for both nodes
    let (status_code, _body) = api_v1_db_schema(
        Extension(ta1.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;

    assert_eq!(status_code, StatusCode::OK);

    let (status_code, _body) = api_v1_db_schema(
        Extension(ta2.agent.clone()),
        axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
    )
    .await;
    assert_eq!(status_code, StatusCode::OK);

    // make about 50 transactions to ta1
    insert_rows(ta1.agent.clone(), 1, 50).await;

    // sent 1-5
    let rows = get_rows(
        ta1.agent.clone(),
        vec![(CrsqlDbVersion(1)..=CrsqlDbVersion(5), None)],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;
    // check ta2 bookie
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![CrsqlDbVersion(1)..=CrsqlDbVersion(5)],
        vec![],
        vec![],
        vec![],
    )
    .await?;

    // sent: 1-5, 9-10
    let rows = get_rows(
        ta1.agent.clone(),
        vec![(CrsqlDbVersion(9)..=CrsqlDbVersion(10), None)],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows,tx_timeout).await?;
    // check for gap 6-8
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![CrsqlDbVersion(6)..=CrsqlDbVersion(8)],
        vec![],
        vec![],
    )
    .await?;

    // create more gaps plus send partials
    // sent 1-5, 9-10, 15-16*, 20
    // * indicates partial version
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (CrsqlDbVersion(20)..=CrsqlDbVersion(20), None),
            (
                CrsqlDbVersion(15)..=CrsqlDbVersion(16),
                Some(CrsqlSeq(0)..=CrsqlSeq(0)),
            ),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;
    // check for gap 11-14 and 17-19
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![
            CrsqlDbVersion(11)..=CrsqlDbVersion(14),
            CrsqlDbVersion(17)..=CrsqlDbVersion(19),
        ],
        vec![(
            CrsqlDbVersion(15)..=CrsqlDbVersion(16),
            CrsqlSeq(0)..=CrsqlSeq(0),
        )],
        vec![],
    )
    .await?;

    // clear versions 21-25. max version is now 55
    insert_rows(ta1.agent.clone(), 21, 25).await;
    // send non-contiguous cleared versions
    // sent 1-5, 9-10, 15-16*, 20, 21, 25
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (CrsqlDbVersion(21)..=CrsqlDbVersion(21), None),
            (CrsqlDbVersion(25)..=CrsqlDbVersion(25), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;

    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![],
        vec![],
        vec![
            CrsqlDbVersion(21)..=CrsqlDbVersion(21),
            CrsqlDbVersion(25)..=CrsqlDbVersion(25),
        ],
    )
    .await?;

    // send some missing gaps, partials and cleared version
    // sent 1-5, 9-10, 14-18, 20, 23-25
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (CrsqlDbVersion(14)..=CrsqlDbVersion(18), None),
            (
                CrsqlDbVersion(15)..=CrsqlDbVersion(16),
                Some(CrsqlSeq(1)..=CrsqlSeq(3)),
            ),
            (CrsqlDbVersion(23)..=CrsqlDbVersion(24), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![
            CrsqlDbVersion(14)..=CrsqlDbVersion(18),
            CrsqlDbVersion(15)..=CrsqlDbVersion(16),
        ],
        vec![
            CrsqlDbVersion(11)..=CrsqlDbVersion(13),
            CrsqlDbVersion(19)..=CrsqlDbVersion(19),
            CrsqlDbVersion(22)..=CrsqlDbVersion(22),
        ],
        vec![],
        vec![CrsqlDbVersion(23)..=CrsqlDbVersion(25)],
    )
    .await?;

    // sent 1-25
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (CrsqlDbVersion(6)..=CrsqlDbVersion(8), None),
            (CrsqlDbVersion(11)..=CrsqlDbVersion(19), None),
            (CrsqlDbVersion(22)..=CrsqlDbVersion(22), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows, tx_timeout).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![CrsqlDbVersion(1)..=CrsqlDbVersion(20)],
        vec![],
        vec![],
        vec![CrsqlDbVersion(21)..=CrsqlDbVersion(25)],
    )
    .await?;

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}

async fn check_bookie_versions(
    ta: TestAgent,
    actor_id: ActorId,
    complete: Vec<RangeInclusive<CrsqlDbVersion>>,
    gap: Vec<RangeInclusive<CrsqlDbVersion>>,
    partials: Vec<(RangeInclusive<CrsqlDbVersion>, RangeInclusive<CrsqlSeq>)>,
    cleared: Vec<RangeInclusive<CrsqlDbVersion>>,
) -> eyre::Result<()> {
    let conn = ta.agent.pool().read().await?;
    let booked = ta
        .bookie
        .write::<&str, _>("test", None)
        .await
        .ensure(actor_id);
    let bookedv = booked.read::<&str, _>("test", None).await;

    for versions in complete {
        for version in versions.clone() {
            // should not be in gaps
            assert!(!conn.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM __corro_bookkeeping_gaps WHERE actor_id = ? and ? between start and end)")?
                .query_row((actor_id, version), |row| row.get(0))?);
        }
        bookedv.contains_all(versions.clone(), Some(&(CrsqlSeq(0)..=CrsqlSeq(3))));
    }

    for versions in partials {
        for version in versions.0.clone() {
            let bk: Vec<(ActorId, CrsqlDbVersion, CrsqlSeq, CrsqlSeq)> = conn
                .prepare(
                    "SELECT site_id, version, start_seq, end_seq FROM __corro_seq_bookkeeping where version = ?",
                )?
                .query_map([version], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })?.collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(
                bk,
                vec![(
                    actor_id,
                    version,
                    *versions.clone().1.start(),
                    *versions.clone().1.end()
                )]
            );

            // should not be in gaps
            assert!(!conn.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM __corro_bookkeeping_gaps WHERE actor_id = ? and ? BETWEEN start and end)")?
                .query_row((actor_id, version), |row| row.get(0))?);

            let partial = bookedv.get_partial(&version);
            assert_ne!(partial, None);
        }
        bookedv.contains_all(versions.0.clone(), Some(&(CrsqlSeq(0)..=CrsqlSeq(3))));
    }

    for versions in gap {
        for version in versions.clone() {
            let needed = bookedv.needed();
            assert!(
                needed.contains(&version),
                "{version:?} should be in {needed:?}"
            );
        }
        assert!(conn.prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM __corro_bookkeeping_gaps WHERE actor_id = ? and start = ? and end = ?)")?
            .query_row((actor_id, versions.start(), versions.end()), |row| row.get(0))?);
    }

    for versions in cleared {
        for version in versions {
            assert!(!conn.prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM crsql_changes WHERE site_id = ? and db_version = ?)")?
            .query_row((actor_id, version), |row| row.get(0))?, "Version {version} not cleared in crsql_changes table");
        }
    }

    Ok(())
}

async fn get_rows(
    agent: Agent,
    v: Vec<(
        RangeInclusive<CrsqlDbVersion>,
        Option<RangeInclusive<CrsqlSeq>>,
    )>,
) -> eyre::Result<Vec<(ChangeV1, ChangeSource, Instant)>> {
    let mut result = vec![];

    let conn = agent.pool().read().await?;
    for versions in v {
        for version in versions.0 {
            let count: u64 = conn.query_row(
                "SELECT COUNT(*) FROM crsql_changes where db_version = ?",
                [version],
                |row| row.get(0),
            )?;
            let mut last = 4;
            // count will be zero for cleared versions
            if count > 0 {
                last = count - 1;
            }
            let mut query =
                r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl
            FROM crsql_changes where db_version = ?"#
                    .to_string();
            let changes: Vec<Change>;
            let seqs = if let Some(seq) = versions.1.clone() {
                let seq_query = " and seq >= ? and seq <= ?";
                query += seq_query;
                let mut prepped = conn.prepare(&query)?;
                changes = prepped
                    .query_map((version, seq.start(), seq.end()), row_to_change)?
                    .collect::<Result<Vec<_>, _>>()?;
                seq
            } else {
                let mut prepped = conn.prepare(&query)?;
                changes = prepped
                    .query_map([version], row_to_change)?
                    .collect::<Result<Vec<_>, _>>()?;
                CrsqlSeq(0)..=CrsqlSeq(last)
            };

            result.push((
                ChangeV1 {
                    actor_id: agent.actor_id(),
                    changeset: Changeset::Full {
                        version,
                        changes,
                        seqs,
                        last_seq: CrsqlSeq(last),
                        ts: agent.clock().new_timestamp().into(),
                    },
                },
                ChangeSource::Broadcast,
                Instant::now(),
            ))
        }
    }
    Ok(result)
}

async fn insert_rows(agent: Agent, start: i64, n: i64) {
    // check locally that everything is in order
    for i in start..=n {
        let (status_code, _) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "INSERT OR REPLACE INTO tests3 (id,text,text2, num, num2) VALUES (?,?,?,?,?)"
                    .into(),
                vec![
                    i.into(),
                    "service-name".into(),
                    "second text".into(),
                    (i + 20).into(),
                    (i + 100).into(),
                ],
            )]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK);

        // let version = body.0.version.unwrap();
        // assert_eq!(version, CrsqlDbVersion(i as u64));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_small_changes() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let agents = futures::StreamExt::fold(futures::stream::iter(0..10).chunks(50), vec![], {
        let tripwire = tripwire.clone();
        move |mut agents: Vec<TestAgent>, to_launch| {
            let tripwire = tripwire.clone();
            async move {
                for n in to_launch {
                    println!("LAUNCHING AGENT #{n}");
                    let mut rng = StdRng::from_entropy();
                    let bootstrap = agents
                        .iter()
                        .map(|ta| ta.agent.gossip_addr())
                        .choose_multiple(&mut rng, 10);
                    agents.push(
                        launch_test_agent(
                            |conf| {
                                conf.gossip_addr("127.0.0.1:0".parse().unwrap())
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
                agents
            }
        }
    })
    .await;

    let mut start_id = 0;

    let _: () = FuturesUnordered::from_iter(agents.iter().map(|ta| {
        let ta = ta.clone();
        start_id += 100000;
        async move {
            tokio::spawn(async move {
                let client: hyper::Client<_, hyper::Body> = hyper::Client::builder().build_http();

                let durs = {
                    let between = Uniform::from(100..=1000);
                    let mut rng = rand::thread_rng();
                    (0..100)
                        .map(|_| between.sample(&mut rng))
                        .collect::<Vec<_>>()
                };

                let api_addr = ta.agent.api_addr();
                let actor_id = ta.agent.actor_id();

                let _: () = FuturesUnordered::from_iter(durs.into_iter().map(|dur| {
                    let client = client.clone();
                    start_id += 1;
                    async move {
                        sleep(Duration::from_millis(dur)).await;

                        let req_body = serde_json::from_value::<Vec<Statement>>(json!([[
                            "INSERT INTO tests (id,text) VALUES (?,?)",
                            [start_id, format!("hello from {actor_id}")]
                        ],]))?;

                        let res = client
                            .request(
                                hyper::Request::builder()
                                    .method(hyper::Method::POST)
                                    .uri(format!("http://{api_addr}/v1/transactions"))
                                    .header(hyper::header::CONTENT_TYPE, "application/json")
                                    .body(serde_json::to_vec(&req_body)?.into())?,
                            )
                            .await?;

                        if res.status() != StatusCode::OK {
                            eyre::bail!("bad status code: {}", res.status());
                        }

                        let body: ExecResponse =
                            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

                        match &body.results[0] {
                            ExecResult::Execute { .. } => {}
                            ExecResult::Error { error } => {
                                eyre::bail!("error: {error}");
                            }
                        }

                        Ok::<_, eyre::Report>(())
                    }
                }))
                .try_collect()
                .await?;

                Ok::<_, eyre::Report>(())
            })
            .await??;
            Ok::<_, eyre::Report>(())
        }
    }))
    .try_collect()
    .await?;

    sleep(Duration::from_secs(10)).await;

    for ta in agents {
        let conn = ta.agent.pool().read().await?;
        let count: i64 = conn.query_row("SELECT count(*) FROM tests", (), |row| row.get(0))?;

        println!("actor: {}, count: {count}", ta.agent.actor_id());
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}
