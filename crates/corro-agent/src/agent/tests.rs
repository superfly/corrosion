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

use crate::{
    agent::process_multiple_changes,
    api::{
        peer::parallel_sync,
        public::{api_v1_db_schema, api_v1_transactions},
    },
    transport::Transport,
};
use corro_tests::*;
use corro_types::agent::Agent;
use corro_types::broadcast::Timestamp;
use corro_types::change::Change;
use corro_types::sync::get_last_cleared_ts;
use corro_types::{
    actor::ActorId,
    agent::migrate,
    api::{row_to_change, ExecResponse, ExecResult, Statement},
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{ChangeSource, ChangeV1, Changeset},
    change::store_empty_changeset,
    sqlite::CrConn,
    sync::generate_sync,
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

    #[allow(clippy::type_complexity)]
        let bk: Vec<(ActorId, Version, Option<Version>, CrsqlDbVersion, Option<CrsqlSeq>)> = ta1
            .agent
            .pool()
            .read()
            .await?
            .prepare("SELECT actor_id, start_version, end_version, db_version, last_seq FROM __corro_bookkeeping")?
            .query_map((), |row| {
                Ok((
                    row.get::<_, ActorId>(0)?,
                    row.get::<_, Version>(1)?,
                    row.get::<_, Option<Version>>(2)?,
                    row.get::<_, CrsqlDbVersion>(3)?,
                    row.get::<_, Option<CrsqlSeq>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;

    assert_eq!(
        bk,
        vec![
            (
                ta1.agent.actor_id(),
                Version(1),
                None,
                CrsqlDbVersion(1),
                Some(CrsqlSeq(0))
            ),
            (
                ta1.agent.actor_id(),
                Version(2),
                None,
                CrsqlDbVersion(2),
                Some(CrsqlSeq(0))
            )
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
    configurable_stress_test(2, 1, 1).await
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

    let actor_versions = tokio_stream::StreamExt::map(futures::stream::iter(iter).chunks(20), {
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
    .await?;

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
                .all(|(n, needed)| *n == changes_count as i64 && *needed == 0)
        {
            break;
        }

        println!("we're not done yet...");

        if start.elapsed() > Duration::from_secs(30) {
            for ta in agents.iter() {
                let conn = ta.agent.pool().read().await?;
                let mut per_actor: BTreeMap<ActorId, RangeInclusiveSet<Version>> = BTreeMap::new();
                let mut prepped = conn.prepare("SELECT actor_id, start_version, coalesce(end_version, start_version) FROM __corro_bookkeeping;")?;
                let mut rows = prepped.query(())?;

                while let Ok(Some(row)) = rows.next() {
                    per_actor
                        .entry(row.get(0)?)
                        .or_default()
                        .insert(row.get(1)?..=row.get(2)?);
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
                        let full_range = Version(1)..=Version(*versions_len as u64);
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
                    .collect::<Result<Vec<(ActorId, Version, Version)>, _>>()?;

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
    for _ in 0..6 {
        let res = parallel_sync(
            &ta2.agent,
            &ta2_transport,
            vec![(ta1.agent.actor_id(), ta1.agent.gossip_addr())],
            generate_sync(&ta2.bookie, ta2.agent.actor_id()).await,
            HashMap::new(),
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
            HashMap::new(),
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
            HashMap::new(),
        )
        .await?;

        println!("ta4 synced {res}");

        tokio::time::sleep(Duration::from_secs(1)).await;
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
                .read("test")
                .await
                .get(&ta1.agent.actor_id())
                .unwrap()
                .read("test")
                .await
                .deref()
        );

        if count as usize != expected_count {
            let buf_count: Vec<(Version, u64)> = conn
                .prepare("select version,count(*) from __corro_buffered_changes group by version")?
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
            "{name}: actor {actor_id} did not reach 10K rows",
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
    let rows = get_rows(ta1.agent.clone(), vec![(Version(1)..=Version(50), None)]).await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;

    // overwrite different version ranges
    insert_rows(ta1.agent.clone(), 1, 5).await;
    insert_rows(ta1.agent.clone(), 10, 10).await;
    insert_rows(ta1.agent.clone(), 23, 25).await;
    insert_rows(ta1.agent.clone(), 30, 31).await;

    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (Version(51)..=Version(55), None),
            (Version(56)..=Version(56), None),
            (Version(57)..=Version(59), None),
            (Version(60)..=Version(60), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![Version(1)..=Version(50)],
        vec![],
        vec![],
        vec![],
    )
    .await?;

    let mut last_cleared: HashMap<ActorId, Option<Timestamp>> = HashMap::new();
    last_cleared.insert(
        ta1.agent.actor_id(),
        get_last_cleared_ts(&ta2.bookie, &ta1.agent.actor_id()).await,
    );

    println!("got last cleared - {last_cleared:?}");

    // initiate sync with ta1 to get cleared
    let res = parallel_sync(
        &ta2.agent,
        &ta2_transport,
        vec![(ta1.agent.actor_id(), ta1.agent.gossip_addr())],
        generate_sync(&ta2.bookie, ta2.agent.actor_id()).await,
        last_cleared,
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
            Version(1)..=Version(5),
            Version(10)..=Version(10),
            Version(23)..=Version(25),
            Version(30)..=Version(31),
        ],
    )
    .await?;

    // ta2 should have ta1's last cleared
    let ta1_cleared = ta1
        .agent
        .booked()
        .read("test_clear_empty")
        .await
        .last_cleared_ts();
    let ta2_ta1_cleared = ta2
        .bookie
        .write("test")
        .await
        .ensure(ta1.agent.actor_id())
        .read("test_clear_empty")
        .await
        .last_cleared_ts();

    assert_eq!(ta1_cleared, ta2_ta1_cleared);

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
    let rows = get_rows(ta1.agent.clone(), vec![(Version(1)..=Version(5), None)]).await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    // check ta2 bookie
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![Version(1)..=Version(5)],
        vec![],
        vec![],
        vec![],
    )
    .await?;

    // sent: 1-5, 9-10
    let rows = get_rows(ta1.agent.clone(), vec![(Version(9)..=Version(10), None)]).await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    // check for gap 6-8
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![Version(6)..=Version(8)],
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
            (Version(20)..=Version(20), None),
            (Version(15)..=Version(16), Some(CrsqlSeq(0)..=CrsqlSeq(0))),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    // check for gap 11-14 and 17-19
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![Version(11)..=Version(14), Version(17)..=Version(19)],
        vec![(Version(15)..=Version(16), CrsqlSeq(0)..=CrsqlSeq(0))],
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
            (Version(21)..=Version(21), None),
            (Version(25)..=Version(25), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;

    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![],
        vec![],
        vec![],
        vec![Version(21)..=Version(21), Version(25)..=Version(25)],
    )
    .await?;

    // send some missing gaps, partials and cleared version
    // sent 1-5, 9-10, 14-18, 20, 23-25
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (Version(14)..=Version(18), None),
            (Version(15)..=Version(16), Some(CrsqlSeq(1)..=CrsqlSeq(3))),
            (Version(23)..=Version(24), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![Version(14)..=Version(18), Version(15)..=Version(16)],
        vec![
            Version(11)..=Version(13),
            Version(19)..=Version(19),
            Version(22)..=Version(22),
        ],
        vec![],
        vec![Version(23)..=Version(25)],
    )
    .await?;

    // sent 1-25
    let rows = get_rows(
        ta1.agent.clone(),
        vec![
            (Version(6)..=Version(8), None),
            (Version(11)..=Version(19), None),
            (Version(22)..=Version(22), None),
        ],
    )
    .await?;
    process_multiple_changes(ta2.agent.clone(), ta2.bookie.clone(), rows).await?;
    check_bookie_versions(
        ta2.clone(),
        ta1.agent.actor_id(),
        vec![Version(1)..=Version(20)],
        vec![],
        vec![],
        vec![Version(21)..=Version(25)],
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
    complete: Vec<RangeInclusive<Version>>,
    gap: Vec<RangeInclusive<Version>>,
    partials: Vec<(RangeInclusive<Version>, RangeInclusive<CrsqlSeq>)>,
    cleared: Vec<RangeInclusive<Version>>,
) -> eyre::Result<()> {
    let conn = ta.agent.pool().read().await?;
    let booked = ta.bookie.write("test").await.ensure(actor_id);
    let bookedv = booked.read("test").await;

    for versions in complete {
        for version in versions.clone() {
            let bk: Vec<(ActorId, Version, Option<Version>)> = conn
                .prepare(
                    "SELECT actor_id, start_version, end_version FROM __corro_bookkeeping where start_version = ?",
                )?
                .query_map([version], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?.collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(bk, vec![(actor_id, version, None)]);

            // should not be in gaps
            assert!(!conn.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM __corro_bookkeeping_gaps WHERE actor_id = ? and ? between start and end)")?
                .query_row((actor_id, version), |row| row.get(0))?);
        }
        bookedv.contains_all(versions.clone(), Some(&(CrsqlSeq(0)..=CrsqlSeq(3))));
    }

    for versions in partials {
        for version in versions.0.clone() {
            let bk: Vec<(ActorId, Version, CrsqlSeq, CrsqlSeq)> = conn
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
        assert!(conn.prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM __corro_bookkeeping WHERE actor_id = ? and start_version = ? and end_version = ?)")?
            .query_row((actor_id, versions.start(), versions.end()), |row| row.get(0))?, "Versions {versions:?} not cleared in corro bookkeeping table");
    }

    Ok(())
}

async fn get_rows(
    agent: Agent,
    v: Vec<(RangeInclusive<Version>, Option<RangeInclusive<CrsqlSeq>>)>,
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
                query = query + seq_query;
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
        // assert_eq!(version, Version(i as u64));
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

    FuturesUnordered::from_iter(agents.iter().map(|ta| {
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

                FuturesUnordered::from_iter(durs.into_iter().map(|dur| {
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

#[test]
fn test_store_empty_changeset() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let mut conn = CrConn::init(rusqlite::Connection::open_in_memory()?)?;

    corro_types::sqlite::setup_conn(&conn)?;
    migrate(&mut conn)?;

    let actor_id = ActorId(uuid::Uuid::new_v4());

    #[derive(Debug, Eq, PartialEq)]
    struct CorroBook {
        actor_id: ActorId,
        start_version: Version,
        end_version: Option<Version>,
    }

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 1)",
        [actor_id],
    )?;

    // store an empty version 1..=2 when 1 was considered non-empty
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(2), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(2))
        }
    );

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 3)",
        [actor_id],
    )?;

    // insert empty version 5..=7 that does not overlap with anything else
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(5)..=Version(7), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(2))
        }
    );
    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(3),
            end_version: None
        }
    );
    assert_eq!(
        rows[2],
        CorroBook {
            actor_id,
            start_version: Version(5),
            end_version: Some(Version(7))
        }
    );

    // insert empty changes 3..=6 which touches non-empty version 3 and empty versions 5..=7
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(3)..=Version(6), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(7))
        }
    );

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 12)",
        [actor_id],
    )?;

    // insert changes that hae the same start as already emptied rows, but go up higher
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(10), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 2);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(10))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(12),
            end_version: None
        }
    );

    // insert changes that hae the same start as already emptied rows, but go up higher

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(11), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 2);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(11))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(12),
            end_version: None
        }
    );

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 13)",
        [actor_id],
    )?;

    // insert empties that don't touch any other rows

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(14)..=Version(14), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 4);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(11))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(12),
            end_version: None
        }
    );
    assert_eq!(
        rows[2],
        CorroBook {
            actor_id,
            start_version: Version(13),
            end_version: None
        }
    );

    assert_eq!(
        rows[3],
        CorroBook {
            actor_id,
            start_version: Version(14),
            end_version: Some(Version(14))
        }
    );

    // empties multiple non-empty versions (12 and 13) and touches already emptied version (14)
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(12)..=Version(14), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(14))
        }
    );

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 15)",
        [actor_id],
    )?;
    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?, 16, 18)",
        [actor_id],
    )?;

    // empties a version in between 2 ranges of empties
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(15), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(18))
        }
    );

    // empties versions overlapping the end of a previous range
    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(23), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 1);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(23))
        }
    );

    // live version after empty range

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 24)",
        [actor_id],
    )?;

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(23), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 2);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(23))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(24),
            end_version: None
        }
    );

    // live version before empty range

    conn.execute(
        "UPDATE __corro_bookkeeping SET start_version = 2 WHERE actor_id = ? AND start_version = 1",
        [actor_id],
    )?;

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 1)",
        [actor_id],
    )?;

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(23), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping ORDER BY start_version ASC")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: None
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(2),
            end_version: Some(Version(23))
        }
    );

    assert_eq!(
        rows[2],
        CorroBook {
            actor_id,
            start_version: Version(24),
            end_version: None
        }
    );

    // live version after empty range, with a gap

    conn.execute(
        "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 29)",
        [actor_id],
    )?;

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(24), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(26)..=Version(27), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(24))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(26),
            end_version: Some(Version(27))
        }
    );

    assert_eq!(
        rows[2],
        CorroBook {
            actor_id,
            start_version: Version(29),
            end_version: None
        }
    );

    // live version before empty range, with a gap

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(29), Default::default())?,
            1
        );
        tx.commit()?;
    }

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(40)..=Version(45), Default::default())?,
            1
        );
        tx.commit()?;
    }

    {
        let tx = conn.transaction()?;
        assert_eq!(
            store_empty_changeset(&tx, actor_id, Version(35)..=Version(37), Default::default())?,
            1
        );
        tx.commit()?;
    }

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    let rows = conn
        .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
        .query_map([], |row| {
            Ok(CorroBook {
                actor_id: row.get(0)?,
                start_version: row.get(1)?,
                end_version: row.get(2)?,
            })
        })
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    println!("rows: {rows:?}");

    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0],
        CorroBook {
            actor_id,
            start_version: Version(1),
            end_version: Some(Version(29))
        }
    );

    assert_eq!(
        rows[1],
        CorroBook {
            actor_id,
            start_version: Version(35),
            end_version: Some(Version(37))
        }
    );

    assert_eq!(
        rows[2],
        CorroBook {
            actor_id,
            start_version: Version(40),
            end_version: Some(Version(45))
        }
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_automatic_bookkeeping_clearing() -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
    let ta2 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

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

    let (status_code, body) = api_v1_transactions(
        Extension(ta1.agent.clone()),
        axum::Json(vec![Statement::WithParams(
            "insert into tests (id, text) values (?,?)".into(),
            vec!["service-id".into(), "service-name".into()],
        )]),
    )
    .await;

    assert_eq!(status_code, StatusCode::OK);

    let version = body.0.version.unwrap();

    assert_eq!(version, Version(1));

    let conn = ta1.agent.pool().read().await?;

    // check locally that everything is in order

    let bk: Vec<(ActorId, Version, Option<Version>, CrsqlDbVersion)> = conn
        .prepare(
            "SELECT actor_id, start_version, end_version, db_version FROM __corro_bookkeeping",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    assert_eq!(
        bk,
        vec![(ta1.agent.actor_id(), version, None, CrsqlDbVersion(1))]
    );

    let mut changes = vec![];
    let mut prepped = conn.prepare(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes"#)?;
    let mut rows = prepped.query([])?;

    while let Some(row) = rows.next()? {
        changes.push(row_to_change(row)?);
    }

    let last_seq = CrsqlSeq((changes.len() - 1) as u64);

    // apply changes on actor #2

    process_multiple_changes(
        ta2.agent.clone(),
        ta2.bookie.clone(),
        vec![(
            ChangeV1 {
                actor_id: ta1.agent.actor_id(),
                changeset: Changeset::Full {
                    version,
                    changes,
                    seqs: CrsqlSeq(0)..=last_seq,
                    last_seq,
                    ts: ta1.agent.clock().new_timestamp().into(),
                },
            },
            ChangeSource::Broadcast,
            Instant::now(),
        )],
    )
    .await?;

    let (status_code, body) = api_v1_transactions(
        Extension(ta1.agent.clone()),
        axum::Json(vec![Statement::WithParams(
            "insert or replace into tests (id, text) values (?,?)".into(),
            vec!["service-id".into(), "service-name-overwrite".into()],
        )]),
    )
    .await;

    assert_eq!(status_code, StatusCode::OK);

    let version = body.0.version.unwrap();

    assert_eq!(version, Version(2));

    let bk: Vec<(ActorId, Version, Option<Version>, Option<CrsqlDbVersion>)> = conn
        .prepare(
            "SELECT actor_id, start_version, end_version, db_version FROM __corro_bookkeeping",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    assert_eq!(
        bk,
        vec![
            (ta1.agent.actor_id(), Version(1), Some(Version(1)), None),
            (ta1.agent.actor_id(), version, None, Some(CrsqlDbVersion(2)))
        ]
    );

    let mut changes = vec![];
    let mut prepped = conn.prepare(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes WHERE db_version = 2"#)?;
    let mut rows = prepped.query([])?;

    while let Some(row) = rows.next()? {
        changes.push(row_to_change(row)?);
    }

    let last_seq = CrsqlSeq((changes.len() - 1) as u64);

    process_multiple_changes(
        ta2.agent.clone(),
        ta2.bookie.clone(),
        vec![(
            ChangeV1 {
                actor_id: ta1.agent.actor_id(),
                changeset: Changeset::Full {
                    version,
                    changes,
                    seqs: CrsqlSeq(0)..=last_seq,
                    last_seq,
                    ts: ta1.agent.clock().new_timestamp().into(),
                },
            },
            ChangeSource::Broadcast,
            Instant::now(),
        )],
    )
    .await?;

    let conn = ta2.agent.pool().read().await?;

    let bk: Vec<(ActorId, Version, Option<Version>, Option<CrsqlDbVersion>)> = conn
        .prepare(
            "SELECT actor_id, start_version, end_version, db_version FROM __corro_bookkeeping",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    assert_eq!(
        bk,
        vec![
            (
                ta1.agent.actor_id(),
                Version(1),
                None,
                Some(CrsqlDbVersion(1))
            ),
            (ta1.agent.actor_id(), version, None, Some(CrsqlDbVersion(2)))
        ]
    );

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
}
