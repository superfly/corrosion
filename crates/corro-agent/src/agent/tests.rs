use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    ops::Deref,
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
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

use crate::{agent::util::*, api::peer::parallel_sync, transport::Transport};
use corro_tests::*;
use corro_types::{
    actor::ActorId,
    agent::migrate,
    api::{ExecResponse, ExecResult, Statement},
    base::{CrsqlDbVersion, CrsqlSeq, Version},
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

    #[derive(Debug, Deserialize)]
    struct TestRecord {
        id: i64,
        text: String,
    }

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

                Ok::<_, eyre::Report>((*actor_id, statements.len()))
            })
        }
    })
    .try_buffer_unordered(10)
    .try_collect::<BTreeMap<_, _>>()
    .await?;

    let changes_count = 4 * input_count;

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

                for (actor_id, versions) in per_actor {
                    if let Some(versions_len) = actor_versions.get(&actor_id) {
                        let full_range = Version(1)..=Version(*versions_len as u64 + 1);
                        let gaps = versions.gaps(&full_range);
                        for gap in gaps {
                            println!("{} gap! {actor_id} => {gap:?}", ta.agent.actor_id());
                        }
                    }
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

#[test]
fn test_in_memory_versions_compaction() -> eyre::Result<()> {
    let mut conn = CrConn::init(rusqlite::Connection::open_in_memory()?)?;

    migrate(&mut conn)?;

    conn.execute_batch(
        "
            CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
            SELECT crsql_as_crr('foo');

            CREATE TABLE foo2 (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
            SELECT crsql_as_crr('foo2');

            CREATE INDEX fooclock ON foo__crsql_clock (site_id, db_version);
            CREATE INDEX foo2clock ON foo2__crsql_clock (site_id, db_version);
            ",
    )?;

    // db version 1
    conn.execute("INSERT INTO foo (a) VALUES (1)", ())?;

    // invalid, but whatever
    conn.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 1, crsql_db_version()", [])?;

    // db version 2
    conn.execute("DELETE FROM foo;", ())?;

    // invalid, but whatever
    conn.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 2, crsql_db_version()", [])?;

    let db_version: CrsqlDbVersion =
        conn.query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;

    assert_eq!(db_version, CrsqlDbVersion(2));

    {
        let mut prepped = conn.prepare("SELECT * FROM __corro_bookkeeping")?;
        let mut rows = prepped.query([])?;

        println!("bookkeeping rows:");
        while let Ok(Some(row)) = rows.next() {
            println!("row: {row:?}");
        }
    }

    {
        let mut prepped =
            conn.prepare("SELECT * FROM foo2__crsql_clock UNION SELECT * FROM foo__crsql_clock;")?;
        let mut rows = prepped.query([])?;

        println!("all clock rows:");
        while let Ok(Some(row)) = rows.next() {
            println!("row: {row:?}");
        }
    }

    {
        let mut prepped = conn.prepare("EXPLAIN QUERY PLAN SELECT DISTINCT db_version FROM foo2__crsql_clock WHERE site_id = ? UNION SELECT DISTINCT db_version FROM foo__crsql_clock WHERE site_id = ?;")?;
        let mut rows = prepped.query([0, 0])?;

        println!("matching clock rows:");
        while let Ok(Some(row)) = rows.next() {
            println!("row: {row:?}");
        }
    }

    let tx = conn.immediate_transaction()?;
    let actor_id: ActorId = tx.query_row("SELECT crsql_site_id()", [], |row| row.get(0))?;

    let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

    println!("to_clear: {to_clear:?}");

    assert!(to_clear.contains(&CrsqlDbVersion(1)));
    assert!(!to_clear.contains(&CrsqlDbVersion(2)));

    tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 1", [])?;
    tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) SELECT crsql_site_id(), 1, 1", [])?;

    let to_clear = find_cleared_db_versions(&tx, &actor_id)?;
    assert!(to_clear.is_empty());

    tx.execute("INSERT INTO foo2 (a) VALUES (2)", ())?;

    // invalid, but whatever
    tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 3, crsql_db_version()", [])?;

    tx.commit()?;

    let tx = conn.immediate_transaction()?;
    let to_clear = find_cleared_db_versions(&tx, &actor_id)?;
    assert!(to_clear.is_empty());

    tx.execute("INSERT INTO foo (a) VALUES (1)", ())?;
    tx.commit()?;

    let tx = conn.immediate_transaction()?;
    let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

    assert!(to_clear.contains(&CrsqlDbVersion(2)));
    assert!(!to_clear.contains(&CrsqlDbVersion(3)));
    assert!(!to_clear.contains(&CrsqlDbVersion(4)));

    tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 2", [])?;
    tx.execute(
        "UPDATE __corro_bookkeeping SET end_version = 2 WHERE start_version = 1;",
        [],
    )?;
    let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

    assert!(to_clear.is_empty());

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

    for _ in 0..6 {
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

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    Ok(())
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
    let mut conn = CrConn::init(rusqlite::Connection::open_in_memory()?)?;

    corro_types::sqlite::setup_conn(&mut conn)?;
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
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(2))?,
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
            store_empty_changeset(&tx, actor_id, Version(5)..=Version(7))?,
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
            store_empty_changeset(&tx, actor_id, Version(3)..=Version(6))?,
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
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(10))?,
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
            store_empty_changeset(&tx, actor_id, Version(1)..=Version(11))?,
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
            store_empty_changeset(&tx, actor_id, Version(14)..=Version(14))?,
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
            store_empty_changeset(&tx, actor_id, Version(12)..=Version(14))?,
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
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(15))?,
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
            store_empty_changeset(&tx, actor_id, Version(15)..=Version(23))?,
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

    Ok(())
}
