use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use hyper::StatusCode;
use rand::{
    distributions::Uniform, prelude::Distribution, rngs::StdRng, seq::IteratorRandom, SeedableRng,
};
use serde::Deserialize;
use serde_json::json;
use spawn::wait_for_all_pending_handles;
use tokio::time::{sleep, timeout, MissedTickBehavior};
use tracing::{debug, info_span};
use tripwire::Tripwire;

use crate::agent::util::*;
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
async fn stress_test() -> eyre::Result<()> {
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

    let mut interval = tokio::time::interval(Duration::from_secs(1));
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
                    .for_actor(ta.agent.actor_id())
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

    sleep(Duration::from_secs(2)).await;

    let ta2 = launch_test_agent(
        |conf| {
            conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                .build()
        },
        tripwire.clone(),
    )
    .await?;

    sleep(Duration::from_secs(1)).await;

    let ta3 = launch_test_agent(
        |conf| {
            conf.bootstrap(vec![ta2.agent.gossip_addr().to_string()])
                .build()
        },
        tripwire.clone(),
    )
    .await?;

    sleep(Duration::from_secs(1)).await;

    let ta4 = launch_test_agent(
        |conf| {
            conf.bootstrap(vec![ta3.agent.gossip_addr().to_string()])
                .build()
        },
        tripwire.clone(),
    )
    .await?;

    sleep(Duration::from_secs(20)).await;

    for ta in [ta2, ta3, ta4] {
        let agent = ta.agent;
        let conn = agent.pool().read().await?;

        let count: u64 = conn
            .prepare_cached("SELECT COUNT(*) FROM testsbool;")?
            .query_row((), |row| row.get(0))?;

        println!("{:#?}", generate_sync(&ta.bookie, agent.actor_id()).await);

        if count as usize != expected_count {
            let buf_count: u64 =
                conn.query_row("select count(*) from __corro_buffered_changes", [], |row| {
                    row.get(0)
                })?;
            println!(
                "BUFFERED COUNT: {buf_count} (actor_id: {})",
                agent.actor_id()
            );

            let ranges = conn
                .prepare("select start_seq, end_seq from __corro_seq_bookkeeping")?
                .query_map([], |row| Ok(row.get::<_, u64>(0)?..=row.get::<_, u64>(1)?))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            println!("ranges: {ranges:?}");
        }

        assert_eq!(
            count as usize,
            expected_count,
            "actor {} did not reach 10K rows",
            agent.actor_id()
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

    Ok(())
}
