use std::time::{Duration, Instant};

use camino::Utf8PathBuf;
use chrono::{DateTime, NaiveDateTime, Utc};
use corro_pg::{start, PgServer};
use corro_tests::{launch_test_agent, TestAgent};
use corro_types::{
    config::{PgConfig, PgTlsConfig},
    tls::{generate_ca, generate_client_cert, generate_server_cert},
};
use pgwire::types::{format::FormatOptions, ToSqlText};
use postgres_types::{Format, IsNull, ToSql, Type};
use rcgen::Certificate;
use rustls::pki_types::pem::PemObject;
use spawn::wait_for_all_pending_handles;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use tripwire::Tripwire;

#[inline]
fn local_ephemeral() -> std::net::SocketAddr {
    std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::LOCALHOST,
        0,
    ))
}

async fn setup_pg_test_server(
    tripwire: Tripwire,
    tls_config: Option<PgTlsConfig>,
) -> (TestAgent, PgServer) {
    _ = tracing_subscriber::fmt::try_init();

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");

    tokio::fs::write(
        tmpdir.path().join("kitchensink.sql"),
        "
            CREATE TABLE kitchensink (
                id BIGINT PRIMARY KEY NOT NULL,
                other_ts DATETIME,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                trusted BOOL,
                score REAL
            );
        ",
    )
    .await
    .expect("failed to write table file");

    let ta = launch_test_agent(
        |builder| {
            builder
                .add_schema_path(tmpdir.path().display().to_string())
                .build()
        },
        tripwire.clone(),
    )
    .await
    .expect("failed to launch test agent");

    let server = start(
        ta.agent.clone(),
        PgConfig {
            bind_addr: local_ephemeral(),
            tls: tls_config,
            readonly: false,
        },
        tripwire,
    )
    .await
    .expect("failed to launch server");

    (ta, server)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_information_schema() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire, None).await;
    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(connection);

    let rows = client
        .query(
            "SELECT nspname FROM pg_catalog.pg_namespace \
             WHERE nspname IN ('main', 'public') \
             ORDER BY nspname",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, &str>("nspname"))
            .collect::<Vec<_>>(),
        ["main"]
    );

    let rows = client
        .query(
            "SELECT table_catalog, table_schema, table_name, table_type, \
             self_referencing_column_name, reference_generation, \
             user_defined_type_catalog, user_defined_type_schema, user_defined_type_name, \
             is_insertable_into, is_typed, commit_action \
             FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let table_names = rows
        .iter()
        .map(|row| row.get::<_, String>("table_name"))
        .collect::<Vec<_>>();
    for expected in [
        "__corro_schema",
        "kitchensink",
        "kitchensink__crsql_clock",
        "kitchensink__crsql_pks",
        "tests",
        "wide",
    ] {
        assert!(
            table_names.iter().any(|table_name| table_name == expected),
            "missing {expected} from information_schema.tables"
        );
    }

    for row in rows {
        assert_eq!(row.get::<_, &str>("table_catalog"), "state");
        assert_eq!(row.get::<_, &str>("table_schema"), "main");
        assert_eq!(row.get::<_, &str>("table_type"), "BASE TABLE");
        assert_eq!(row.get::<_, &str>("is_insertable_into"), "YES");
        assert_eq!(row.get::<_, &str>("is_typed"), "NO");
        assert_eq!(
            row.get::<_, Option<&str>>("self_referencing_column_name"),
            None
        );
        assert_eq!(row.get::<_, Option<&str>>("reference_generation"), None);
        assert_eq!(
            row.get::<_, Option<&str>>("user_defined_type_catalog"),
            None
        );
        assert_eq!(row.get::<_, Option<&str>>("user_defined_type_schema"), None);
        assert_eq!(row.get::<_, Option<&str>>("user_defined_type_name"), None);
        assert_eq!(row.get::<_, Option<&str>>("commit_action"), None);
    }

    let rows = client
        .query(
            "SELECT * FROM information_schema.columns \
             WHERE table_schema = 'main' AND table_name = 'kitchensink' \
             ORDER BY ordinal_position",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].columns().len(), 44);

    let id = &rows[0];
    assert_eq!(id.get::<_, &str>("table_catalog"), "state");
    assert_eq!(id.get::<_, &str>("table_schema"), "main");
    assert_eq!(id.get::<_, &str>("table_name"), "kitchensink");
    assert_eq!(id.get::<_, &str>("column_name"), "id");
    assert_eq!(id.get::<_, i64>("ordinal_position"), 1);
    assert_eq!(id.get::<_, Option<&str>>("column_default"), None);
    assert_eq!(id.get::<_, &str>("is_nullable"), "NO");
    assert_eq!(id.get::<_, &str>("data_type"), "bigint");
    assert_eq!(id.get::<_, Option<i64>>("numeric_precision"), Some(64));
    assert_eq!(id.get::<_, Option<i64>>("numeric_precision_radix"), Some(2));
    assert_eq!(id.get::<_, Option<i64>>("numeric_scale"), Some(0));
    assert_eq!(id.get::<_, &str>("udt_catalog"), "state");
    assert_eq!(id.get::<_, &str>("udt_schema"), "pg_catalog");
    assert_eq!(id.get::<_, &str>("udt_name"), "int8");
    assert_eq!(id.get::<_, &str>("dtd_identifier"), "1");
    assert_eq!(id.get::<_, &str>("is_identity"), "NO");
    assert_eq!(id.get::<_, &str>("is_generated"), "NEVER");
    assert_eq!(id.get::<_, &str>("is_updatable"), "YES");

    let other_ts = &rows[1];
    assert_eq!(other_ts.get::<_, &str>("column_name"), "other_ts");
    assert_eq!(other_ts.get::<_, &str>("is_nullable"), "YES");
    assert_eq!(
        other_ts.get::<_, &str>("data_type"),
        "timestamp without time zone"
    );
    assert_eq!(other_ts.get::<_, &str>("udt_name"), "timestamp");
    assert_eq!(
        other_ts.get::<_, Option<i64>>("datetime_precision"),
        Some(6)
    );

    let updated_at = &rows[2];
    assert_eq!(updated_at.get::<_, &str>("column_name"), "updated_at");
    assert_eq!(updated_at.get::<_, &str>("is_nullable"), "NO");
    assert_eq!(
        updated_at.get::<_, Option<&str>>("column_default"),
        Some("CURRENT_TIMESTAMP")
    );

    let trusted = &rows[3];
    assert_eq!(trusted.get::<_, &str>("column_name"), "trusted");
    assert_eq!(trusted.get::<_, &str>("data_type"), "boolean");
    assert_eq!(trusted.get::<_, &str>("udt_name"), "bool");

    let score = &rows[4];
    assert_eq!(score.get::<_, &str>("column_name"), "score");
    assert_eq!(score.get::<_, &str>("is_nullable"), "YES");
    assert_eq!(score.get::<_, &str>("data_type"), "double precision");
    assert_eq!(score.get::<_, &str>("udt_name"), "float8");

    let internal_columns = client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'kitchensink__crsql_pks' \
             ORDER BY ordinal_position",
            &[],
        )
        .await
        .unwrap();
    assert!(!internal_columns.is_empty());

    let rows = client
        .query(
            "SELECT * FROM information_schema.table_constraints \
             WHERE table_schema = 'main' \
             AND table_name IN ('kitchensink', 'wide') \
             ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns().len(), 10);

    for (row, table_name) in rows.iter().zip(["kitchensink", "wide"]) {
        assert_eq!(row.get::<_, &str>("constraint_catalog"), "state");
        assert_eq!(row.get::<_, &str>("constraint_schema"), "main");
        assert_eq!(
            row.get::<_, &str>("constraint_name"),
            format!("{table_name}_pkey")
        );
        assert_eq!(row.get::<_, &str>("table_catalog"), "state");
        assert_eq!(row.get::<_, &str>("table_schema"), "main");
        assert_eq!(row.get::<_, &str>("table_name"), table_name);
        assert_eq!(row.get::<_, &str>("constraint_type"), "PRIMARY KEY");
        assert_eq!(row.get::<_, &str>("is_deferrable"), "NO");
        assert_eq!(row.get::<_, &str>("initially_deferred"), "NO");
        assert_eq!(row.get::<_, &str>("enforced"), "YES");
    }

    // pg_class should expose user tables with relkind 'r' and the "main" namespace.
    let pg_class_rows = client
        .query(
            "SELECT oid, relname, relnamespace, relkind, relnatts \
             FROM pg_catalog.pg_class \
             WHERE relname = 'kitchensink'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(pg_class_rows.len(), 1);
    let kitchensink_oid: i64 = pg_class_rows[0].get("oid");
    assert!(kitchensink_oid >= 16384);
    assert_eq!(pg_class_rows[0].get::<_, &str>("relname"), "kitchensink");
    assert_eq!(pg_class_rows[0].get::<_, i64>("relnamespace"), 2200);
    assert_eq!(pg_class_rows[0].get::<_, &str>("relkind"), "r");
    assert_eq!(pg_class_rows[0].get::<_, i64>("relnatts"), 5);

    // pg_attribute should be populated from the same source as
    // information_schema.columns, so that tools like TablePlus can JOIN.
    let pg_attr_rows = client
        .query(
            "SELECT pa.attname, pa.atttypid, pa.attnum, pa.attnotnull, pa.atthasdef \
             FROM pg_catalog.pg_attribute pa \
             JOIN pg_catalog.pg_class pc ON pa.attrelid = pc.oid \
             WHERE pc.relname = 'kitchensink' \
             ORDER BY pa.attnum",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(pg_attr_rows.len(), 5);
    assert_eq!(pg_attr_rows[0].get::<_, &str>("attname"), "id");
    assert_eq!(pg_attr_rows[0].get::<_, i64>("attnum"), 1);
    assert_eq!(pg_attr_rows[0].get::<_, i64>("attnotnull"), 1);
    assert_eq!(pg_attr_rows[0].get::<_, i64>("atthasdef"), 0);
    // int8 OID is 20
    assert_eq!(pg_attr_rows[0].get::<_, i64>("atttypid"), 20);

    assert_eq!(pg_attr_rows[4].get::<_, &str>("attname"), "score");
    // float8 OID is 701
    assert_eq!(pg_attr_rows[4].get::<_, i64>("atttypid"), 701);

    // Querying a table with a REAL column should work (issue #513).
    client
        .execute("INSERT INTO kitchensink (id, score) VALUES (1, 2.5)", &[])
        .await
        .unwrap();
    let row = client
        .query_one("SELECT score FROM kitchensink WHERE id = 1", &[])
        .await
        .unwrap();
    let score: f64 = row.get(0);
    assert!((score - 2.5).abs() < 1e-9);

    // Look up the OID of the kitchensink table from pg_class.
    let kitchensink_oid: i64 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'kitchensink'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    // The TablePlus column-listing query: JOINs information_schema.columns
    // with pg_attribute, and calls format_type() and col_description().
    let col_rows = client
        .query(
            &format!(
                "SELECT ordinal_position, column_name, udt_name AS data_type, \
                 format_type(atttypid, atttypmod) as format_type, \
                 pg_catalog.col_description({kitchensink_oid}, ordinal_position) as comment \
                 FROM information_schema.columns \
                 JOIN pg_attribute pa ON attrelid = {kitchensink_oid} AND attname = column_name \
                 WHERE table_name = 'kitchensink' AND table_schema = 'main' \
                 ORDER BY ordinal_position"
            ),
            &[],
        )
        .await
        .unwrap();
    assert!(
        !col_rows.is_empty(),
        "column listing query returned no rows"
    );
    assert_eq!(col_rows[0].get::<_, &str>("column_name"), "id");
    assert_eq!(col_rows[0].get::<_, &str>("format_type"), "bigint");
    assert_eq!(col_rows[0].get::<_, Option<&str>>("comment"), None);

    // PostgreSQL ::type cast syntax should work (stripped before parsing).
    let count_rows = client
        .query(
            "select reltuples::int8 as count from pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace \
             where nspname='main' AND relname='kitchensink'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(count_rows.len(), 1);

    // The TablePlus full column-listing query (with numeric_precision,
    // datetime_precision, etc.)
    let full_col_rows = client
        .query(
            &format!(
                "SELECT ordinal_position,column_name,udt_name AS data_type,\
                 format_type(atttypid, atttypmod) as format_type,\
                 numeric_precision,datetime_precision,numeric_scale,\
                 character_maximum_length AS data_length,is_nullable,\
                 column_name as check,column_name as check_constraint,\
                 column_default,column_name AS foreign_key,\
                 pg_catalog.col_description({kitchensink_oid},ordinal_position) as comment \
                 FROM information_schema.columns \
                 JOIN pg_attribute pa ON attrelid={kitchensink_oid} AND attname=column_name \
                 WHERE table_name='kitchensink' AND table_schema='main'"
            ),
            &[],
        )
        .await;
    assert!(
        full_col_rows.is_ok(),
        "full column listing query failed: {:?}",
        full_col_rows.err()
    );

    // The TablePlus index-listing query (uses pg_index, pg_am, pg_get_indexdef,
    // regexp_replace, etc.)
    let index_rows = client
        .query(
            "SELECT ix.relname as index_name, upper(am.amname) AS index_algorithm, \
             indisunique as is_unique, pg_get_indexdef(indexrelid) as index_definition, \
             replace(regexp_replace(regexp_replace(regexp_replace( \
             pg_get_indexdef(indexrelid), ' WHERE .+|INCLUDE .+', ''), ' WITH .+', ''), \
             '.*\\((.*)\\)', '\\1'), ' ', '') AS column_name, \
             CASE WHEN position(' WHERE ' in pg_get_indexdef(indexrelid))>0 \
             THEN regexp_replace(pg_get_indexdef(indexrelid),'.+WHERE ','') \
             WHEN position(' WITH ' in pg_get_indexdef(indexrelid))>0 \
             THEN regexp_replace(pg_get_indexdef(indexrelid),'.+WITH ','') \
             ELSE '' END AS condition, \
             CASE WHEN position(' INCLUDE ' in pg_get_indexdef(indexrelid))>0 \
             THEN regexp_replace(pg_get_indexdef(indexrelid),'.+INCLUDE ','') \
             WHEN position(' WITH ' in pg_get_indexdef(indexrelid))>0 \
             THEN regexp_replace(pg_get_indexdef(indexrelid),'.+WITH ','') \
             ELSE '' END AS include, \
             pg_catalog.obj_description(i.indexrelid,'pg_class') as comment \
             FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid \
             JOIN pg_class ix ON ix.oid = i.indexrelid \
             JOIN pg_namespace n ON t.relnamespace = n.oid \
             JOIN pg_am as am ON ix.relam = am.oid \
             WHERE t.relname = 'kitchensink' AND n.nspname = 'main'",
            &[],
        )
        .await;
    assert!(
        index_rows.is_ok(),
        "index listing query failed: {:?}",
        index_rows.err()
    );

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (ta, server) = setup_pg_test_server(tripwire, None).await;

    let sema = ta.agent.write_sema().clone();

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let (mut client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        // let (mut client, client_conn) =
        // tokio_postgres::connect("host=localhost port=5432 user=jerome", NoTls).await?;
        println!("client is ready!");
        tokio::spawn(client_conn);

        let _permit = sema.acquire().await;

        println!("before prepare");
        let stmt = client.prepare("SELECT 1").await.unwrap();
        println!(
            "after prepare: params: {:?}, columns: {:?}",
            stmt.params(),
            stmt.columns()
        );

        println!("before query");
        // add a timeout because the semaphore shouldn't block anything here
        // it will fail if the semaphore prevents this query.
        let rows = tokio::time::timeout(Duration::from_millis(100), client.query(&stmt, &[]))
            .await
            .expect("timed out")
            .expect("failed to query rows");

        println!("rows count: {}", rows.len());
        for row in rows {
            println!("ROW!!! {row:?}");
        }

        println!("before execute");
        let start = Instant::now();
        let (affected_res, sema_elapsed) = tokio::join!(
            async {
                let affected = client
                    .execute("INSERT INTO tests VALUES (1,2)", &[])
                    .await?;
                Ok::<_, tokio_postgres::Error>((affected, start.elapsed()))
            },
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                drop(_permit);
                start.elapsed()
            }
        );

        let (affected, exec_elapsed) = affected_res.unwrap();

        println!("after execute, affected: {affected}, sema elapsed: {sema_elapsed:?}, exec elapsed: {exec_elapsed:?}");

        assert_eq!(affected, 1);

        assert!(exec_elapsed > sema_elapsed);

        let row = client
            .query_one("SELECT * FROM crsql_changes", &[])
            .await
            .unwrap();
        println!("CHANGE ROW: {row:?}");

        client
            .batch_execute("SELECT 1; SELECT 2; SELECT 3;")
            .await
            .unwrap();
        println!("after batch exec");

        client
            .batch_execute("SELECT 1; BEGIN; SELECT 3;")
            .await
            .unwrap();
        println!("after batch exec 2");

        client
            .batch_execute("SELECT 3; COMMIT; SELECT 3;")
            .await
            .unwrap();
        println!("after batch exec 3");

        let tx = client.transaction().await.unwrap();
        println!("after begin I assume");
        let res = tx
            .execute(
                "INSERT INTO tests VALUES ($1, $2)",
                &[&2i64, &"hello world"],
            )
            .await
            .unwrap();
        println!("res (rows affected): {res}");
        let res = tx
            .execute(
                "INSERT INTO tests2 VALUES ($1, $2)",
                &[&2i64, &"hello world 2"],
            )
            .await
            .unwrap();
        println!("res (rows affected): {res}");
        tx.commit().await.unwrap();
        println!("after commit");

        let row = client
            .query_one("SELECT * FROM tests t WHERE t.id = ?", &[&2i64])
            .await
            .unwrap();
        println!("ROW: {row:?}");

        let row = client
            .query_one("SELECT * FROM tests t WHERE t.id = ?", &[&2i64])
            .await
            .unwrap();
        println!("ROW: {row:?}");

        let row = client
            .query_one("SELECT * FROM tests t WHERE t.id IN (?)", &[&2i64])
            .await
            .unwrap();
        println!("ROW: {row:?}");

        let row = client
        .query_one("SELECT t.id, t.text, t2.text as t2text FROM tests t LEFT JOIN tests2 t2 WHERE t.id = ? LIMIT ?", &[&2i64, &1i64])
        .await.unwrap();
        println!("ROW: {row:?}");

        println!("t.id: {:?}", row.try_get::<_, i64>(0));
        println!("t.text: {:?}", row.try_get::<_, String>(1));
        println!("t2text: {:?}", row.try_get::<_, String>(2));

        let now: DateTime<Utc> = Utc::now();
        println!("NOW: {now:?}");

        let row = client
                .query_one(
                    "INSERT INTO kitchensink (other_ts, id, updated_at) VALUES (?1, ?2, ?1) RETURNING updated_at",
                    &[&now.naive_utc(), &1i64],
                )
                .await.unwrap();

        println!("ROW: {row:?}");
        let updated_at = row.try_get::<_, NaiveDateTime>(0).unwrap();
        println!("updated_at: {updated_at:?}");

        assert_eq!(
            now.timestamp_micros(),
            updated_at.and_utc().timestamp_micros()
        );

        let future: DateTime<Utc> = Utc::now() + Duration::from_secs(1);
        println!("NOW: {future:?}");

        let row = client
                .query_one(
                    "UPDATE kitchensink SET other_ts = $ts, updated_at = $ts WHERE id = $id AND updated_at > ? RETURNING updated_at",
                    &[&future.naive_utc(), &1i64, &(now - Duration::from_secs(1)).naive_utc()],
                )
                .await.unwrap();

        println!("ROW: {row:?}");
        let updated_at = row.try_get::<_, NaiveDateTime>(0).unwrap();
        println!("updated_at: {updated_at:?}");

        assert_eq!(
            future.timestamp_micros(),
            updated_at.and_utc().timestamp_micros()
        );

        let row = client
            .query_one(
                "SELECT COUNT(*) AS yep, COUNT(id) yeppers FROM kitchensink",
                &[],
            )
            .await
            .unwrap();
        println!("COUNT ROW: {row:?}");

        // Test bool column round-trip
        client
            .execute(
                "INSERT INTO kitchensink (id, trusted) VALUES ($1, $2)",
                &[&10i64, &true],
            )
            .await
            .unwrap();
        client
            .execute(
                "INSERT INTO kitchensink (id, trusted) VALUES ($1, $2)",
                &[&11i64, &false],
            )
            .await
            .unwrap();
        let rows = client
            .query(
                "SELECT trusted FROM kitchensink WHERE id >= 10 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, Option<bool>>(0), Some(true));
        assert_eq!(rows[1].get::<_, Option<bool>>(0), Some(false));
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tracing_test::traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test_pg_readonly() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (ta, server) = setup_pg_test_server(tripwire.clone(), None).await;

    let readonly_server = start(
        ta.agent.clone(),
        PgConfig {
            bind_addr: local_ephemeral(),
            tls: None,
            readonly: true,
        },
        tripwire,
    )
    .await
    .expect("failed to start readonly server");

    // Do some writes first
    {
        let conn_str = format!(
            "host={} port={} user=testuser",
            server.local_addr.ip(),
            server.local_addr.port()
        );

        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("failed to connect to server");
        println!("client is ready!");
        tokio::spawn(client_conn);
        client
            .execute("INSERT INTO tests VALUES (1,2)", &[])
            .await
            .expect("failed to insert");
    }

    // Then use the readonly conn
    {
        let conn_str = format!(
            "host={} port={} user=testuser",
            readonly_server.local_addr.ip(),
            readonly_server.local_addr.port()
        );

        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("failed to connect to readonly server");
        println!("readonly client is ready!");
        tokio::spawn(client_conn);
        assert_eq!(
            client
                .query_one("SELECT * FROM tests", &[])
                .await
                .unwrap()
                .get::<_, String>(1),
            "2"
        );
        assert!(client
            .execute("INSERT INTO tests VALUES (3,4)", &[])
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .message()
            .contains("readonly database"));
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tracing_test::traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test_pg_corrrosion_shutdown() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire.clone(), None).await;

    // Connect to the server
    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("failed to connect to server");
    println!("client is ready!");
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        if let Err(e) = client_conn.await {
            tx.send(e).unwrap();
        }
    });

    // Simulate a graceful shutdown
    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;

    // Check that we receive 57P01 error
    let e = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .unwrap()
        .unwrap();
    assert!(client.is_closed());
    assert!(e.as_db_error().unwrap().code().code().eq("57P01"));
    assert!(e
        .as_db_error()
        .unwrap()
        .message()
        .contains("Corrosion is shutting down"));
}

struct TestCertificates {
    ca_cert: Certificate,
    client_cert_signed: String,
    client_key: Vec<u8>,
    ca_file: Utf8PathBuf,
    server_cert_file: Utf8PathBuf,
    server_key_file: Utf8PathBuf,
}

async fn generate_and_write_certs(tmpdir: &TempDir) -> TestCertificates {
    let ca_cert = generate_ca().expect("failed to generate CA");
    let ca_pem = &ca_cert.serialize_pem().expect("failed to serialize CA");
    let (server_cert, server_cert_signed) = generate_server_cert(
        ca_pem,
        &ca_cert.serialize_private_key_pem(),
        std::net::Ipv4Addr::LOCALHOST.into(),
    )
    .expect("failed to generate server cert");

    let (client_cert, client_cert_signed) =
        generate_client_cert(ca_pem, &ca_cert.serialize_private_key_pem())
            .expect("failed to generate client cert");

    let base_path = Utf8PathBuf::from(tmpdir.path().display().to_string());

    let cert_file = base_path.join("cert.pem");
    let key_file = base_path.join("cert.key");
    let ca_file = base_path.join("ca.pem");

    let client_cert_file = base_path.join("client-cert.pem");
    let client_key_file = base_path.join("client-cert.key");

    tokio::fs::write(&cert_file, &server_cert_signed)
        .await
        .expect("failed to write server cert");
    tokio::fs::write(&key_file, server_cert.serialize_private_key_pem())
        .await
        .expect("failed to write server key");

    tokio::fs::write(
        &ca_file,
        ca_cert.serialize_pem().expect("failed to serialize CA"),
    )
    .await
    .expect("failed to write CA");

    tokio::fs::write(&client_cert_file, &client_cert_signed)
        .await
        .expect("failed to write client cert");
    tokio::fs::write(&client_key_file, client_cert.serialize_private_key_pem())
        .await
        .expect("failed to write client key");

    TestCertificates {
        server_cert_file: cert_file,
        server_key_file: key_file,
        ca_cert,
        client_cert_signed,
        client_key: client_cert.serialize_private_key_der(),
        ca_file,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg_ssl() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let tmpdir = TempDir::new().unwrap();
    let certs = generate_and_write_certs(&tmpdir).await;

    let (ta, server) = setup_pg_test_server(
        tripwire,
        Some(PgTlsConfig {
            cert_file: certs.server_cert_file,
            key_file: certs.server_key_file,
            ca_file: None,
            verify_client: false,
        }),
    )
    .await;

    let sema = ta.agent.write_sema().clone();

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        root_cert_store
            .add(rustls::pki_types::CertificateDer::from_slice(
                &certs.ca_cert.serialize_der().unwrap(),
            ))
            .unwrap();
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = MakeRustlsConnect::new(config);

        println!("connecting to: {conn_str}");

        let (client, client_conn) = tokio_postgres::connect(&conn_str, connector)
            .await
            .expect("failed to connect to server");

        tokio::spawn(client_conn);

        let _permit = sema.acquire().await;

        println!("before query");

        client
            .simple_query("SELECT 1")
            .await
            .expect("failed to run query");
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg_mtls() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let tmpdir = TempDir::new().unwrap();

    let certs = generate_and_write_certs(&tmpdir).await;

    let (ta, server) = setup_pg_test_server(
        tripwire,
        Some(PgTlsConfig {
            cert_file: certs.server_cert_file,
            key_file: certs.server_key_file,
            ca_file: Some(certs.ca_file),
            verify_client: true,
        }),
    )
    .await;

    let sema = ta.agent.write_sema().clone();

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        root_cert_store
            .add(rustls::pki_types::CertificateDer::from_slice(
                &certs
                    .ca_cert
                    .serialize_der()
                    .expect("failed to serialize cert to DER"),
            ))
            .expect("failed to add root cert");

        let client_cert =
            rustls::pki_types::CertificateDer::pem_slice_iter(certs.client_cert_signed.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to read client cert");

        let key = rustls::pki_types::PrivateKeyDer::try_from(certs.client_key.as_slice())
            .expect("failed to read key")
            .clone_key();
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store.clone())
            .with_client_auth_cert(client_cert, key)
            .expect("failed to build client config");

        let connector = MakeRustlsConnect::new(config);

        println!("connecting to: {conn_str} with client auth cert");
        let (client, client_conn) = tokio_postgres::connect(&conn_str, connector)
            .await
            .expect("failed to connect");

        tokio::spawn(client_conn);

        println!("successfully connected!");

        let _permit = sema.acquire().await;

        client.simple_query("SELECT 1").await.unwrap();

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = MakeRustlsConnect::new(config);

        println!("connecting to: {conn_str} without client auth cert");
        let result = tokio_postgres::connect(&conn_str, connector).await;
        assert!(
            result.is_err(),
            "expected connect to fail without client auth cert"
        );

        println!("successfully failed to connect without client auth cert");
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

// Checks for what types of arrays unnest can accept
async fn run_typing_unnest_case<T: Sync + Send + ToSql>(
    client: &tokio_postgres::Client,
    param_type: &str,
    should_work: &[&str],
    should_fail: &[&str],
    example_data: Vec<T>,
) {
    for fail_example in should_fail {
        let q =
            format!("SELECT CAST(value0 AS {param_type}) FROM unnest(cast ($1 as {fail_example}))");
        let r = client.query(&q, &[&example_data]).await;
        if r.is_ok() {
            println!("[FAIL] {fail_example}: Expected error, but got success");
            panic!("test failed");
        }
        let error = r.unwrap_err();
        let message = error.as_db_error().unwrap().message();
        let expected = "please use CAST($N AS T) where T is one of:";
        if !message.contains(expected) {
            println!(
                "[FAIL] {fail_example}: Expected error to contain {expected}, but got: {message}"
            );
            panic!("test failed");
        }
    }
    for work_example in should_work {
        let q =
            format!("SELECT CAST(value0 AS {param_type}) FROM unnest(cast ($1 as {work_example}))");
        let r = client.query(&q, &[&example_data]).await;
        if r.is_err() {
            println!("[FAIL] {work_example}: {:?}", r.unwrap_err());
            panic!("test failed");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unnest_typing() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire, None).await;

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        println!("client is ready!");
        tokio::spawn(client_conn);

        // Untyped arrays should fail - I wasn't able to make the "anyarray" type work
        {
            assert!(client
                .query("SELECT value0 FROM unnest($1)", &[&vec![1i64, 2, 3]])
                .await
                .unwrap_err()
                .as_db_error()
                .unwrap()
                .message()
                .contains("Untyped array argument"));
        }

        // Arrays only work for specific types
        // Test int types
        {
            let should_work = [
                "int[]",
                "integer[]",
                "bigint[]",
                "INT[]",
                "INTEGER[]",
                "BIGINT[]",
                "InT[]",
                "InTeGer[]",
                "BiGInT[]",
                "int      []",
            ];
            let should_fail = [
                "int", "integer", "bigint", "INT", "INTEGER", "BIGINT", "InT", "InTeGer", "BiGInT",
                "int",
            ];
            run_typing_unnest_case(&client, "int", &should_work, &should_fail, vec![1i64, 2, 3])
                .await;

            // Array types are very pedantic - need i64 or else it won't work :(
            let pedantic = (
                vec![1i32, 2, 3],
                vec![1i16, 2, 3],
                vec![1i8, 2, 3],
                vec![1u32, 2, 3],
                vec![1u8, 2, 3],
            );
            assert!(client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[]))",
                    &[&pedantic.0]
                )
                .await
                .is_err());
            assert!(client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[]))",
                    &[&pedantic.1]
                )
                .await
                .is_err());
            assert!(client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[]))",
                    &[&pedantic.2]
                )
                .await
                .is_err());
            assert!(client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[]))",
                    &[&pedantic.3]
                )
                .await
                .is_err());
            assert!(client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[]))",
                    &[&pedantic.4]
                )
                .await
                .is_err());
        }
        // Test float types
        {
            let should_work = [
                "real[]",
                "float[]",
                "double[]",
                "REAL[]",
                "FLOAT[]",
                "DOUBLE[]",
                "ReAl[]",
                "FlOat[]",
                "DoUbLe[]",
                "real      []",
                "float      []",
                "double      []",
            ];
            let should_fail = [
                "real", "float", "double", "REAL", "FLOAT", "DOUBLE", "ReAl", "FlOat", "DoUbLe",
            ];
            run_typing_unnest_case(
                &client,
                "float",
                &should_work,
                &should_fail,
                vec![1.0, 2.0, 3.0],
            )
            .await;

            // Array types are very pedantic - need f64 or else it won't work :(
            let pedantic = (vec![1.0f32, 2.0, 3.0], vec![1i64, 2, 3]);
            assert!(client
                .query(
                    "SELECT CAST(value0 AS float) FROM unnest(CAST($1 AS float[]))",
                    &[&pedantic.0]
                )
                .await
                .is_err());
            assert!(client
                .query(
                    "SELECT CAST(value0 AS float) FROM unnest(CAST($1 AS float[]))",
                    &[&pedantic.1]
                )
                .await
                .is_err());
        }
        // Test text types
        {
            let should_work = ["text[]", "TEXT[]", "Text[]", "text      []"];
            let should_fail = ["text", "TEXT", "Text", "text"];
            run_typing_unnest_case(
                &client,
                "text",
                &should_work,
                &should_fail,
                vec!["a", "b", "c"],
            )
            .await;
        }
        // Test blob types
        {
            let should_work = ["blob[]", "BLOB[]", "Blob[]", "blob      []"];
            let should_fail = ["blob", "BLOB", "Blob", "blob"];
            run_typing_unnest_case(
                &client,
                "blob",
                &should_work,
                &should_fail,
                vec![b"a", b"b", b"c"],
            )
            .await;
        }
        // Test bool types
        {
            let should_work = [
                "bool[]",
                "BOOL[]",
                "Bool[]",
                "boolean[]",
                "BOOLEAN[]",
                "bool      []",
            ];
            let should_fail = ["bool", "BOOL", "boolean", "BOOLEAN"];
            run_typing_unnest_case(
                &client,
                "bool",
                &should_work,
                &should_fail,
                vec![true, false, true],
            )
            .await;
        }
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

// wrapper so we can easily switch between text and binary formats
#[derive(Debug)]
struct SqlVec<'a, T> {
    inner: &'a Vec<T>,
    format: Format,
}

// test text encoding/decoding
impl<'a, T: ToSqlText + ToSql> ToSql for SqlVec<'a, T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.format {
            Format::Text => self.inner.to_sql_text(ty, out, &FormatOptions::default()),
            Format::Binary => self.inner.to_sql(ty, out),
        }
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        matches!(ty.kind(), postgres_types::Kind::Array(_))
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        self.format
    }

    postgres_types::to_sql_checked!();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unnest_max_parameters() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire, None).await;

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        println!("client is ready!");
        tokio::spawn(client_conn);

        // expected_max_arrays parameters should work
        let expected_max_arrays = 32;
        let per_column = 16;
        {
            let cols = (0..expected_max_arrays)
                .map(|i| {
                    (0..per_column)
                        .map(|j| i * 1000 + j as i64)
                        .collect::<Vec<i64>>()
                })
                .collect::<Vec<Vec<i64>>>();
            let rets = (0..expected_max_arrays)
                .map(|i| format!("CAST(value{i} AS int)"))
                .collect::<Vec<_>>()
                .join(",");
            let arrs = (0..expected_max_arrays)
                .map(|i| format!("CAST(${i} AS int[])"))
                .collect::<Vec<_>>()
                .join(",");
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = cols
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            let rows = client
                .query(&format!("SELECT {rets} FROM unnest({arrs})"), &params)
                .await
                .unwrap();
            for (i, row) in rows.iter().enumerate() {
                for j in 0..per_column {
                    let val: i64 = row.get(j);
                    assert_eq!(val, cols[i][j]);
                }
            }
        }

        // but not expected_max_arrays + 1
        {
            let cols = (0..expected_max_arrays + 1)
                .map(|i| {
                    (0..per_column)
                        .map(|j| i * 1000 + j as i64)
                        .collect::<Vec<i64>>()
                })
                .collect::<Vec<Vec<i64>>>();
            let rets = (0..expected_max_arrays + 1)
                .map(|i| format!("CAST(value{i} AS int)"))
                .collect::<Vec<_>>()
                .join(", ");
            let arrs = (0..expected_max_arrays + 1)
                .map(|i| format!("CAST(${i} AS int[])"))
                .collect::<Vec<_>>()
                .join(", ");
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = cols
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            assert!(client
                .query(&format!("SELECT {rets} FROM unnest({arrs})"), &params,)
                .await
                .is_err());
        }
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_request_cancellation() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire, None).await;

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        println!("client is ready!");
        tokio::spawn(client_conn);

        // cancel the query after 2 seconds
        let cancel_token = client.cancel_token();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            cancel_token.cancel_query(NoTls).await.unwrap();
        });

        let res = client.query("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 1000000000) SELECT MAX(x) FROM cnt", &[]).await;
        assert!(res.is_err());
        assert!(res
            .err()
            .unwrap()
            .as_db_error()
            .unwrap()
            .message()
            .contains("interrupted"));
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unnest_vtab() {
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

    let (_ta, server) = setup_pg_test_server(tripwire, None).await;

    let conn_str = format!(
        "host={} port={} user=testuser",
        server.local_addr.ip(),
        server.local_addr.port()
    );

    {
        let (client, client_conn) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
        println!("client is ready!");
        tokio::spawn(client_conn);

        // Test single array unnest with int type
        {
            let col1 = vec![1i64, 2, 3, 4, 1337, 12312312312];
            for format in [Format::Text, Format::Binary] {
                for func in ["unnest", "corro_unnest"] {
                    let sql_vec = SqlVec {
                        inner: &col1,
                        format,
                    };
                    let rows = client
                        .query(
                            &format!("SELECT CAST(value0 AS int) FROM {func}(CAST($1 AS int[]))"),
                            &[&sql_vec],
                        )
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), col1.len());
                    for (i, row) in rows.iter().enumerate() {
                        let val: i64 = row.get(0);
                        assert_eq!(val, col1[i]);
                    }
                }
            }
        }

        // Test single array unnest with text type
        {
            for format in [Format::Text, Format::Binary] {
                for func in ["unnest", "corro_unnest"] {
                    let col1 = vec!["a", "b", "c", "d", "e", "f", ""];
                    let sql_vec = SqlVec {
                        inner: &col1,
                        format,
                    };
                    let rows = client
                        .query(
                            &format!("SELECT CAST(value0 AS text) FROM {func}(CAST($1 AS text[]))"),
                            &[&sql_vec],
                        )
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), col1.len());
                    for (i, row) in rows.iter().enumerate() {
                        let val: String = row.get(0);
                        assert_eq!(val, col1[i]);
                    }
                }
            }
        }

        // Test single array unnest with float type
        {
            let col1 = vec![1.0, 2.0, 3.0, 4.0, 1337.0, 12312312312.0];
            for format in [Format::Text, Format::Binary] {
                for func in ["unnest", "corro_unnest"] {
                    let sql_vec = SqlVec {
                        inner: &col1,
                        format,
                    };
                    let rows = client
                        .query(
                            &format!(
                                "SELECT CAST(value0 AS float) FROM {func}(CAST($1 AS float[]))"
                            ),
                            &[&sql_vec],
                        )
                        .await
                        .unwrap();
                    for (i, row) in rows.iter().enumerate() {
                        let val: f64 = row.get(0);
                        assert_eq!(val, col1[i]);
                    }
                }
            }
        }

        // Test single array unnest with blob type
        // TODO: pgwire's text encoding for blob[] is currently broken but we'd work for proper clients
        {
            let col1 = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
            for func in ["unnest", "corro_unnest"] {
                let rows = client
                    .query(
                        &format!("SELECT CAST(value0 AS blob) FROM {func}(CAST($1 AS blob[]))"),
                        &[&col1],
                    )
                    .await
                    .unwrap();
                for (i, row) in rows.iter().enumerate() {
                    let val: Vec<u8> = row.get(0);
                    assert_eq!(val, col1[i]);
                }
            }
        }

        // Test single array unnest with bool type
        {
            let col1 = vec![true, false, true, false, true];
            for format in [Format::Text, Format::Binary] {
                for func in ["unnest", "corro_unnest"] {
                    let sql_vec = SqlVec {
                        inner: &col1,
                        format,
                    };
                    let rows = client
                        .query(
                            &format!("SELECT CAST(value0 AS bool) FROM {func}(CAST($1 AS bool[]))"),
                            &[&sql_vec],
                        )
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), col1.len());
                    for (i, row) in rows.iter().enumerate() {
                        let val: bool = row.get(0);
                        assert_eq!(val, col1[i]);
                    }
                }
            }
        }

        // Now try all at once with different types, use corro_unnest
        {
            let col1 = vec![1i64, 2, 3, 4, 1337, 12312312312];
            let col2 = vec!["a", "b", "c", "d", "e", "f"];
            let col3 = vec![1.0, 2.0, 3.0, 4.0, 1337.0, 12312312312.0];
            let col4 = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
            let rows = client
                .query(
                    "SELECT
                    CAST(value0 AS int), CAST(value1 AS text), CAST(value2 AS float), CAST(value3 AS blob) FROM corro_unnest(CAST($1 AS int[]), CAST($2 AS text[]), CAST($3 AS float[]), CAST($4 AS blob[]))",
                    &[&col1, &col2, &col3, &col4],
                )
                .await
                .unwrap();
            for (i, row) in rows.iter().enumerate() {
                let val0: i64 = row.get(0);
                let val1: String = row.get(1);
                let val2: f64 = row.get(2);
                let val3: Vec<u8> = row.get(3);
                assert_eq!(val0, col1[i]);
                assert_eq!(val1, col2[i]);
                assert_eq!(val2, col3[i]);
                assert_eq!(val3, col4[i]);
            }
        }

        // If one array is longer than the others, the shorter arrays should be padded with NULLs
        // This matches the behavior of PostgreSQL
        {
            let col1 = vec![1i64, 2, 3];
            let col2 = vec!["a", "b", "c", "d", "e", "f"];
            let statement = "SELECT CAST(value0 AS int), CAST(value1 AS text) FROM unnest(CAST($1 AS int[]), CAST($2 AS text[]))";
            let rows = client.query(statement, &[&col1, &col2]).await.unwrap();
            assert_eq!(rows.len(), col2.len());
            for (i, row) in rows.iter().enumerate() {
                let val0: Option<i64> = row.get(0);
                let val1: String = row.get(1);
                assert_eq!(val0, col1.get(i).copied());
                assert_eq!(val1, col2[i]);
            }
        }

        // Test with WHERE clause
        {
            let col1 = vec![10i64, 20, 30, 40];
            let rows = client
                .query(
                    "SELECT CAST(value0 AS int) FROM unnest(CAST($1 AS int[])) WHERE value0 > 20",
                    &[&col1],
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            for (i, row) in rows.iter().enumerate() {
                let val0: i64 = row.get(0);
                assert_eq!(val0, col1[i + 2]);
            }
        }

        // Test INSERT, JOIN and IN
        {
            let ids = [1i64, 2, 3];
            let texts = ["one", "two", "three"];
            client
                .execute("INSERT INTO tests (id, text) SELECT value0, value1 FROM unnest(CAST($1 AS int[]), CAST($2 AS text[]))", &[&ids, &texts])
                .await
                .unwrap();

            let joiner = [1i64, 2];
            let rows = client
                .query(
                    "SELECT t.id, t.text, CAST(u.value0 AS int) FROM tests t 
                     JOIN unnest(CAST($1 AS int[])) u ON t.id = u.value0",
                    &[&joiner],
                )
                .await
                .unwrap();

            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get::<_, i64>(0), 1);
            assert_eq!(rows[0].get::<_, String>(1), "one");
            assert_eq!(rows[1].get::<_, i64>(0), 2);
            assert_eq!(rows[1].get::<_, String>(1), "two");

            let rows = client
                .query(
                    "SELECT t.id, t.text FROM tests t WHERE t.id IN (SELECT value0 FROM unnest(CAST($1 AS int[])))",
                    &[&joiner],
                )
                .await
                .unwrap();

            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get::<_, i64>(0), 1);
            assert_eq!(rows[0].get::<_, String>(1), "one");
            assert_eq!(rows[1].get::<_, i64>(0), 2);
            assert_eq!(rows[1].get::<_, String>(1), "two");
        }
    }

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}
