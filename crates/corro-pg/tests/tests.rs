use std::time::{Duration, Instant};

use camino::Utf8PathBuf;
use chrono::{DateTime, NaiveDateTime, Utc};
use corro_pg::{start, PgServer};
use corro_tests::{launch_test_agent, TestAgent};
use corro_types::{
    config::{PgConfig, PgTlsConfig},
    tls::{generate_ca, generate_client_cert, generate_server_cert},
};
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
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    let e = tokio::time::timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
    assert!(client.is_closed());
    assert!(e.as_db_error().unwrap().code().code().eq("57P01"));
    assert!(e.as_db_error().unwrap().message().contains("Corrosion is shutting down"));
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
