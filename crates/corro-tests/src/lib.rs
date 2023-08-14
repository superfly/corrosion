use std::sync::Arc;

use camino::Utf8PathBuf;
use corro_agent::agent::start;
use corro_types::{
    agent::Agent,
    config::{Config, ConfigBuilder, ConfigBuilderError, TlsConfig},
};
use tempfile::TempDir;
use tripwire::Tripwire;

pub const TEST_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS tests2 (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS testsblob (
            id BLOB NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;
    "#;

#[derive(Clone)]
pub struct TestAgent {
    pub agent: Agent,
    pub tmpdir: Arc<TempDir>,
}

pub async fn launch_test_agent<F: FnOnce(ConfigBuilder) -> Result<Config, ConfigBuilderError>>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    let tmpdir = tempfile::tempdir()?;

    let schema_path = tmpdir.path().join("schema");

    let tls_path = Utf8PathBuf::from(tmpdir.path().display().to_string()).join("tls");
    let cert_path = tls_path.join("cert.pem");
    let key_path = tls_path.join("key.pem");

    let cert = rcgen::generate_simple_self_signed(vec![hostname::get()
        .unwrap()
        .to_string_lossy()
        .into_owned()])?;
    let key = cert.serialize_private_key_pem();
    let cert = cert.serialize_pem().unwrap();
    tokio::fs::create_dir_all(tls_path).await?;
    tokio::fs::write(&cert_path, &cert).await?;
    tokio::fs::write(&key_path, &key).await?;

    let conf = f(Config::builder()
        .api_addr("127.0.0.1:0".parse()?)
        .gossip_addr("127.0.0.1:0".parse()?)
        .tls_config(TlsConfig {
            cert_file: cert_path,
            key_file: key_path,
            ca_file: None,
            insecure: true,
            default_server_name: "cluster.test".into(),
        })
        .admin_path(tmpdir.path().join("admin.sock").display().to_string())
        .db_path(tmpdir.path().join("corrosion.db").display().to_string())
        .add_schema_path(schema_path.display().to_string()))?;

    tokio::fs::create_dir(&schema_path).await?;
    tokio::fs::write(schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes()).await?;

    let schema_paths = conf.db.schema_paths.clone();

    let agent = start(conf, tripwire).await?;

    {
        let client = corro_client::CorrosionApiClient::new(agent.api_addr());
        client.schema_from_paths(&schema_paths).await?;
    }

    Ok(TestAgent {
        agent,
        tmpdir: Arc::new(tmpdir),
    })
}
