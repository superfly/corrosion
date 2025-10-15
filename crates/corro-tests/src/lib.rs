use std::sync::Arc;

use corro_agent::agent::start_with_config;
// Reexport CorrosionClient and CorrosionApiClient
pub use corro_client::{CorrosionApiClient, CorrosionClient};
use corro_types::{
    agent::{Agent, Bookie},
    config::{Config, ConfigBuilder, ConfigBuilderError},
    sqlite::{rusqlite_to_crsqlite, SqlitePool},
};
use tripwire::Tripwire;

pub mod tempdir;
use tempdir::TempDir;

pub const TEST_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS tests2 (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS tests3 (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT "",
            text2 TEXT NOT NULL DEFAULT "",
            num INTEGER NOT NULL DEFAULT 0,
            num2 INTEGER NOT NULL DEFAULT 0
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS testsblob (
            id BLOB NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS testsbool (
            id INTEGER NOT NULL PRIMARY KEY,
            b boolean not null default false
        );

        CREATE TABLE IF NOT EXISTS wide (
            id1 BLOB NOT NULL,
            id2 TEXT NOT NULL,

            int INTEGER NOT NULL DEFAULT 1,
            float REAL NOT NULL DEFAULT 1.0,

            blob BLOB,

            PRIMARY KEY (id1, id2)
        );
    "#;

#[derive(Clone)]
pub struct TestAgent {
    pub agent: Agent,
    pub bookie: Bookie,
    pub tmpdir: Arc<TempDir>,
    pub config: Config,
}

pub async fn launch_test_agent<F: FnOnce(ConfigBuilder) -> Result<Config, ConfigBuilderError>>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    let tmpdir = TempDir::new(tempfile::tempdir()?);
    let schema_path = tmpdir.path().join("schema");

    let conf = f(Config::builder()
        .api_addr("127.0.0.1:0".parse()?)
        .gossip_addr("127.0.0.1:0".parse()?)
        .admin_path(tmpdir.path().join("admin.sock").display().to_string())
        .db_path(tmpdir.path().join("corrosion.db").display().to_string())
        .add_schema_path(schema_path.display().to_string()))?;

    tokio::fs::create_dir(&schema_path).await?;
    tokio::fs::write(schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes()).await?;

    let (agent, bookie, _, _) = start_with_config(conf.clone(), tripwire).await?;

    Ok(TestAgent {
        agent,
        bookie,
        tmpdir: Arc::new(tmpdir),
        config: conf,
    })
}

impl TestAgent {
    pub fn client(&self) -> CorrosionClient {
        CorrosionClient::new(self.agent.api_addr(), self.agent.db_path()).unwrap()
    }

    pub fn api_client(&self) -> CorrosionApiClient {
        CorrosionApiClient::new(self.agent.api_addr()).unwrap()
    }

    // Use for out-of-band inserts
    pub fn oob_pool(&self) -> SqlitePool {
        sqlite_pool::Config::new(self.agent.db_path())
            .max_size(1)
            .create_pool_transform(rusqlite_to_crsqlite)
            .unwrap()
    }
}

/// Clone a test agent by copying its database using VACUUM INTO
/// This ensures identical database state without re-running setup logic
pub async fn clone_test_agent(source: &TestAgent, tripwire: Tripwire) -> eyre::Result<TestAgent> {
    let tmpdir = TempDir::new(tempfile::tempdir()?);
    let schema_path = tmpdir.path().join("schema");
    let target_db_path = tmpdir.path().join("corrosion.db");

    // Use VACUUM INTO to clone the database
    let source_conn = source.agent.pool().read().await?;
    source_conn.execute("VACUUM INTO ?", [target_db_path.display().to_string()])?;
    drop(source_conn);

    // Create config for new agent with cloned DB
    let conf = Config::builder()
        .api_addr("127.0.0.1:0".parse()?)
        .gossip_addr("127.0.0.1:0".parse()?)
        .admin_path(tmpdir.path().join("admin.sock").display().to_string())
        .db_path(target_db_path.display().to_string())
        .add_schema_path(schema_path.display().to_string())
        .build()?;

    // Copy schema files
    tokio::fs::create_dir(&schema_path).await?;
    for entry in std::fs::read_dir(source.tmpdir.path().join("schema"))? {
        let entry = entry?;
        if entry.path().is_file() {
            let file_name = entry.file_name();
            tokio::fs::copy(entry.path(), schema_path.join(&file_name)).await?;
        }
    }

    let (agent, bookie, _, _) = start_with_config(conf.clone(), tripwire).await?;

    Ok(TestAgent {
        agent,
        bookie,
        tmpdir: Arc::new(tmpdir),
        config: conf,
    })
}

impl Drop for TestAgent {
    fn drop(&mut self) {
        if std::env::var_os("NO_TEMPDIR_CLEANUP").is_some() {
            println!("Dropping test agent {}", self.agent.actor_id());
        }
    }
}
