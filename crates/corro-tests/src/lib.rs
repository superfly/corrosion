use std::sync::Arc;

use corro_agent::agent::start_with_config;
// Reexport CorrosionClient and CorrosionApiClient
pub use corro_client::{CorrosionApiClient, CorrosionClient};
use corro_types::{
    agent::{Agent, Bookie},
    config::{Config, ConfigBuilder, ConfigBuilderError, CrsqliteConfig},
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

/// When any of these variables is set, all `launch_test_agent` calls use the
/// supplied cr-sqlite versions. Leave all three unset to run in the default V1
/// mode. This allows configuring each format version explicitly in CI without
/// modifying individual tests.
///
/// Usage:
/// - `CORRO_METADATA_WRITE_VERSION=2 CORRO_METADATA_USE_VERSION=2 \
///    CORRO_SYNC_LOG_VERSION=2 cargo test -p corro-agent` (dual-write)
/// - `CORRO_METADATA_WRITE_VERSION=3 CORRO_METADATA_USE_VERSION=2 \
///    CORRO_SYNC_LOG_VERSION=2 cargo test -p corro-agent` (V2-only)
pub const ENV_METADATA_WRITE_VERSION: &str = "CORRO_METADATA_WRITE_VERSION";
pub const ENV_METADATA_USE_VERSION: &str = "CORRO_METADATA_USE_VERSION";
pub const ENV_SYNC_LOG_VERSION: &str = "CORRO_SYNC_LOG_VERSION";

pub fn test_crsqlite_config() -> Option<CrsqliteConfig> {
    let parse_version = |name| std::env::var(name).ok()?.parse().ok();
    let metadata_write_version = parse_version(ENV_METADATA_WRITE_VERSION);
    let metadata_use_version = parse_version(ENV_METADATA_USE_VERSION);
    let sync_log_version = parse_version(ENV_SYNC_LOG_VERSION);

    (metadata_write_version.is_some()
        || metadata_use_version.is_some()
        || sync_log_version.is_some())
    .then_some(CrsqliteConfig {
        metadata_write_version,
        metadata_use_version,
        sync_log_version,
    })
}

pub fn is_v2_mode() -> bool {
    test_crsqlite_config().is_some()
}

pub fn test_config<F: FnOnce(ConfigBuilder) -> Result<Config, ConfigBuilderError>>(
    f: F,
) -> eyre::Result<(TempDir, Config)> {
    let tmpdir = TempDir::new(tempfile::tempdir()?);
    let schema_path = tmpdir.path().join("schema");

    let builder = Config::builder()
        .api_addr("127.0.0.1:0".parse()?)
        .gossip_addr("127.0.0.1:0".parse()?)
        .admin_path(tmpdir.path().join("admin.sock").display().to_string())
        .db_path(tmpdir.path().join("corrosion.db").display().to_string())
        .add_schema_path(schema_path.display().to_string());

    // Apply explicit cr-sqlite versions when configured for CI.
    let builder = if let Some(crsqlite_config) = test_crsqlite_config() {
        builder.crsqlite(crsqlite_config)
    } else {
        builder
    };

    let conf = f(builder)?;

    std::fs::create_dir(&schema_path)?;
    std::fs::write(schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes())?;

    Ok((tmpdir, conf))
}

pub async fn launch_test_agent<F: FnOnce(ConfigBuilder) -> Result<Config, ConfigBuilderError>>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    let (tmpdir, conf) = test_config(f)?;
    let (agent, bookie, _, _) = start_with_config(conf.clone(), tripwire).await?;

    Ok(TestAgent {
        agent,
        bookie,
        tmpdir: Arc::new(tmpdir),
        config: conf,
    })
}

/// Launch a test agent with V2 packed mode enabled.
///
/// This sets `metadata-write-version=2`, `metadata-use-version=2`, and `sync-log-version=2`.
/// The database must already be V2-compatible when this is used; startup does not run
/// a potentially unbounded migration loop.
///
/// Use this for tests that explicitly need V2 packed mode. For running the entire
/// suite in V2 packed mode, set the three `CORRO_*_VERSION` variables instead.
pub async fn launch_test_agent_v2<
    F: FnOnce(ConfigBuilder) -> Result<Config, ConfigBuilderError>,
>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    launch_test_agent(
        |conf| {
            f(conf.crsqlite(CrsqliteConfig {
                metadata_write_version: Some(2),
                metadata_use_version: Some(2),
                sync_log_version: Some(2),
            }))
        },
        tripwire,
    )
    .await
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
