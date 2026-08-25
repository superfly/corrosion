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

/// Environment variable name. When set, all `launch_test_agent` calls
/// automatically enable V2 packed mode (sync-log-version=2, metadata-use-version=2).
/// The `metadata-write-version` is determined by the value:
/// - `"1"` or `"true"` → `metadata-write-version=2` (dual-write, default)
/// - `"3"` → `metadata-write-version=3` (V2-only, fresh databases only)
///
/// This allows running the entire test suite in V2 packed mode for CI without
/// modifying individual tests.
///
/// Usage:
/// - `CORRO_TEST_V2=1 cargo test -p corro-agent` (dual-write, 2,2,2)
/// - `CORRO_TEST_V2=3 cargo test -p corro-agent` (V2-only, 3,2,2)
pub const ENV_TEST_V2: &str = "CORRO_TEST_V2";

/// Returns the `metadata-write-version` to use when V2 packed mode is enabled,
/// or `None` if V2 packed mode is disabled.
pub fn v2_write_version() -> Option<i64> {
    match std::env::var(ENV_TEST_V2) {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => Some(2),
        Ok(v) if v == "3" => Some(3),
        _ => None,
    }
}

pub fn is_v2_mode() -> bool {
    v2_write_version().is_some()
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

    // Auto-enable V2 packed mode if env var is set
    let builder = if let Some(write_version) = v2_write_version() {
        builder.crsqlite(CrsqliteConfig {
            metadata_write_version: Some(write_version),
            metadata_use_version: Some(2),
            sync_log_version: Some(2),
        })
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
/// The V1→V2 migration is run synchronously during startup (via `run_maintenance_until_done`
/// in `apply_crsqlite_config`) so the agent starts fully in V2 packed mode.
///
/// Use this for tests that explicitly need V2 packed mode. For running the entire
/// suite in V2 packed mode, set `CORRO_TEST_V2=1` instead.
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
