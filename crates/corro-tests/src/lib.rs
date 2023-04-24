use std::sync::Arc;

use corro_types::agent::Agent;
use corrosion::{
    agent::start,
    config::{Config, ConfigBuilder, ConfigError},
};
use tempfile::TempDir;
use tripwire::Tripwire;

const TEST_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS tests2 (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;
    "#;

#[derive(Clone)]
pub struct TestAgent {
    pub agent: Agent,
    _tmpdir: Arc<TempDir>,
}

pub async fn launch_test_agent<F: FnOnce(ConfigBuilder) -> Result<Config, ConfigError>>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    let tmpdir = tempfile::tempdir()?;

    let conf = f(Config::builder()
        .api_addr("127.0.0.1:0".parse()?)
        .gossip_addr("127.0.0.1:0".parse()?)
        .base_path(tmpdir.path().display().to_string()))?;

    if conf.schema_path == conf.base_path.join("schema") {
        tokio::fs::create_dir(&conf.schema_path).await?;
        tokio::fs::write(conf.schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes()).await?;
    }

    start(conf, tripwire).await.map(|agent| TestAgent {
        agent,
        _tmpdir: Arc::new(tmpdir),
    })
}
