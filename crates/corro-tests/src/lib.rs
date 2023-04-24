use std::{net::SocketAddr, sync::Arc};

use corro_types::agent::Agent;
use corrosion::{
    agent::start,
    config::{Config, ConfigBuilder},
};
use tempfile::TempDir;
use tracing::trace;
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

pub async fn launch_test_agent(
    id: &str,
    bootstrap: Vec<String>,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    launch_test_agent_w_gossip(id, "127.0.0.1:0".parse()?, bootstrap, "test", tripwire).await
}

pub async fn launch_test_agent_with_region(
    id: &str,
    bootstrap: Vec<String>,
    region: &str,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    launch_test_agent_w_gossip(id, "127.0.0.1:0".parse()?, bootstrap, region, tripwire).await
}

pub async fn launch_test_agent_w_builder<F: FnOnce(ConfigBuilder) -> eyre::Result<Config>>(
    f: F,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    let tmpdir = tempfile::tempdir()?;

    let conf = f(Config::builder().base_path(tmpdir.path().display().to_string()))?;

    let schema_path = tmpdir.path().join("schema");
    tokio::fs::create_dir(&schema_path).await?;
    tokio::fs::write(schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes()).await?;

    start(conf, tripwire).await.map(|agent| TestAgent {
        agent,
        _tmpdir: Arc::new(tmpdir),
    })
}

pub async fn launch_test_agent_w_gossip(
    id: &str,
    gossip_addr: SocketAddr,
    bootstrap: Vec<String>,
    _region: &str,
    tripwire: Tripwire,
) -> eyre::Result<TestAgent> {
    trace!("launching test agent {id}");
    launch_test_agent_w_builder(
        move |conf| {
            Ok(conf
                .gossip_addr(gossip_addr)
                .api_addr("127.0.0.1:0".parse()?)
                .bootstrap(bootstrap)
                .build()?)
        },
        tripwire,
    )
    .await
}
