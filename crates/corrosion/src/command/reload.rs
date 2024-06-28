use std::{net::SocketAddr, path::Path};

use corro_client::CorrosionApiClient;
use tracing::info;

pub async fn run<P: AsRef<Path>>(
    api_addr: SocketAddr,
    schema_paths: &[P],
    allow_destructive: bool,
) -> eyre::Result<()> {
    let client = CorrosionApiClient::new(api_addr);

    client
        .schema_from_paths(schema_paths, allow_destructive)
        .await?;
    info!("Successfully reloaded Corrosion's schema from paths!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use corro_tests::launch_test_agent;
    use spawn::wait_for_all_pending_handles;
    use tripwire::Tripwire;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_operations() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

        let client = corro_client::CorrosionApiClient::new(ta.agent.api_addr());
        client
            .schema_from_paths(&ta.agent.config().db.schema_paths, false)
            .await?;

        let mut conf = ta.agent.config().as_ref().clone();
        let new_path = ta.tmpdir.path().join("schema2");
        tokio::fs::create_dir_all(&new_path).await?;

        tokio::fs::write(
            new_path.join("blah.sql"),
            b"CREATE TABLE blah (id BIGINT NOT NULL PRIMARY KEY);",
        )
        .await?;

        conf.db
            .schema_paths
            .push(new_path.display().to_string().into());

        println!("conf: {conf:?}");

        run(ta.agent.api_addr(), &conf.db.schema_paths, false).await?;

        assert!(ta.agent.schema().read().tables.contains_key("blah"));

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
