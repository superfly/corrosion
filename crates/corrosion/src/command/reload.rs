use std::path::Path;

use crate::admin::AdminConn;
use corro_admin::Command;
use tracing::info;

pub async fn run<P: AsRef<Path>>(admin_path: P) -> eyre::Result<()> {
    let mut conn = AdminConn::connect(admin_path).await?;
    if let Err(e) = conn.send_command(Command::Reload).await {
        error!("Failed to reload schema: {e}");
        return Err(eyre!("Failed to reload schema: {e}"));
    }
    info!("Successfully reloaded Corrosion's schema from configured paths!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use corro_admin::{start_server, AdminConfig};
    use corro_tests::launch_test_agent;
    use spawn::wait_for_all_pending_handles;
    use tripwire::Tripwire;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_operations() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

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
        ta.agent.set_config(conf);

        start_server(
            ta.agent.clone(),
            ta.bookie.clone(),
            AdminConfig {
                listen_path: ta.agent.config().admin.uds_path.clone(),
                config_path: ta.agent.config().db.path.clone(),
            },
            None,
            tripwire.clone(),
        )?;

        run(ta.agent.config().admin.uds_path.as_std_path()).await?;

        assert!(ta.agent.schema().read().tables.contains_key("blah"));

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
