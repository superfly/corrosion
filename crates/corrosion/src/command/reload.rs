use std::path::Path;

use corro_admin::Command;
use tracing::info;

use crate::admin::AdminConn;

pub async fn run<P: AsRef<Path>>(admin_path: P) -> eyre::Result<()> {
    let mut conn = AdminConn::connect(admin_path).await?;
    conn.send_command(Command::Reload).await?;
    info!("Successfully reloaded Corrosion!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use camino::Utf8PathBuf;
    use corro_admin::AdminConfig;
    use corro_tests::launch_test_agent;
    use spawn::wait_for_all_pending_handles;
    use tripwire::Tripwire;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_operations() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

        let config_path: Utf8PathBuf = ta
            .tmpdir
            .path()
            .join("config.toml")
            .display()
            .to_string()
            .into();

        let agent = ta.agent.clone();
        let listen_path = agent.config().admin_path.clone();
        corro_admin::start_server(
            agent,
            AdminConfig {
                listen_path,
                config_path: config_path.clone(),
            },
            tripwire,
        )?;

        let mut conf = ta.agent.config().as_ref().clone();
        let new_path = ta.tmpdir.path().join("schema2");
        tokio::fs::create_dir_all(&new_path).await?;

        tokio::fs::write(
            new_path.join("blah.sql"),
            b"CREATE TABLE blah (id BIGINT PRIMARY KEY);",
        )
        .await?;

        conf.schema_paths
            .push(new_path.display().to_string().into());

        let conf_bytes = toml::to_vec(&conf)?;

        tokio::fs::write(&config_path, conf_bytes).await?;

        run(&ta.agent.config().admin_path).await?;

        assert!(ta
            .agent
            .config()
            .schema_paths
            .iter()
            .any(|p| *p == new_path));

        assert!(ta.agent.schema().read().tables.contains_key("blah"));

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
