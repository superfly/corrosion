use std::path::Path;

use corro_admin::{Command, LogLevel, Response};
use futures::{SinkExt, TryStreamExt};
use tokio::net::UnixStream;
use tokio_serde::{formats::Json, Framed};
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{error, event, info};

pub async fn run<P: AsRef<Path>>(admin_path: P) -> eyre::Result<()> {
    handle_reload(admin_path).await
}

type FramedStream = Framed<
    tokio_util::codec::Framed<UnixStream, LengthDelimitedCodec>,
    Response,
    Command,
    Json<Response, Command>,
>;

async fn connect<P: AsRef<Path>>(path: P) -> eyre::Result<FramedStream> {
    let stream = UnixStream::connect(path).await?;
    Ok(Framed::new(
        tokio_util::codec::Framed::new(stream, LengthDelimitedCodec::new()),
        Json::default(),
    ))
}

async fn handle_reload<P: AsRef<Path>>(path: P) -> eyre::Result<()> {
    let mut stream = connect(path).await?;

    stream.send(Command::Reload).await?;

    loop {
        let res = stream.try_next().await?;

        match res {
            None => {
                error!("Failed to get response from Corrosion's admin!");
                break;
            }
            Some(res) => match res {
                Response::Log { level, msg, ts } => match level {
                    LogLevel::Trace => event!(tracing::Level::TRACE, %ts, "{msg}"),
                    LogLevel::Debug => event!(tracing::Level::DEBUG, %ts, "{msg}"),
                    LogLevel::Info => event!(tracing::Level::INFO, %ts, "{msg}"),
                    LogLevel::Warn => event!(tracing::Level::WARN, %ts, "{msg}"),
                    LogLevel::Error => event!(tracing::Level::ERROR, %ts, "{msg}"),
                },
                Response::Error { msg } => {
                    error!("{msg}");
                    break;
                }
                Response::Success => {
                    info!("Successfully reloaded Corrosion!");
                    break;
                }
            },
        }
    }

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

        handle_reload(&ta.agent.config().admin_path).await?;

        assert!(ta
            .agent
            .config()
            .schema_paths
            .iter()
            .any(|p| *p == new_path));

        assert!(ta.agent.0.schema.read().tables.contains_key("blah"));

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
