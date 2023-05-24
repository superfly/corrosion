use std::{fmt::Display, time::Duration};

use camino::Utf8PathBuf;
use corro_types::{
    agent::{reload, Agent},
    config::Config,
    sqlite::SqlitePoolError,
};
use futures::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use spawn::spawn_counted;
use time::OffsetDateTime;
use tokio::{
    net::{UnixListener, UnixStream},
    task::block_in_place,
};
use tokio_serde::{formats::Json, Framed};
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error, info, warn};
use tripwire::Tripwire;

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub listen_path: Utf8PathBuf,
    pub config_path: Utf8PathBuf,
}

pub fn start_server(
    agent: Agent,
    config: AdminConfig,
    mut tripwire: Tripwire,
) -> Result<(), AdminError> {
    _ = std::fs::remove_file(&config.listen_path);
    info!("Starting Corrosion admin socket at {}", config.listen_path);

    if let Some(parent) = config.listen_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let ln = UnixListener::bind(&config.listen_path)?;

    spawn_counted(async move {
        loop {
            let stream = tokio::select! {
                accept_res = ln.accept() => match accept_res {
                    Ok((stream, _addr)) => stream,
                    Err(e) => {
                        error!("error accepting for admin connections: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                },
                _ = &mut tripwire => {
                    info!("Admin tripped!");
                    break;
                }
            };

            spawn_counted({
                let agent = agent.clone();
                let config = config.clone();
                async move {
                    if let Err(e) = handle_conn(agent, config, stream).await {
                        error!("could not handle admin connection: {e}");
                    }
                }
            });
        }
        info!("Admin is done.")
    });

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Reload,
    Backup { path: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for tracing::Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Log {
        level: LogLevel,
        msg: String,
        ts: OffsetDateTime,
    },
    Error {
        msg: String,
    },
    Success,
}

type FramedStream = Framed<
    tokio_util::codec::Framed<UnixStream, LengthDelimitedCodec>,
    Command,
    Response,
    Json<Command, Response>,
>;

async fn handle_conn(
    agent: Agent,
    config: AdminConfig,
    stream: UnixStream,
) -> Result<(), AdminError> {
    // wrap in stream in line delimited json decoder
    let mut stream: FramedStream = tokio_serde::Framed::new(
        tokio_util::codec::Framed::new(stream, LengthDelimitedCodec::new()),
        Json::<Command, Response>::default(),
    );

    loop {
        match stream.try_next().await {
            Ok(Some(cmd)) => match cmd {
                Command::Reload => match Config::load(config.config_path.as_str()) {
                    Ok(conf) => {
                        info_log(&mut stream, "Reloading...").await;
                        if let Err(e) = reload(&agent, conf).await {
                            send_error(&mut stream, e).await;
                            continue;
                        }
                        send(&mut stream, Response::Success).await;
                    }
                    Err(e) => {
                        send_error(&mut stream, e).await;
                    }
                },
                Command::Backup { path } => match handle_backup(&agent, path).await {
                    Ok(_) => {
                        send_success(&mut stream).await;
                    }
                    Err(e) => {
                        send_error(&mut stream, e).await;
                    }
                },
            },
            Ok(None) => {
                debug!("done with admin conn");
                break;
            }
            Err(e) => {
                error!("could not decode incoming frame as command: {e}");
                send_error(&mut stream, e).await;
                break;
            }
        }
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

async fn handle_backup(agent: &Agent, path: String) -> Result<(), BackupError> {
    let conn = agent.read_only_pool().get().await?;
    block_in_place(|| conn.execute("VACUUM INTO ?;", [&path]))?;
    Ok(())
}

async fn send(stream: &mut FramedStream, res: Response) {
    if let Err(e) = stream.send(res).await {
        warn!("could not send response=: {e}");
    }
}

async fn send_log<M: Into<String>>(stream: &mut FramedStream, level: LogLevel, msg: M) {
    send(
        stream,
        Response::Log {
            level,
            msg: msg.into(),
            ts: OffsetDateTime::now_utc(),
        },
    )
    .await
}

async fn info_log<M: Into<String>>(stream: &mut FramedStream, msg: M) {
    send_log(stream, LogLevel::Info, msg).await
}

async fn send_success(stream: &mut FramedStream) {
    send(stream, Response::Success).await
}

async fn send_error<E: Display>(stream: &mut FramedStream, error: E) {
    send(
        stream,
        Response::Error {
            msg: error.to_string(),
        },
    )
    .await
}
