use std::{fmt::Display, time::Duration};

use camino::Utf8PathBuf;
use corro_types::{
    agent::{Agent, LockKind, LockMeta, LockState},
    sqlite::SqlitePoolError,
    sync::generate_sync,
};
use futures::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use spawn::spawn_counted;
use time::OffsetDateTime;
use tokio::net::{UnixListener, UnixStream};
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
    Ping,
    Sync(SyncCommand),
    Locks { top: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncCommand {
    Generate,
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
    Json(serde_json::Value),
}

type FramedStream = Framed<
    tokio_util::codec::Framed<UnixStream, LengthDelimitedCodec>,
    Command,
    Response,
    Json<Command, Response>,
>;

#[derive(Serialize, Deserialize)]
pub struct LockMetaElapsed {
    pub label: String,
    pub kind: LockKind,
    pub state: LockState,
    pub duration: Duration,
}

impl From<LockMeta> for LockMetaElapsed {
    fn from(value: LockMeta) -> Self {
        LockMetaElapsed {
            label: value.label.into(),
            kind: value.kind,
            state: value.state,
            duration: value.started_at.elapsed(),
        }
    }
}

async fn handle_conn(
    agent: Agent,
    _config: AdminConfig,
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
                Command::Ping => send_success(&mut stream).await,
                Command::Sync(SyncCommand::Generate) => {
                    info_log(&mut stream, "generating sync...").await;
                    let sync_state = generate_sync(agent.bookie(), agent.actor_id()).await;
                    match serde_json::to_value(&sync_state) {
                        Ok(json) => send(&mut stream, Response::Json(json)).await,
                        Err(e) => send_error(&mut stream, e).await,
                    }
                }
                Command::Locks { top } => {
                    info_log(&mut stream, "gathering top locks").await;
                    let registry = {
                        agent
                            .bookie()
                            .read("admin:registry")
                            .await
                            .registry()
                            .clone()
                    };

                    let topn: Vec<LockMetaElapsed> = {
                        registry
                            .map
                            .read()
                            .values()
                            .take(top)
                            .cloned()
                            .map(LockMetaElapsed::from)
                            .collect()
                    };

                    match serde_json::to_value(&topn) {
                        Ok(json) => send(&mut stream, Response::Json(json)).await,
                        Err(e) => send_error(&mut stream, e).await,
                    }
                }
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
