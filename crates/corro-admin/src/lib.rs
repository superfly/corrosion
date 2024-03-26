use std::{fmt::Display, time::Duration};

use camino::Utf8PathBuf;
use corro_types::{
    actor::{ActorId, ClusterId},
    agent::{Agent, Bookie, KnownVersion, LockKind, LockMeta, LockState},
    base::Version,
    broadcast::{FocaCmd, FocaInput},
    sqlite::SqlitePoolError,
    sync::generate_sync,
};
use futures::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use spawn::spawn_counted;
use time::OffsetDateTime;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
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
    bookie: Bookie,
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
                let bookie = bookie.clone();
                let config = config.clone();
                async move {
                    if let Err(e) = handle_conn(agent, &bookie, config, stream).await {
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
    Cluster(ClusterCommand),
    Actor(ActorCommand),
    CompactEmpties,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncCommand {
    Generate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    Rejoin,
    Members,
    MembershipStates,
    SetId(ClusterId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActorCommand {
    Version { actor_id: ActorId, version: Version },
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
    bookie: &Bookie,
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
                    let sync_state = generate_sync(bookie, agent.actor_id()).await;
                    match serde_json::to_value(&sync_state) {
                        Ok(json) => send(&mut stream, Response::Json(json)).await,
                        Err(e) => send_error(&mut stream, e).await,
                    }
                    send_success(&mut stream).await;
                }
                Command::CompactEmpties => {
                    info_log(&mut stream, "compacting empty versions...").await;
                    let pool = agent.pool();
                    
                    // clear_overwritten_versions(&agent, agent.booked(), pool).await;
                }
                Command::Locks { top } => {
                    info_log(&mut stream, "gathering top locks").await;
                    let registry = bookie.registry();

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
                    send_success(&mut stream).await;
                }
                Command::Cluster(ClusterCommand::Rejoin) => {
                    let (cb_tx, cb_rx) = oneshot::channel();

                    if let Err(e) = agent
                        .tx_foca()
                        .send(FocaInput::Cmd(FocaCmd::Rejoin(cb_tx)))
                        .await
                    {
                        send_error(&mut stream, e).await;
                        continue;
                    }

                    if let Err(e) = cb_rx.await {
                        send_error(&mut stream, e).await;
                        continue;
                    }

                    info_log(&mut stream, "Rejoined cluster with a renewed identity").await;

                    send_success(&mut stream).await;
                }
                Command::Cluster(ClusterCommand::Members) => {
                    debug_log(&mut stream, "gathering members").await;

                    let values = {
                        let members = agent.members().read();
                        members
                            .states
                            .iter()
                            .map(|(actor_id, state)| {
                                let rtts =
                                    members.rtts.get(&state.addr).map(|rtt| rtt.buf.to_vec());
                                json!({
                                    "id": actor_id,
                                    "state": state,
                                    "rtts": rtts,
                                })
                            })
                            .collect::<Vec<_>>()
                    };

                    for value in values {
                        send(&mut stream, Response::Json(value)).await;
                    }
                }
                Command::Cluster(ClusterCommand::MembershipStates) => {
                    info_log(&mut stream, "gathering membership state").await;

                    let (tx, mut rx) = mpsc::channel(1024);
                    if let Err(e) = agent
                        .tx_foca()
                        .send(FocaInput::Cmd(FocaCmd::MembershipStates(tx)))
                        .await
                    {
                        send_error(&mut stream, e).await;
                        continue;
                    }

                    while let Some(member) = rx.recv().await {
                        match serde_json::to_value(&member) {
                            Ok(json) => send(&mut stream, Response::Json(json)).await,
                            Err(e) => send_error(&mut stream, e).await,
                        }
                    }
                    send_success(&mut stream).await;
                }
                Command::Cluster(ClusterCommand::SetId(cluster_id)) => {
                    info_log(&mut stream, format!("setting new cluster id: {cluster_id}")).await;

                    let mut conn = match agent.pool().write_priority().await {
                        Ok(conn) => conn,
                        Err(e) => {
                            send_error(&mut stream, e).await;
                            continue;
                        }
                    };

                    let res = block_in_place(|| {
                        let tx = conn.transaction()?;

                        tx.execute("INSERT OR REPLACE INTO __corro_state (key, value) VALUES ('cluster_id', ?)", [cluster_id])?;

                        let (cb_tx, cb_rx) = oneshot::channel();

                        agent
                            .tx_foca()
                            .blocking_send(FocaInput::Cmd(FocaCmd::ChangeIdentity(
                                agent.actor(cluster_id),
                                cb_tx,
                            )))
                            .map_err(|_| ProcessingError::Send)?;

                        cb_rx
                            .blocking_recv()
                            .map_err(|_| ProcessingError::CallbackRecv)?
                            .map_err(|e| ProcessingError::String(e.to_string()))?;

                        tx.commit()?;

                        agent.set_cluster_id(cluster_id);

                        Ok::<_, ProcessingError>(())
                    });

                    if let Err(e) = res {
                        send_error(&mut stream, e).await;
                        continue;
                    }

                    send_success(&mut stream).await;
                }
                Command::Actor(ActorCommand::Version { actor_id, version }) => {
                    let json = {
                        let bookie = bookie.read("admin actor version").await;
                        let booked = match bookie.get(&actor_id) {
                            Some(booked) => booked,
                            None => {
                                send_error(&mut stream, format!("unknown actor id: {actor_id}"))
                                    .await;
                                continue;
                            }
                        };
                        let booked_read = booked.read("admin actor version booked").await;
                        match booked_read.get(&version) {
                            Some(known) => match known {
                                KnownVersion::Cleared => {
                                    Ok(serde_json::Value::String("cleared".into()))
                                }
                                KnownVersion::Current(known) => serde_json::to_value(known)
                                    .map(|v| serde_json::json!({"current": v})),
                                KnownVersion::Partial(known) => serde_json::to_value(known)
                                    .map(|v| serde_json::json!({"partial": v})),
                            },
                            None => Ok(serde_json::Value::Null),
                        }
                    };

                    match json {
                        Ok(j) => _ = send(&mut stream, Response::Json(j)).await,
                        Err(e) => {
                            _ = send_error(&mut stream, e).await;
                            continue;
                        }
                    }

                    send_success(&mut stream).await;
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
pub enum ProcessingError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("could not send via channel")]
    Send,
    #[error("could not receive response from a callback")]
    CallbackRecv,
    #[error("{0}")]
    String(String),
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
async fn debug_log<M: Into<String>>(stream: &mut FramedStream, msg: M) {
    send_log(stream, LogLevel::Debug, msg).await
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
