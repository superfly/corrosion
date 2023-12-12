use std::{
    collections::{HashMap, HashSet},
    env::current_dir,
    net::SocketAddr,
    time::{Duration, Instant},
};

use camino::Utf8PathBuf;
use clap::Args;
use corro_client::CorrosionApiClient;
use corro_tpl::{Dynamic, TemplateCommand, TemplateState};
use futures::{stream::FuturesUnordered, StreamExt};
use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, Debouncer};
use tokio::{
    sync::mpsc::{self, channel, Receiver, Sender},
    task::block_in_place,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

#[derive(Args)]
pub struct TemplateFlags {
    #[arg(short, long)]
    once: bool,
}

pub async fn run(
    api_addr: SocketAddr,
    template: &Vec<String>,
    flags: &TemplateFlags,
) -> eyre::Result<()> {
    let client = CorrosionApiClient::new(api_addr);

    let mut filepaths = vec![];

    let cwd = current_dir()?;

    let mut futs = FuturesUnordered::new();

    for tpl in template {
        let mut splitted = tpl.splitn(3, ':');
        let mut src: Utf8PathBuf = splitted
            .next()
            .ok_or_else(|| eyre::eyre!("missing source template"))?
            .into();

        if src.is_relative() {
            src = cwd.join(&src).canonicalize()?.try_into()?;
        }

        let (notify_tx, mut notify_rx) = channel(1);

        filepaths.push((src.clone(), notify_tx));

        let mut mtime = match tokio::fs::metadata(&src).await {
            Ok(meta) => {
                if meta.is_dir() {
                    eyre::bail!("source path should be a file, not a directory");
                }
                meta.modified()?
            }
            Err(e) => return Err(e.into()),
        };

        let dst: Utf8PathBuf = splitted
            .next()
            .ok_or_else(|| eyre::eyre!("missing destination file"))?
            .into();

        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // rejoin
        let cmd = splitted.next().map(shellwords::split).transpose()?;

        debug!("src: {src}, dst: {dst}, cmd: {cmd:?}");

        let input = tokio::fs::read_to_string(&src).await?;

        let dir = tempfile::tempdir()?;

        let client = client.clone();

        let once = flags.once;

        futs.push(async move {
            let mut checksum = crc32fast::hash(input.as_bytes());

            let mut engine = corro_tpl::Engine::new::<std::fs::File>(client.clone());

            let mut tpl = engine.compile_mut(&input)?;
            let tmp_filepath = dir.path().join(Uuid::new_v4().as_simple().to_string());

            info!("Watching and rendering {src} to {dst}");

            'outer: loop {
                let f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp_filepath)
                    .await?;

                let f = f.into_std().await;

                let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
                let cancel = CancellationToken::new();

                let _drop_cancel = cancel.clone().drop_guard();
                let state = TemplateState { cmd_tx, cancel };

                let rhai_engine = tpl.evaluator_mut();
                rhai_engine.set_default_tag(Dynamic::from(state.clone()));

                debug!("rendering template...");

                let res = block_in_place(|| {
                    let start = Instant::now();
                    let res = tpl.render(f, state);
                    debug!("rendered template in {:?}", start.elapsed());
                    res
                });

                if let Err(e) = res {
                    error!("could not render template '{src}': {e}");
                    break;
                }

                debug!("rendered template");

                tokio::fs::rename(&tmp_filepath, &dst).await?;

                debug!("wrote file");

                if let Some(ref args) = cmd {
                    let mut iter = args.iter();
                    if let Some(cmd) = iter.next() {
                        let mut cmd = tokio::process::Command::new(cmd);
                        for arg in iter {
                            cmd.arg(arg);
                        }

                        cmd.spawn()?.wait().await?;
                    }
                }

                if once {
                    break;
                }

                const DEBOUNCE_DEADLINE: Duration = Duration::from_millis(100);
                let mut deadline = None;

                loop {
                    enum Branch {
                        Recompile,
                        Render,
                    }

                    let branch = {
                        let deadline_check = async {
                            if let Some(sleep) = deadline.as_mut() {
                                sleep.await
                            } else {
                                futures::future::pending().await
                            }
                        };
                        tokio::select! {
                            Some(_) = notify_rx.recv() => Branch::Recompile,
                            Some(TemplateCommand::Render) = cmd_rx.recv() => Branch::Render,
                            _ = deadline_check => {
                                debug!("debounce deadline reached, re-rendering");
                                break;
                            },
                            else => {
                                warn!("template renderer is done");
                                break 'outer;
                            }
                        }
                    };

                    match branch {
                        Branch::Recompile => {
                            trace!("checking if we need to recompile the template at {src}");
                            let meta = tokio::fs::metadata(&src).await?;
                            let new_mtime = meta.modified()?;
                            trace!("new mtime: {new_mtime:?}");

                            if mtime != new_mtime {
                                mtime = new_mtime;
                                debug!("mtime changed, checksumming...");
                                let input = tokio::fs::read_to_string(&src).await?;
                                let new_checksum = crc32fast::hash(input.as_bytes());
                                if checksum != new_checksum {
                                    info!(
                                        "Template at {src} changed, re-compiling and re-rendering"
                                    );
                                    tpl = engine.compile_mut(&input)?;
                                    checksum = new_checksum;
                                    // break from inner loop
                                    break;
                                } else {
                                    debug!("checksum did not change");
                                }
                            } else {
                                trace!("mtime did not change");
                            }
                        }
                        Branch::Render => {
                            // debug!("re-rendering {src}");
                            if deadline.is_none() {
                                deadline = Some(Box::pin(tokio::time::sleep(DEBOUNCE_DEADLINE)));
                            }
                        }
                    }
                }
            }

            Ok::<_, eyre::Report>(src)
        });
    }

    tokio::spawn(async_watch(filepaths));

    while let Some(res) = futs.next().await {
        match res {
            Ok(path) => info!("done with template at {path}"),
            Err(e) => error!("could not process template: {e}"),
        }
    }

    Ok(())
}

fn async_watcher() -> notify::Result<(Debouncer<RecommendedWatcher>, Receiver<DebounceEventResult>)>
{
    let (tx, rx) = channel(1);

    // Automatically select the best implementation for your platform.
    // You can also access each implementation directly e.g. INotifySubscriptioner.
    let debouncer = new_debouncer(
        Duration::from_secs(1),
        None,
        move |res: DebounceEventResult| {
            if let Err(e) = tx.blocking_send(res) {
                error!("could not send file change notifications! {e}");
            }
        },
    )?;

    Ok((debouncer, rx))
}

async fn async_watch(paths: Vec<(Utf8PathBuf, Sender<()>)>) -> notify::Result<()> {
    let (mut debouncer, mut rx) = async_watcher()?;

    let mut map: HashMap<Utf8PathBuf, Vec<Sender<()>>> = HashMap::new();

    for (path, sender) in paths {
        match map.entry(path) {
            std::collections::hash_map::Entry::Occupied(mut senders) => {
                senders.get_mut().push(sender)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                debouncer
                    .watcher()
                    .watch(entry.key().as_std_path(), RecursiveMode::NonRecursive)?;
                entry.insert(vec![sender]);
            }
        }
    }

    while let Some(res) = rx.recv().await {
        match res {
            Ok(events) => {
                let changed_set = events.into_iter().fold(HashSet::new(), |mut set, event| {
                    set.insert(Utf8PathBuf::from(event.path.display().to_string()));
                    set
                });

                let mut paths_to_delete = vec![];

                for path in changed_set {
                    if let Some(senders) = map.get_mut(&path) {
                        let mut to_delete = vec![];
                        for (i, sender) in senders.iter().enumerate() {
                            if let Err(_e) = sender.send(()).await {
                                warn!("could not send template change notification for {path}");
                                to_delete.push(i);
                            }
                        }
                        for i in to_delete {
                            senders.remove(i);
                        }
                        if senders.is_empty() {
                            paths_to_delete.push(path);
                        }
                    }
                }

                for path in paths_to_delete {
                    map.remove(&path);
                    _ = debouncer.watcher().unwatch(path.as_std_path());
                }
            }
            Err(e) => error!("subscription error: {:?}", e),
        }
    }

    Ok(())
}
