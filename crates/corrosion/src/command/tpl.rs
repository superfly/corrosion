use std::net::SocketAddr;

use camino::Utf8PathBuf;
use corro_client::CorrosionApiClient;
use futures::{stream::FuturesUnordered, StreamExt};
use rhai_tpl::{TemplateCommand, TemplateWriter};
use tokio::{sync::mpsc, task::block_in_place};
use uuid::Uuid;

pub async fn run(api_addr: SocketAddr, template: &Vec<String>) -> eyre::Result<()> {
    _ = tracing_subscriber::fmt::fmt().try_init();
    let client = CorrosionApiClient::new(api_addr);
    let engine = rhai_tpl::Engine::new(client.clone());

    println!("template run");

    let mut futs = FuturesUnordered::new();

    for tpl in template {
        let mut splitted = tpl.splitn(3, ':');
        let src: Utf8PathBuf = splitted
            .next()
            .ok_or_else(|| eyre::eyre!("missing source template"))?
            .into();

        match tokio::fs::metadata(&src).await {
            Ok(meta) => {
                if meta.is_dir() {
                    eyre::bail!("source path should be a file, not a directory");
                }
            }
            Err(e) => return Err(e.into()),
        }

        let dst: Utf8PathBuf = splitted
            .next()
            .ok_or_else(|| eyre::eyre!("missing destination file"))?
            .into();

        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // rejoin
        let cmd = splitted.next().map(|s| shellwords::split(s)).transpose()?;

        println!("src: {src}, dst: {dst}, cmd: {cmd:?}");

        let input = tokio::fs::read_to_string(&src).await?;

        let dir = tempfile::tempdir()?;

        let tpl = engine.compile(&input)?;

        futs.push(async move {
            let tmp_filepath = dir.path().join(Uuid::new_v4().as_simple().to_string());
            loop {
                let f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp_filepath)
                    .await?;
                let (tx, mut rx) = mpsc::channel(1);
                let w = TemplateWriter::new(f.into_std().await, tx);

                let res = block_in_place(|| tpl.render(w));

                if let Err(e) = res {
                    // TODO: tracing crate? I think so!
                    eprintln!("[ERROR] could not render template '{src}': {e}");
                    break;
                }

                tokio::fs::rename(&tmp_filepath, &dst).await?;

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

                match rx.recv().await {
                    None => {
                        println!("template renderer is done");
                        break;
                    }
                    Some(TemplateCommand::Render) => {
                        println!("re-rendering {src}");
                    }
                }
            }

            Ok::<_, eyre::Report>(())
        });
    }

    while let Some(res) = futs.next().await {
        println!("got a res: {res:?}");
    }

    Ok(())
}
