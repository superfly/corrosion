use std::net::SocketAddr;

use camino::Utf8PathBuf;
use clap::Parser;
use corro_admin::AdminConfig;
use corro_types::{
    actor::ActorId,
    config::{Config, LogFormat},
};
use metrics_exporter_prometheus::PrometheusBuilder;
use spawn::wait_for_all_pending_handles;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    signal::unix::SignalKind,
};
use tracing::{debug, error, info};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // do this first!
    let mut hangup = tokio::signal::unix::signal(SignalKind::hangup())?;

    println!("Starting Corrosion v{VERSION}");
    let app = <App as clap::Parser>::parse();

    let config_path = app.config;

    println!("Using config file: {}", config_path);
    let config = Config::load(config_path.as_str()).expect("could not read config from file");

    let directives = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    println!("tracing-filter directives: {directives}");
    let (filter, diags) = tracing_filter::legacy::Filter::parse(&directives);
    if let Some(diags) = diags {
        eprintln!("While parsing env filters: {diags}");
    }

    // Tracing
    let (env_filter, _handle) = tracing_subscriber::reload::Layer::new(filter.layer());

    let sub = tracing_subscriber::registry::Registry::default().with(env_filter);

    match config.log_format {
        LogFormat::Plaintext => {
            sub.with(tracing_subscriber::fmt::Layer::new()).init();
        }
        LogFormat::Json => {
            sub.with(tracing_subscriber::fmt::Layer::new().json())
                .init();
        }
    }

    let config_parent_path: Utf8PathBuf = config_path
        .parent()
        .map(|p| p.into())
        .unwrap_or_else(|| "./".into());

    let mut actor_id_file = tokio::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(config_parent_path.join("actor_id"))
        .await?;

    let mut id_str = String::new();
    actor_id_file.read_to_string(&mut id_str).await?;

    let actor_id = ActorId(if id_str.is_empty() {
        let id = Uuid::new_v4();
        actor_id_file.write_all(id.to_string().as_bytes()).await?;
        id
    } else {
        id_str.parse()?
    });

    debug!("actor_id from file: {actor_id}");

    if let Some(metrics_addr) = config.metrics_addr {
        setup_prometheus(metrics_addr).expect("could not setup prometheus");
    }

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let agent = corrosion::agent::start(actor_id, config.clone(), tripwire.clone())
        .await
        .expect("could not start agent");

    tokio::spawn({
        let agent = agent.clone();
        let config_path = config_path.clone();
        let mut tripwire = tripwire.clone();
        async move {
            loop {
                tokio::select! {
                    _ = hangup.recv() => {},
                    _ = &mut tripwire => {
                        break;
                    }
                }
                info!("Reloading config from '{}'", config_path);

                let new_conf = match Config::load(config_path.as_str()) {
                    Ok(conf) => conf,
                    Err(e) => {
                        error!("could not load new config: {e}");
                        continue;
                    }
                };

                if let Err(e) = corro_types::agent::reload(&agent, new_conf).await {
                    error!("could not reload config: {e}");
                }
            }
        }
    });

    corro_admin::start_server(
        agent,
        AdminConfig {
            listen_path: config.admin_path,
            config_path,
        },
        tripwire,
    )?;

    tripwire_worker.await;

    wait_for_all_pending_handles().await;

    Ok(())
}

#[derive(Parser)]
#[clap(version = VERSION)]
pub(crate) struct App {
    /// Set the config file path
    #[clap(long, short, default_value = "corrosion.toml")]
    pub(crate) config: Utf8PathBuf,
}

fn setup_prometheus(addr: SocketAddr) -> eyre::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets(&[
            0.001, // 1ms
            0.005, // 5ms
            0.025, // 25ms
            0.050, // 50ms
            0.100, // 100ms
            0.200, // 200ms
            1.0,   // 1s
            5.0,   // 5s
            10.0,  // 10s :screaming:
            30.0, 60.0,
        ])?
        .install()?;
    Ok(())
}
