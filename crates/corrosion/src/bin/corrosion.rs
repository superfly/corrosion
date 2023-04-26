use std::net::SocketAddr;

use camino::Utf8PathBuf;
use clap::Parser;
use corrosion::config::LogFormat;
use metrics_exporter_prometheus::PrometheusBuilder;
use spawn::wait_for_all_pending_handles;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    println!("Starting Corrosion v{VERSION}");
    let app = <App as clap::Parser>::parse();

    println!("Using config file: {}", app.config);
    let config = corrosion::config::Config::read_from_file_and_env(app.config.as_str())
        .expect("could not read config from file");

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

    if let Some(metrics_addr) = config.metrics_addr {
        setup_prometheus(metrics_addr).expect("could not setup prometheus");
    }

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let _agent = corrosion::agent::start(config.clone(), tripwire.clone())
        .await
        .expect("could not start agent");
    // spawn_counted(
    //     corro_admin::start(config.admin_path.into_std_path_buf(), agent, tripwire)
    //         .inspect(|_| info!("corrosion admin start is done")),
    // );

    tripwire_worker.await;

    wait_for_all_pending_handles().await;

    Ok(())
}

/// Proxies stuff!
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
