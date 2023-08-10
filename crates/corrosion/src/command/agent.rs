use std::net::SocketAddr;

use camino::Utf8PathBuf;
use corro_admin::AdminConfig;
use corro_types::config::Config;
use metrics_exporter_prometheus::PrometheusBuilder;
use spawn::wait_for_all_pending_handles;
use tracing::{error, info};

use crate::VERSION;

pub async fn run(config: Config, config_path: &Utf8PathBuf) -> eyre::Result<()> {
    info!("Starting Corrosion Agent v{VERSION}");

    if let Some(metrics_addr) = config.metrics_addr {
        setup_prometheus(metrics_addr).expect("could not setup prometheus");
    }

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let agent = corro_agent::agent::start(config.clone(), tripwire.clone())
        .await
        .expect("could not start agent");

    corro_admin::start_server(
        agent,
        AdminConfig {
            listen_path: config.admin_path.clone(),
            config_path: config_path.clone(),
        },
        tripwire,
    )?;

    if !config.schema_paths.is_empty() {
        let client = corro_client::CorrosionApiClient::new(config.api_addr);
        match client
            .schema_from_paths(config.schema_paths.as_slice())
            .await
        {
            Ok(res) => {
                info!("Applied schema in {:?}s", res.time);
            }
            Err(e) => {
                error!("could not apply schema: {e}");
            }
        }
    }

    tripwire_worker.await;

    wait_for_all_pending_handles().await;

    Ok(())
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
