use std::{net::SocketAddr, time::Duration};

use camino::Utf8PathBuf;
use corro_admin::AdminConfig;
use corro_types::config::{Config, PrometheusConfig};
use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use spawn::wait_for_all_pending_handles;
use tokio_metrics::RuntimeMonitor;
use tracing::{error, info};

use crate::VERSION;

pub async fn run(config: Config, config_path: &Utf8PathBuf) -> eyre::Result<()> {
    info!("Starting Corrosion Agent v{VERSION}");

    if let Some(PrometheusConfig { bind_addr }) = config.telemetry.prometheus {
        setup_prometheus(bind_addr).expect("could not setup prometheus");
        let info = crate::version();
        gauge!("corro.build.info", 1.0, "version" => info.crate_info.version.to_string(), "ts" => info.timestamp.to_string(), "rustc_version" => info.compiler.version.to_string());

        start_tokio_runtime_reporter();
    }

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let agent = corro_agent::agent::start(config.clone(), tripwire.clone())
        .await
        .expect("could not start agent");

    corro_admin::start_server(
        agent,
        AdminConfig {
            listen_path: config.admin.uds_path.clone(),
            config_path: config_path.clone(),
        },
        tripwire,
    )?;

    if !config.db.schema_paths.is_empty() {
        let client = corro_client::CorrosionApiClient::new(config.api.bind_addr);
        match client
            .schema_from_paths(config.db.schema_paths.as_slice())
            .await
        {
            Ok(Some(res)) => {
                info!("Applied schema in {}s", res.time);
            }
            Ok(None) => {
                info!("No schema files to apply, skipping.");
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
            2.0,   // 2s
            3.0,   // 3s
            4.0,   // 4s
            5.0,   // 5s
            10.0,  // 10s :screaming:
            30.0, 60.0,
        ])?
        .install()?;
    Ok(())
}

fn start_tokio_runtime_reporter() {
    let handle = tokio::runtime::Handle::current();

    {
        let runtime_monitor = RuntimeMonitor::new(&handle);
        tokio::spawn(async move {
            for metrics in runtime_monitor.intervals() {
                gauge!("corro.tokio.workers_count", metrics.workers_count as f64);
                gauge!(
                    "corro.tokio.total_park_count",
                    metrics.total_park_count as f64
                );
                gauge!("corro.tokio.max_park_count", metrics.max_park_count as f64);
                gauge!("corro.tokio.min_park_count", metrics.min_park_count as f64);
                gauge!(
                    "corro.tokio.total_noop_count",
                    metrics.total_noop_count as f64
                );
                gauge!("corro.tokio.max_noop_count", metrics.max_noop_count as f64);
                gauge!("corro.tokio.min_noop_count", metrics.min_noop_count as f64);
                gauge!(
                    "corro.tokio.total_steal_count",
                    metrics.total_steal_count as f64
                );
                gauge!(
                    "corro.tokio.max_steal_count",
                    metrics.max_steal_count as f64
                );
                gauge!(
                    "corro.tokio.min_steal_count",
                    metrics.min_steal_count as f64
                );
                gauge!(
                    "corro.tokio.total_steal_operations",
                    metrics.total_steal_operations as f64
                );
                gauge!(
                    "corro.tokio.max_steal_operations",
                    metrics.max_steal_operations as f64
                );
                gauge!(
                    "corro.tokio.min_steal_operations",
                    metrics.min_steal_operations as f64
                );
                gauge!(
                    "corro.tokio.num_remote_schedules",
                    metrics.num_remote_schedules as f64
                );
                gauge!(
                    "corro.tokio.total_local_schedule_count",
                    metrics.total_local_schedule_count as f64
                );
                gauge!(
                    "corro.tokio.max_local_schedule_count",
                    metrics.max_local_schedule_count as f64
                );
                gauge!(
                    "corro.tokio.min_local_schedule_count",
                    metrics.min_local_schedule_count as f64
                );
                gauge!(
                    "corro.tokio.total_overflow_count",
                    metrics.total_overflow_count as f64
                );
                gauge!(
                    "corro.tokio.max_overflow_count",
                    metrics.max_overflow_count as f64
                );
                gauge!(
                    "corro.tokio.min_overflow_count",
                    metrics.min_overflow_count as f64
                );
                gauge!(
                    "corro.tokio.total_polls_count",
                    metrics.total_polls_count as f64
                );
                gauge!(
                    "corro.tokio.max_polls_count",
                    metrics.max_polls_count as f64
                );
                gauge!(
                    "corro.tokio.min_polls_count",
                    metrics.min_polls_count as f64
                );
                gauge!(
                    "corro.tokio.total_busy_seconds",
                    metrics.total_busy_duration.as_secs_f64()
                );
                gauge!(
                    "corro.tokio.max_busy_seconds",
                    metrics.max_busy_duration.as_secs_f64()
                );
                gauge!(
                    "corro.tokio.min_busy_seconds",
                    metrics.min_busy_duration.as_secs_f64()
                );
                gauge!(
                    "corro.tokio.injection_queue_depth",
                    metrics.injection_queue_depth as f64
                );
                gauge!(
                    "corro.tokio.total_local_queue_depth",
                    metrics.total_local_queue_depth as f64
                );
                gauge!(
                    "corro.tokio.max_local_queue_depth",
                    metrics.max_local_queue_depth as f64
                );
                gauge!(
                    "corro.tokio.min_local_queue_depth",
                    metrics.min_local_queue_depth as f64
                );
                gauge!(
                    "corro.tokio.budget_forced_yield_count",
                    metrics.budget_forced_yield_count as f64
                );
                gauge!(
                    "corro.tokio.io_driver_ready_count",
                    metrics.io_driver_ready_count as f64
                );

                // wait 2s
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }
}
