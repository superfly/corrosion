use std::{net::SocketAddr, time::Duration};

use antithesis_sdk::prelude::*;
use build_info::VersionControl;
use camino::Utf8PathBuf;
use corro_admin::{AdminConfig, TracingHandle};
use corro_types::{
    config::{Config, PrometheusConfig},
    gauge::PERSISTENT_GAUGE_REGISTRY,
    persistent_gauge,
};
use metrics::gauge;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use metrics_util::MetricKindMask;
use spawn::wait_for_all_pending_handles;
use tokio_metrics::RuntimeMonitor;
use tracing::{error, info};
// use tracing_filter::{legacy::Filter, FilterLayer};
// use tracing_subscriber::{reload::Handle, Registry};

use crate::VERSION;

pub async fn run(
    config: Config,
    config_path: &Utf8PathBuf,
    tracing_handle: Option<TracingHandle>,
) -> eyre::Result<()> {
    info!("Starting Corrosion Agent v{VERSION}");

    if let Some(PrometheusConfig { bind_addr }) = config.telemetry.prometheus {
        setup_prometheus(bind_addr).expect("could not setup prometheus");
        let info = crate::version().clone();

        // I know this is cloned a lot, but I don't care since it's called once
        // and then we won't be hearing about it ever again
        let unknown: String = "unknown".into();
        let (git_commit, git_branch) = if let Some(VersionControl::Git(git)) = info.version_control
        {
            (git.commit_short_id, git.branch.unwrap_or(unknown.clone()))
        } else {
            (unknown.clone(), unknown.clone())
        };

        persistent_gauge!(
            "corro.build.info",
            "version" => info.crate_info.version.to_string(),
            "ts" => info.timestamp.to_string(),
            "rustc_version" => info.compiler.version.to_string(),
            "git_commit" => git_commit.clone(),
            "git_branch" => git_branch.clone()
        )
        .set(1.0);

        // Ensure all persistent gauges get touched every 30s
        PERSISTENT_GAUGE_REGISTRY.clone().spawn_handle_update();

        start_tokio_runtime_reporter();
    }

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let (agent, bookie, _, handles) =
        corro_agent::agent::start_with_config(config.clone(), tripwire.clone())
            .await
            .expect("could not start agent");

    corro_admin::start_server(
        agent.clone(),
        bookie.clone(),
        AdminConfig {
            listen_path: config.admin.uds_path.clone(),
            config_path: config_path.clone(),
        },
        tracing_handle,
        tripwire,
    )?;

    if !config.db.schema_paths.is_empty() {
        let client = corro_client::CorrosionApiClient::new(*config.api.bind_addr.first().unwrap());
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

    antithesis_init();
    tripwire_worker.await;

    // wait for handles to finish
    for handle in handles {
        if let Err(e) = handle.await {
            error!("error from task handle: {e:?}");
        }
    }
    // wind down subs when handles are dropped
    agent.subs_manager().drop_handles().await;

    wait_for_all_pending_handles().await;

    Ok(())
}

fn setup_prometheus(addr: SocketAddr) -> eyre::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .idle_timeout(MetricKindMask::GAUGE, Some(Duration::from_secs(120)))
        .set_buckets_for_metric(
            Matcher::Suffix("chunk_size".into()),
            &[1.0, 10.0, 75.0, 250.0, 375.0, 500.0, 650.0],
        )?
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
                gauge!("corro.tokio.workers_count").set(metrics.workers_count as f64);
                gauge!("corro.tokio.total_park_count").set(metrics.total_park_count as f64);
                gauge!("corro.tokio.max_park_count").set(metrics.max_park_count as f64);
                gauge!("corro.tokio.min_park_count").set(metrics.min_park_count as f64);
                gauge!("corro.tokio.total_noop_count").set(metrics.total_noop_count as f64);
                gauge!("corro.tokio.max_noop_count").set(metrics.max_noop_count as f64);
                gauge!("corro.tokio.min_noop_count").set(metrics.min_noop_count as f64);
                gauge!("corro.tokio.total_steal_count").set(metrics.total_steal_count as f64);
                gauge!("corro.tokio.max_steal_count").set(metrics.max_steal_count as f64);
                gauge!("corro.tokio.min_steal_count").set(metrics.min_steal_count as f64);
                gauge!("corro.tokio.total_steal_operations")
                    .set(metrics.total_steal_operations as f64);
                gauge!("corro.tokio.max_steal_operations").set(metrics.max_steal_operations as f64);
                gauge!("corro.tokio.min_steal_operations").set(metrics.min_steal_operations as f64);
                gauge!("corro.tokio.num_remote_schedules").set(metrics.num_remote_schedules as f64);
                gauge!("corro.tokio.total_local_schedule_count")
                    .set(metrics.total_local_schedule_count as f64);
                gauge!("corro.tokio.max_local_schedule_count")
                    .set(metrics.max_local_schedule_count as f64);
                gauge!("corro.tokio.min_local_schedule_count")
                    .set(metrics.min_local_schedule_count as f64);
                gauge!("corro.tokio.total_overflow_count").set(metrics.total_overflow_count as f64);
                gauge!("corro.tokio.max_overflow_count").set(metrics.max_overflow_count as f64);
                gauge!("corro.tokio.min_overflow_count").set(metrics.min_overflow_count as f64);
                gauge!("corro.tokio.total_polls_count").set(metrics.total_polls_count as f64);
                gauge!("corro.tokio.max_polls_count").set(metrics.max_polls_count as f64);
                gauge!("corro.tokio.min_polls_count").set(metrics.min_polls_count as f64);
                gauge!("corro.tokio.total_busy_seconds")
                    .set(metrics.total_busy_duration.as_secs_f64());
                gauge!("corro.tokio.max_busy_seconds").set(metrics.max_busy_duration.as_secs_f64());
                gauge!("corro.tokio.min_busy_seconds").set(metrics.min_busy_duration.as_secs_f64());
                gauge!("corro.tokio.injection_queue_depth")
                    .set(metrics.injection_queue_depth as f64);
                gauge!("corro.tokio.total_local_queue_depth")
                    .set(metrics.total_local_queue_depth as f64);
                gauge!("corro.tokio.max_local_queue_depth")
                    .set(metrics.max_local_queue_depth as f64);
                gauge!("corro.tokio.min_local_queue_depth")
                    .set(metrics.min_local_queue_depth as f64);
                gauge!("corro.tokio.budget_forced_yield_count")
                    .set(metrics.budget_forced_yield_count as f64);
                gauge!("corro.tokio.io_driver_ready_count")
                    .set(metrics.io_driver_ready_count as f64);

                // wait 2s
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }
}
