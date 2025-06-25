use antithesis_sdk::random;
use clap::Parser;
use corro_api_types::{sqlite::ChangeType, Statement, TypedQueryEvent};
use corro_client::CorrosionClient;
use eyre::{eyre, Result};
use futures::StreamExt;
use hickory_resolver::AsyncResolver;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};
use tripwire::Tripwire;

/// Subscibes to a set of corrosion nodes and continously queries them!
#[derive(Parser)]
pub(crate) struct App {
    /// Set the config file path
    #[clap(long, short, default_value = "fly.toml")]
    pub(crate) config: PathBuf,

    pub(crate) corrosion_addr: Vec<String>,
}

const DEPLOY_STATS_QUERY: &str = r#"
SELECT
    t.name as team_name,
    t.state as team_state,
    COUNT(DISTINCT d.id) as active_deployments,
    COUNT(DISTINCT cs.id) as active_services,
    MAX(COALESCE(d.updated_at, cs.updated_at)) as last_deployment_update
FROM teams t
LEFT JOIN deployments d ON d.team_id = t.id
LEFT JOIN consul_services cs ON cs.team_id = t.id
GROUP BY t.id
"#;

const TEAM_QUERY: &str = r#"
    SELECT t.id, t.name, COUNT(distinct u.id) as active_users FROM teams t
        LEFT JOIN users u ON u.team_id = t.id
    WHERE t.name like ?
    GROUP BY t.id
"#;

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
struct TeamStats {
    id: i64,
    name: String,
    active_users: i64,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
struct DeploymentStats {
    team_name: String,
    team_state: String,
    active_deployments: i64,
    active_services: i64,
    last_deployment_update: Option<f64>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    let app = <App as clap::Parser>::parse();

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();
    tracing_subscriber::fmt::init();

    // todo: Pass CorrosionClient to all functions instead of addresses
    subscribe_all(app.corrosion_addr.clone(), tripwire.clone());

    query_all(app.corrosion_addr.clone(), tripwire.clone());

    table_updates_all(app.corrosion_addr, tripwire.clone());

    println!("tripwire_worker.await");
    tripwire_worker.await;
    Ok(())
}

fn subscribe_all(corrosion_addrs: Vec<String>, tripwire: Tripwire) {
    for addr in corrosion_addrs {
        let new_tripwire: Tripwire = tripwire.clone();
        tokio::spawn(async move {
            if let Err(e) = subscribe(addr, new_tripwire).await {
                error!("failed to subscribe: {e}");
            }
        });
    }
}

fn query_all(corrosion_addrs: Vec<String>, tripwire: Tripwire) {
    for addr in corrosion_addrs {
        let new_tripwire: Tripwire = tripwire.clone();
        tokio::spawn(async move {
            if let Err(e) = query(addr, new_tripwire).await {
                error!("failed to query: {e}");
            }
        });
    }
}

fn table_updates_all(corrosion_addrs: Vec<String>, tripwire: Tripwire) {
    for addr in corrosion_addrs {
        let new_tripwire: Tripwire = tripwire.clone();
        tokio::spawn(async move {
            if let Err(e) = table_updates(addr, new_tripwire).await {
                error!("failed to table updates: {e}");
            }
        });
    }
}

async fn table_updates(addr: String, tripwire: Tripwire) -> eyre::Result<()> {
    info!("starting table updates for {addr}");
    let client = {
        loop {
            match create_client(&addr).await {
                Ok(client) => break client,
                Err(e) => {
                    error!("could not create corrosion client: {e}");
                    sleep(Duration::from_secs(60)).await;
                }
            }
        }
    };

    // todo: subscribe to updates for each table
    let tables = vec!["users", "teams", "deployments"];
    // let tables = vec!["todos"];
    for table in tables {
        let client = client.clone();
        let tripwire = tripwire.clone();
        tokio::spawn(async move {
            loop {
                if tripwire.is_shutting_down() {
                    info!("tripped! Shutting down");
                    break;
                }

                info!("subscribing to updates for table: {table}");
                match client.updates(table).await {
                    Ok(mut stream) => {
                        info!("subscribed to updates for table: {table}");
                        while let Some(update) = stream.next().await {
                            // todo: check if the update is right e.g row should be absent on delete
                            // but present on upserts.
                            debug!("update: {update:?}");
                        }
                    }
                    Err(e) => {
                        error!("failed to subscribe to updates: {e}");
                    }
                }
                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    Ok(())
}

async fn query(addr: String, tripwire: Tripwire) -> eyre::Result<()> {
    let mut corro_client = None;
    loop {
        if tripwire.is_shutting_down() {
            info!("tripped! Shutting down");
            break;
        }

        if corro_client.is_none() {
            match create_client(&addr).await {
                Ok(client) => {
                    corro_client = Some(client);
                }
                Err(e) => {
                    error!("could not create corrosion client: {e}");
                    sleep(Duration::from_secs(60)).await;
                    continue;
                }
            }
        }

        let client: &CorrosionClient = corro_client.as_ref().unwrap();
        // flood corrosion with queries
        // TODO: limit the number of task we are spawning
        let num_range = (1..10).collect::<Vec<_>>();
        let loop_count = random::random_choice(&num_range).cloned().unwrap_or(500);
        for _ in 0..loop_count {
            let client = client.clone();
            tokio::spawn(async move {
                let start = Instant::now();
                let letters = "abcdefghijkl";
                for letter in letters.chars() {
                    match client
                        .query_typed::<TeamStats>(
                            &Statement::WithParams(
                                TEAM_QUERY.into(),
                                vec![format!("%{}%", letter).into()],
                            ),
                            None,
                        )
                        .await
                    {
                        Ok(mut res) => {
                            while let Some(row) = res.next().await {
                                debug!("team stats: {row:?}");
                                // simulate a slow client
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                        Err(e) => {
                            error!("query error: {e}");
                        }
                    }
                }

                info!("query took: {:?}", start.elapsed());
            });
        }

        let num_range = (1..10).collect::<Vec<_>>();
        let random_timeout = random::random_choice(&num_range).unwrap_or(&2);
        sleep(Duration::from_secs(random_timeout * 60)).await;
    }
    Ok(())
}

async fn subscribe(addr: String, mut tripwire: Tripwire) -> eyre::Result<()> {
    let mut last_change_id = None;
    let mut sub_id = None;
    let mut corro_client = None;
    loop {
        if tripwire.is_shutting_down() {
            info!("tripped! Shutting down");
            break;
        }

        if corro_client.is_none() {
            match create_client(&addr).await {
                Ok(client) => {
                    corro_client = Some(client);
                }
                Err(e) => {
                    error!("could not create corrosion client: {e}");
                    sleep(Duration::from_secs(60)).await;
                    continue;
                }
            }
        }

        let client: &CorrosionClient = corro_client.as_ref().unwrap();
        let mut sub: corro_client::sub::SubscriptionStream<DeploymentStats> =
            match if let Some(sub_id) = sub_id {
                client
                    .subscription_typed::<DeploymentStats>(sub_id, true, last_change_id)
                    .await
            } else {
                client
                    .subscribe_typed::<DeploymentStats>(
                        &Statement::WithParams(DEPLOY_STATS_QUERY.into(), vec![]),
                        true,
                        None,
                    )
                    .await
            } {
                Ok(sub) => {
                    let id = sub.id();
                    info!("Subscribed w/ id {id}");
                    sub_id = Some(id);
                    sub
                }
                Err(e) => {
                    error!("could not subscribe: {e}");
                    if !matches!(
                        e,
                        corro_client::Error::Hyper(_)
                            | corro_client::Error::Http(_)
                            | corro_client::Error::InvalidUri(_)
                    ) {
                        // not a transient error, reset the subscription state
                        last_change_id = None;
                        sub_id = None;
                    }
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

        loop {
            let msg_res = tokio::select! {
                msg_res = sub.next() => match msg_res {
                    Some(msg_res) => msg_res,
                    None => break,
                },
                _ = &mut tripwire => {
                    info!("tripwire");
                    break;
                },
            };

            match msg_res {
                Ok(TypedQueryEvent::Change(change_kind, _, stats, change_id)) => {
                    last_change_id = Some(change_id);
                    if matches!(change_kind, ChangeType::Update | ChangeType::Insert) {
                        // update some internal state that we can use to check later
                        debug!("deployment stats: {stats:?}");
                    }
                }
                Ok(TypedQueryEvent::Error(e)) => {
                    error!("subscription error: {e}");
                    sub_id = None;
                    last_change_id = None;
                    break;
                }
                Ok(TypedQueryEvent::Row(_, stats)) => {
                    debug!("deployment stats: {stats:?}");
                }
                Ok(_) => {}
                Err(e) => {
                    error!("subscription client error: {e}");
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn create_client(addr: &str) -> Result<CorrosionClient> {
    let mut host_port = addr.split(':');
    let host_name = host_port.next().unwrap_or("localhost");

    let system_resolver = AsyncResolver::tokio_from_system_conf()?;
    match system_resolver.lookup_ip(host_name).await?.iter().next() {
        Some(ip_addr) => {
            let addr =
                SocketAddr::from((ip_addr, host_port.next().unwrap_or("8080").parse().unwrap()));
            let corro_client = CorrosionClient::new(addr, "/var/lib/corrosion2/state.db");
            Ok(corro_client)
        }
        None => Err(eyre!("could not resolve ip address for {}", addr)),
    }
}
