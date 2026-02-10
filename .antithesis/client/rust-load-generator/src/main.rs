mod storage;

use antithesis_sdk::random;
use clap::Parser;
use config::{Config, File};
use corro_api_types::{SqliteValue, Statement, TypedQueryEvent};
use corro_client::CorrosionClient;
use eyre::{eyre, Result};
use futures::StreamExt;
use hickory_resolver::Resolver;
use serde::{Deserialize, Serialize};
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
    #[clap(long, short, default_value = "/etc/load-gen/config.toml")]
    config: String,
    corrosion_addr: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AppConfig {
    servers: Vec<CorrosionConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CorrosionConfig {
    addr: String,
    db_path: PathBuf,
}

impl CorrosionConfig {
    async fn create_client(&self) -> Result<CorrosionClient> {
        let mut host_port = self.addr.split(':');
        let host_name = host_port.next().unwrap_or("localhost");

        let system_resolver = Resolver::builder_tokio()?.build();
        match system_resolver.lookup_ip(host_name).await?.iter().next() {
            Some(ip_addr) => {
                let addr = SocketAddr::from((
                    ip_addr,
                    host_port.next().unwrap_or("8080").parse().unwrap(),
                ));
                Ok(CorrosionClient::new(addr, self.db_path.clone())?)
            }
            None => Err(eyre!("could not resolve ip address for {}", self.addr)),
        }
    }
}

const DEPLOY_STATS_QUERY: &str = r#"
SELECT
    t.name as team_name,
    t.state as team_state,
    d.id as deployment,
    cs.id as service,
    MAX(d.updated_at, cs.updated_at) as last_deployment_update
FROM teams t
LEFT JOIN deployments d ON d.team_id = t.id
LEFT JOIN consul_services cs ON cs.team_id = t.id
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

    let config: AppConfig = Config::builder()
        .add_source(File::with_name(&app.config))
        .build()?
        .try_deserialize()?;

    let (tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();
    tracing_subscriber::fmt::init();

    for cfg in config.servers {
        spawn_all_tasks(cfg.clone(), tripwire.clone());
    }

    info!("tripwire_worker.await");
    tripwire_worker.await;
    Ok(())
}

fn spawn_all_tasks(cfg: CorrosionConfig, tripwire: Tripwire) {
    let cfg_clone = cfg.clone();
    let tripwire_clone = tripwire.clone();
    tokio::spawn(async move {
        if let Err(e) = subscribe(cfg_clone, tripwire_clone).await {
            error!("failed to subscribe: {e}");
        }
    });

    let cfg_clone = cfg.clone();
    let tripwire_clone = tripwire.clone();
    tokio::spawn(async move {
        if let Err(e) = query(cfg_clone, tripwire_clone).await {
            error!("failed to query: {e}");
        }
    });

    tokio::spawn(async move {
        if let Err(e) = table_updates(cfg, tripwire).await {
            error!("failed to table updates: {e}");
        }
    });
}

async fn table_updates(cfg: CorrosionConfig, tripwire: Tripwire) -> eyre::Result<()> {
    info!("starting table updates for {}", cfg.addr);
    let client = {
        loop {
            match cfg.create_client().await {
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

async fn query(cfg: CorrosionConfig, tripwire: Tripwire) -> eyre::Result<()> {
    let mut corro_client = None;
    loop {
        if tripwire.is_shutting_down() {
            info!("tripped! Shutting down");
            break;
        }

        if corro_client.is_none() {
            match cfg.create_client().await {
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
                                vec![format!("%{letter}%").into()],
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

async fn subscribe(cfg: CorrosionConfig, mut tripwire: Tripwire) -> eyre::Result<()> {
    let mut last_change_id = None;
    let mut corro_client: Option<CorrosionClient> = None;
    let mut storage: Option<storage::SubscriptionDb> = None;

    loop {
        if tripwire.is_shutting_down() {
            info!("tripped! Shutting down");
            break;
        }

        if corro_client.is_none() {
            match cfg.create_client().await {
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

        // Use untyped subscription to get raw SqliteValue events
        let mut sub: corro_client::sub::SubscriptionStream<Vec<SqliteValue>> = match client
            .subscribe_typed::<Vec<SqliteValue>>(
                &Statement::WithParams(DEPLOY_STATS_QUERY.into(), vec![]),
                false,
                last_change_id,
            )
            .await
        {
            Ok(sub) => {
                let id = sub.id();
                info!("Subscribed w/ id {id}");

                // Initialize storage for this subscription (columns will be set when we receive them)
                let subs_db_path = cfg
                    .db_path
                    .parent()
                    .unwrap()
                    .join(format!("load-gen/{id}.db"));

                if let Some(path) = subs_db_path.parent() {
                    info!("creating directory: {path:?}");
                    tokio::fs::create_dir_all(path).await?;
                }
                if storage.is_none() {
                    let st = storage::SubscriptionDb::new(
                        &subs_db_path,
                        id,
                        DEPLOY_STATS_QUERY,
                        "query",
                    )?;
                    storage = Some(st);
                }
                sub
            }
            Err(e) => {
                error!("could not subscribe: {e}");
                if !matches!(
                    e,
                    corro_client::Error::Reqwest(_)
                        | corro_client::Error::Http(_)
                        | corro_client::Error::InvalidUri(_)
                ) {
                    last_change_id = None;
                    storage = None;
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

            let storage_ref = storage.as_mut().unwrap();
            match msg_res {
                Ok(TypedQueryEvent::Columns(columns)) => {
                    info!("Received columns: {:?}", columns);
                    if let Err(e) = storage_ref.initialize_columns(columns) {
                        error!("Failed to initialize columns: {e}");
                    }
                }
                Ok(TypedQueryEvent::Row(row_id, values)) => {
                    debug!("Received row: row_id={}, values={:?}", row_id.0, values);
                    if let Err(e) = storage.as_mut().unwrap().insert_row(row_id, &values) {
                        error!("Failed to insert row: {e}");
                    }
                }
                Ok(TypedQueryEvent::Change(change_type, row_id, values, change_id)) => {
                    last_change_id = Some(change_id);
                    debug!(
                        "Received change: type={:?}, row_id={}, change_id={}",
                        change_type, row_id.0, change_id.0
                    );

                    if let Err(e) =
                        storage
                            .as_mut()
                            .unwrap()
                            .handle_change(change_type, row_id, &values)
                    {
                        error!("Failed to handle change: {e}");
                    }
                }
                Ok(TypedQueryEvent::EndOfQuery { change_id, .. }) => {
                    if let Some(change_id) = change_id {
                        last_change_id = Some(change_id);
                        info!("End of query, last change_id: {}", change_id.0);
                    }
                }
                Ok(TypedQueryEvent::Error(e)) => {
                    error!("subscription error: {e}");
                    last_change_id = None;
                    storage = None;
                    break;
                }
                Err(e) => {
                    error!("subscription client error: {e}");
                    break;
                }
            }
        }
    }

    Ok(())
}
