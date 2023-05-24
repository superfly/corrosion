use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::SocketAddr,
    time::{Duration, Instant, SystemTime},
};

use bb8_rusqlite::RusqliteConnectionManager;
use camino::Utf8PathBuf;
use clap::Parser;
use consul::{AgentCheck, AgentService, Client};
use corro_types::api::{RqliteResponse, Statement};
use hyper::{client::HttpConnector, Body};
use metrics::{histogram, increment_counter};
use serde::{Deserialize, Serialize};
use spawn::wait_for_all_pending_handles;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, error, info, trace};

const CONSUL_PULL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    corrosion: CorrosionConfig,
    consul: consul::Config,
}

impl Config {
    /// Reads configuration from a TOML file, given its path. Environment
    /// variables can override whatever is set in the config file.
    pub fn read_from_file_and_env(config_path: &str) -> eyre::Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::new(config_path, config::FileFormat::Toml))
            .add_source(config::Environment::default().separator("__"))
            .build()?;
        Ok(config.try_deserialize()?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CorrosionConfig {
    api_addr: SocketAddr,
    base_path: Utf8PathBuf,
}

#[derive(Clone)]
pub struct CorrosionClient {
    api_addr: SocketAddr,
    pool: bb8::Pool<bb8_rusqlite::RusqliteConnectionManager>,
    api_client: hyper::Client<HttpConnector, Body>,
}

impl CorrosionClient {
    pub fn new(config: &CorrosionConfig) -> Self {
        Self {
            api_addr: config.api_addr,
            pool: bb8::Pool::builder()
                .max_size(5)
                .max_lifetime(Some(Duration::from_secs(30)))
                .build_unchecked(RusqliteConnectionManager::new(
                    config.base_path.join("state").join("state.sqlite"),
                )),
            api_client: hyper::Client::builder().http2_only(true).build_http(),
        }
    }

    pub fn pool(&self) -> &bb8::Pool<RusqliteConnectionManager> {
        &self.pool
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> eyre::Result<RqliteResponse> {
        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(format!("http://{}/db/execute?transaction", self.api_addr))
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::ACCEPT, "application/json")
            .body(Body::from(serde_json::to_vec(&statements)?))?;

        let res = self.api_client.request(req).await?;

        if !res.status().is_success() {
            return Err(eyre::eyre!("bad response code {}", res.status()));
        }

        let bytes = hyper::body::to_bytes(res.into_body()).await?;

        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[derive(Parser)]
#[clap(version = "0.1.0")]
pub(crate) struct App {
    /// Set the config file path
    #[clap(long, short, default_value = "corro-consul.toml")]
    pub(crate) config: Utf8PathBuf,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let app = <App as clap::Parser>::parse();

    println!("Using config file: {}", app.config);

    let config = Config::read_from_file_and_env(app.config.as_str())
        .expect("could not read config from file");

    tracing_subscriber::fmt::init();

    let (mut tripwire, tripwire_worker) = tripwire::Tripwire::new_signals();

    let node: &'static str = Box::leak(
        hostname::get()?
            .into_string()
            .expect("could not convert hostname to string")
            .into_boxed_str(),
    );

    let corrosion = CorrosionClient::new(&config.corrosion);
    let consul = consul::Client::new(config.consul)?;

    setup(&corrosion).await?;

    let mut consul_services: HashMap<String, u64> = HashMap::new();
    let mut consul_checks: HashMap<String, u64> = HashMap::new();

    {
        let conn = corrosion.pool().get().await?;

        let mut prepped = conn.prepare("SELECT id, hash FROM __corro_consul_services")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => {
                    break;
                }
            };

            consul_services.insert(row.get(0)?, u64::from_be_bytes(row.get(1)?));
        }

        let mut prepped = conn.prepare("SELECT id, hash FROM __corro_consul_checks")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => {
                    break;
                }
            };

            consul_checks.insert(row.get::<_, String>(1)?, u64::from_be_bytes(row.get(1)?));
        }
    }

    let mut pull_interval = interval(CONSUL_PULL_INTERVAL);

    loop {
        tokio::select! {
            _ = pull_interval.tick() => {
                let results = update_consul(&consul, node, &corrosion, &mut consul_services, &mut consul_checks, false).await;
                debug!("got results: {results:?}");
                for (kind, res) in results {
                    match res {
                        Ok(stats) if stats.upserted > 0 || stats.deleted > 0 => {
                            info!("updated consul {kind}: {stats:?}");
                        },
                        Err(e) => {
                            error!("could not update consul {kind}: {e}");
                        },
                        _ => {
                            debug!("nothing to update");
                        }
                    }
                }
            },
            _ = &mut tripwire => {
                debug!("tripped consul loop");
                break;
            }
        }
    }

    tripwire_worker.await;

    wait_for_all_pending_handles().await;

    Ok(())
}

async fn setup(corrosion: &CorrosionClient) -> eyre::Result<()> {
    let mut conn = corrosion.pool().get().await?;

    let tx = conn.transaction()?;

    tx.execute_batch(
        "
            CREATE TABLE IF NOT EXISTS __corro_consul_services (
                id TEXT NOT NULL PRIMARY KEY,
                hash BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS __corro_consul_checks (
                id TEXT NOT NULL PRIMARY KEY,
                hash BLOB NOT NULL
            );
            ",
    )?;

    tx.commit()?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ConsulCheckNotesDirectives {
    hash_include: Vec<ConsulCheckField>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConsulCheckField {
    Status,
    Output,
}

#[derive(Debug, Default)]
pub struct ApplyStats {
    pub upserted: usize,
    pub deleted: usize,
}

pub fn hash_service(svc: &AgentService) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    svc.hash(&mut hasher);
    hasher.finish()
}

pub fn hash_check(check: &AgentCheck) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    hasher.write(check.service_name.as_bytes());
    hasher.write(check.service_id.as_bytes());
    if let Some(notes) = check
        .notes
        .as_ref()
        .and_then(|notes| serde_json::from_str::<ConsulCheckNotesDirectives>(notes).ok())
    {
        for field in notes.hash_include {
            match field {
                ConsulCheckField::Status => {
                    trace!("hashing status: '{}'", check.status.as_str());
                    hasher.write(check.status.as_str().as_bytes());
                }
                ConsulCheckField::Output => {
                    trace!("hashing output: '{}'", check.output);
                    hasher.write(check.output.as_bytes());
                }
            }
        }
    } else {
        trace!("no special notes");
        hasher.write(check.status.as_str().as_bytes());
    }
    hasher.finish()
}

pub fn upsert_service_statements(
    statements: &mut Vec<Statement>,
    node: &'static str,
    svc: AgentService,
    hash: u64,
    updated_at: i64,
) -> eyre::Result<()> {
    // run this by corrosion so it's part of the same transaction
    statements.push(Statement::WithParams(vec![
        "INSERT INTO __corro_consul_services ( id, hash )
            VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET
                hash = excluded.hash;"
            .into(),
        svc.id.clone().into(),
        hash.to_be_bytes().to_vec().into(),
    ]));

    // upsert!
    statements.push(Statement::WithParams(vec![
        "INSERT INTO consul_services ( node, id, name, tags, meta, port, address, updated_at )
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(node, id) DO UPDATE SET
            name = excluded.name,
            tags = excluded.tags,
            meta = excluded.meta,
            port = excluded.port,
            address = excluded.address,
            updated_at = excluded.updated_at;"
            .into(),
        node.into(),
        svc.id.into(),
        svc.name.into(),
        serde_json::to_string(&svc.tags)?.into(),
        serde_json::to_string(&svc.meta)?.into(),
        svc.port.into(),
        svc.address.into(),
        updated_at.into(),
    ]));

    Ok(())
}

pub fn upsert_check_statements(
    statements: &mut Vec<Statement>,
    node: &'static str,
    check: AgentCheck,
    hash: u64,
    updated_at: i64,
) -> eyre::Result<()> {
    // run this by corrosion so it's part of the same transaction
    statements.push(Statement::WithParams(vec![
        "INSERT INTO __corro_consul_checks ( id, hash )
            VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET
                hash = excluded.hash;"
            .into(),
        check.id.clone().into(),
        hash.to_be_bytes().to_vec().into(),
    ]));

    // upsert!
    statements.push(Statement::WithParams(vec![
        "INSERT INTO consul_checks ( node, id, service_id, service_name, name, status, output, updated_at )
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(node, id) DO UPDATE SET
            service_id = excluded.service_id,
            service_name = excluded.service_name,
            name = excluded.name,
            status = excluded.status,
            output = excluded.output,
            updated_at = excluded.updated_at;"
            .into(),
        node.into(),
        check.id.into(),
        check.service_id.into(),
        check.service_name.into(),
        check.name.into(),
        check.status.as_str().into(),
        check.output.into(),
        updated_at.into(),
    ]));

    Ok(())
}

pub async fn update_consul_services(
    corrosion: &CorrosionClient,
    mut services: HashMap<String, AgentService>,
    hashes: &mut HashMap<String, u64>,
    node: &'static str,
    skip_hash_check: bool,
) -> eyre::Result<ApplyStats> {
    let updated_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("could not get system time")
        .as_millis() as i64;

    let mut to_upsert = vec![];
    let mut to_delete = vec![];

    let mut statements = vec![];

    let mut stats = ApplyStats::default();

    {
        for (id, old_hash) in hashes.iter() {
            if let Some(svc) = services.remove(id) {
                let hash = hash_service(&svc);
                if skip_hash_check || *old_hash != hash {
                    info!("upserting service '{id}'");

                    to_upsert.push((svc.id.clone(), hash));

                    upsert_service_statements(&mut statements, node, svc, hash, updated_at)?;
                    stats.upserted += 1;
                }
            } else {
                info!("deleting service: {id}");
                to_delete.push(id.clone());
            }
        }
    }

    // new services
    for (id, svc) in services {
        info!("upserting service '{id}'");

        let hash = hash_service(&svc);
        to_upsert.push((svc.id.clone(), hash));
        upsert_service_statements(&mut statements, node, svc, hash, updated_at)?;
        stats.upserted += 1;
    }

    for id in to_delete.iter() {
        statements.push(Statement::WithParams(vec![
            "DELETE FROM __corro_consul_services WHERE id = ?;".into(),
            (*id).clone().into(),
        ]));
        statements.push(Statement::WithParams(vec![
            "DELETE FROM consul_services WHERE node = ? AND id = ?;".into(),
            node.into(),
            (*id).clone().into(),
        ]));
        stats.deleted += 1;
    }

    if !statements.is_empty() {
        corrosion.execute(statements).await?;
        info!("updated consul services");
    }

    for (id, hash) in to_upsert {
        hashes.insert(id, hash);
    }

    for id in to_delete {
        hashes.remove(id.as_str());
    }

    Ok(stats)
}

pub async fn update_consul_checks(
    corrosion: &CorrosionClient,
    mut checks: HashMap<String, AgentCheck>,
    hashes: &mut HashMap<String, u64>,
    node: &'static str,
    skip_hash_check: bool,
) -> eyre::Result<ApplyStats> {
    let updated_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("could not get system time")
        .as_millis() as i64;

    let mut to_upsert = vec![];
    let mut to_delete = vec![];

    let mut statements = vec![];

    let mut stats = ApplyStats::default();

    {
        for (id, old_hash) in hashes.iter() {
            if let Some(check) = checks.remove(id) {
                let hash = hash_check(&check);
                if skip_hash_check || *old_hash != hash {
                    info!("upserting check '{id}'");

                    to_upsert.push((check.id.clone(), hash));

                    upsert_check_statements(&mut statements, node, check, hash, updated_at)?;
                    stats.upserted += 1;
                }
            } else {
                info!("deleting check: {id}");
                to_delete.push(id.clone());
            }
        }
    }

    // new checks
    for (id, check) in checks {
        info!("upserting check '{id}'");

        let hash = hash_check(&check);
        to_upsert.push((check.id.clone(), hash));
        upsert_check_statements(&mut statements, node, check, hash, updated_at)?;
        stats.upserted += 1;
    }

    for id in to_delete.iter() {
        statements.push(Statement::WithParams(vec![
            "DELETE FROM __corro_consul_checks WHERE id = ?;".into(),
            (*id).clone().into(),
        ]));
        statements.push(Statement::WithParams(vec![
            "DELETE FROM consul_checks WHERE node = ? AND id = ?;".into(),
            node.into(),
            (*id).clone().into(),
        ]));
        stats.deleted += 1;
    }

    if !statements.is_empty() {
        corrosion.execute(statements).await?;
        info!("updated consul checks");
    }

    for (id, hash) in to_upsert {
        hashes.insert(id, hash);
    }

    for id in to_delete {
        hashes.remove(id.as_str());
    }

    Ok(stats)
}

pub async fn update_consul(
    consul: &Client,
    node: &'static str,
    corrosion: &CorrosionClient,
    service_hashes: &mut HashMap<String, u64>,
    check_hashes: &mut HashMap<String, u64>,
    skip_hash_check: bool,
) -> [(&'static str, eyre::Result<ApplyStats>); 2] {
    let fut_services = async move {
        let start = Instant::now();
        (
            "services",
            match timeout(Duration::from_secs(5), consul.agent_services()).await {
                Ok(Ok(services)) => {
                    histogram!(
                        "corro_consul.consul.response.time.seconds",
                        start.elapsed().as_secs_f64()
                    );
                    match update_consul_services(
                        &corrosion,
                        services,
                        service_hashes,
                        node,
                        skip_hash_check,
                    )
                    .await
                    {
                        Ok(stats) => Ok(stats),
                        Err(e) => Err(e),
                    }
                }
                Ok(Err(e)) => {
                    increment_counter!("corro_consul.consul.response.errors", "error" => e.to_string(), "type" => "services");
                    Err(e.into())
                }
                Err(e) => {
                    increment_counter!("corro_consul.consul.response.errors", "error" => "timed out", "type" => "services");
                    Err(e.into())
                }
            },
        )
    };

    let fut_checks = async move {
        let start = Instant::now();
        (
            "checks",
            match timeout(Duration::from_secs(5), consul.agent_checks()).await {
                Ok(Ok(checks)) => {
                    histogram!(
                        "corro_consul.consul.response.time.seconds",
                        start.elapsed().as_secs_f64()
                    );
                    match update_consul_checks(
                        &corrosion,
                        checks,
                        check_hashes,
                        node,
                        skip_hash_check,
                    )
                    .await
                    {
                        Ok(stats) => Ok(stats),
                        Err(e) => Err(e),
                    }
                }
                Ok(Err(e)) => {
                    increment_counter!("corro_consul.consul.response.errors", "error" => e.to_string(), "type" => "checks");
                    Err(e.into())
                }
                Err(e) => {
                    increment_counter!("corro_consul.consul.response.errors", "error" => "timed out", "type" => "checks");
                    Err(e.into())
                }
            },
        )
    };

    let (svcs, checks) = tokio::join!(fut_services, fut_checks);
    [svcs, checks]
}

#[cfg(test)]
mod tests {
    use super::*;

    use corro_tests::launch_test_agent;
    use rusqlite::OptionalExtension;
    use tokio::time::sleep;
    use tripwire::Tripwire;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_operations() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let tmpdir = tempfile::tempdir()?;
        let schema_path = tmpdir.path().join("schema");
        tokio::fs::create_dir(&schema_path).await?;
        tokio::fs::copy(
            "../../tests/fixtures/consul.sql",
            schema_path.join("consul.sql"),
        )
        .await?;

        let ta1 = launch_test_agent(
            |conf| {
                conf.add_schema_path(schema_path.display().to_string())
                    .build()
            },
            tripwire.clone(),
        )
        .await?;
        let ta2 = launch_test_agent(
            |conf| {
                conf.add_schema_path(schema_path.display().to_string())
                    .bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        let ta1_client = CorrosionClient::new(&CorrosionConfig {
            api_addr: ta1.agent.api_addr().unwrap(),
            base_path: ta1.agent.base_path(),
        });

        setup(&ta1_client).await?;

        let mut services = HashMap::new();

        let svc = AgentService {
            id: "service-id".into(),
            name: "service-name".into(),
            tags: vec![],
            meta: vec![("app_id".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
            port: 1337,
            address: "127.0.0.1".into(),
        };

        services.insert("service-id".into(), svc.clone());

        let mut hashes = HashMap::new();

        let applied =
            update_consul_services(&ta1_client, services.clone(), &mut hashes, "node-1", false)
                .await?;

        assert_eq!(applied.upserted, 1);
        assert_eq!(applied.deleted, 0);

        let svc_hash = hash_service(&svc);

        assert_eq!(hashes.get("service-id"), Some(&svc_hash));

        {
            let conn = ta1_client.pool().get().await?;
            let hash_bytes = conn.query_row(
                "SELECT hash FROM __corro_consul_services WHERE id = ?",
                ["service-id"],
                |row| row.get(0),
            )?;

            let hash = u64::from_be_bytes(hash_bytes);
            assert_eq!(svc_hash, hash);
        }

        let applied =
            update_consul_services(&ta1_client, services, &mut hashes, "node-1", false).await?;

        assert_eq!(applied.upserted, 0);
        assert_eq!(applied.deleted, 0);

        assert_eq!(hashes.get("service-id"), Some(&hash_service(&svc)));

        sleep(Duration::from_secs(2)).await;

        let ta2_client = CorrosionClient::new(&CorrosionConfig {
            api_addr: ta2.agent.api_addr().unwrap(),
            base_path: ta2.agent.base_path(),
        });

        {
            let conn = ta2_client.pool().get().await?;
            let app_id: i64 =
                conn.query_row("SELECT app_id FROM consul_services LIMIT 1", (), |row| {
                    row.get(0)
                })?;
            assert_eq!(app_id, 123);
        }

        let applied =
            update_consul_services(&ta1_client, HashMap::new(), &mut hashes, "node-1", false)
                .await?;

        assert_eq!(applied.upserted, 0);
        assert_eq!(applied.deleted, 1);

        assert_eq!(hashes.get("service-id"), None);

        {
            let conn = ta1_client.pool().get().await?;
            let hash_bytes: Option<[u8; 8]> = conn
                .query_row(
                    "SELECT hash FROM __corro_consul_services WHERE id = ?",
                    ("service-id",),
                    |row| row.get(0),
                )
                .optional()?;

            assert_eq!(hash_bytes, None);
        }

        sleep(Duration::from_secs(1)).await;

        {
            let conn = ta2_client.pool().get().await?;
            let app_id: Option<i64> = conn
                .query_row("SELECT app_id FROM consul_services LIMIT 1", (), |row| {
                    row.get(0)
                })
                .optional()?;
            assert_eq!(app_id, None);
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
