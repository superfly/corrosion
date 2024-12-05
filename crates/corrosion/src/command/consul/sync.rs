use consul_client::{AgentCheck, AgentService, Client};
use corro_api_types::ColumnType;
use corro_client::CorrosionClient;
use corro_types::{api::Statement, config::ConsulConfig};
use futures::future::select;
use metrics::{counter, histogram};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    signal::unix::{signal, SignalKind},
    time::{interval, timeout},
};
use tracing::{debug, error, info, trace};

const CONSUL_PULL_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run<P: AsRef<Path>>(
    config: &ConsulConfig,
    api_addr: SocketAddr,
    db_path: P,
) -> eyre::Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    let sigterm_recv = sigterm.recv();
    tokio::pin!(sigterm_recv);
    let sigint_recv = sigint.recv();
    tokio::pin!(sigint_recv);

    let mut stop_signal = select(sigterm_recv, sigint_recv);

    let node: &'static str = Box::leak(
        hostname::get()?
            .into_string()
            .expect("could not convert hostname to string")
            .into_boxed_str(),
    );

    let corrosion = CorrosionClient::new(api_addr, db_path);
    let consul = consul_client::Client::new(config.client.clone())?;

    info!("Setting up corrosion for consul sync");
    setup(&corrosion).await?;

    let mut consul_services: HashMap<String, u64> = HashMap::new();
    let mut consul_checks: HashMap<String, u64> = HashMap::new();

    {
        let conn = corrosion.pool().get().await?;

        info!("Populating initial service hashes");
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

        info!("Populating initial checks hashes");
        let mut prepped = conn.prepare("SELECT id, hash FROM __corro_consul_checks")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => {
                    break;
                }
            };

            consul_checks.insert(row.get::<_, String>(0)?, u64::from_be_bytes(row.get(1)?));
        }
    }
    update_hashes(&corrosion, node, &mut consul_services, &mut consul_checks).await?;

    let mut pull_interval = interval(CONSUL_PULL_INTERVAL);

    info!("Starting consul pull interval");
    loop {
        let res = tokio::select! {
            _ = pull_interval.tick() => {
                update_consul(&consul, node, &corrosion, &mut consul_services, &mut consul_checks, false).await
            },
            _ = &mut stop_signal => {
                debug!("tripped consul loop");
                break;
            }
        };

        debug!("got results: {res:?}");

        match res {
            Ok((svc_stats, check_stats)) => {
                if !svc_stats.is_zero() {
                    info!("updated consul services: {svc_stats:?}");
                }
                if !check_stats.is_zero() {
                    info!("updated consul checks: {check_stats:?}");
                }
            }
            Err(e) => match e {
                UpdateError::Execute(ExecuteError::Sqlite(_)) => {
                    return Err(e.into());
                }
                e => {
                    error!("non-fatal update error, continuing. error: {e}");
                }
            },
        }
    }

    Ok(())
}

async fn setup(corrosion: &CorrosionClient) -> eyre::Result<()> {
    let mut conn = corrosion.pool().get().await?;
    {
        let tx = conn.transaction()?;

        info!("Creating internal tables");
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
    }
    info!("Ensuring schema...");

    struct ColumnInfo {
        name: String,
        kind: corro_api_types::ColumnType,
    }

    let col_infos: Vec<ColumnInfo> = conn
        .prepare("PRAGMA table_info(consul_services)")?
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                kind: row.get(2)?,
            })
        })
        .map_err(|e| eyre::eyre!("could not query consul_services' table_info: {e}"))?
        .collect::<Result<Vec<_>, _>>()?;

    let expected_cols = [
        ("node", vec![ColumnType::Text]),
        ("id", vec![ColumnType::Text]),
        ("name", vec![ColumnType::Text]),
        ("tags", vec![ColumnType::Text, ColumnType::Blob]),
        ("meta", vec![ColumnType::Text, ColumnType::Blob]),
        ("port", vec![ColumnType::Integer]),
        ("address", vec![ColumnType::Text]),
        ("updated_at", vec![ColumnType::Integer]),
    ];

    for (name, kind) in expected_cols {
        if !col_infos
            .iter()
            .any(|info| info.name == name && kind.contains(&info.kind))
        {
            eyre::bail!("expected a column consul_services.{name} w/ type {kind:?}");
        }
    }

    let col_infos: Vec<ColumnInfo> = conn
        .prepare("PRAGMA table_info(consul_checks)")?
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                kind: row.get(2)?,
            })
        })
        .map_err(|e| eyre::eyre!("could not query consul_checks' table_info: {e}"))?
        .collect::<Result<Vec<_>, _>>()?;

    let expected_cols = [
        ("node", vec![ColumnType::Text]),
        ("id", vec![ColumnType::Text]),
        ("service_id", vec![ColumnType::Text]),
        ("service_name", vec![ColumnType::Text]),
        ("name", vec![ColumnType::Text]),
        ("status", vec![ColumnType::Text]),
        ("output", vec![ColumnType::Text]),
        ("updated_at", vec![ColumnType::Integer]),
    ];

    for (name, kind) in expected_cols {
        if !col_infos
            .iter()
            .any(|info| info.name == name && kind.contains(&info.kind))
        {
            eyre::bail!("expected a column consul_checks.{name} w/ type {kind:?}");
        }
    }

    Ok(())
}

async fn update_hashes(
    corrosion: &CorrosionClient,
    nodename: &str,
    service_hashes: &mut HashMap<String, u64>,
    check_hashes: &mut HashMap<String, u64>,
) -> eyre::Result<()> {
    let (services, checks) = {
        let conn = corrosion.pool().get().await?;

        let services = conn
            .prepare(
                "SELECT id, name, tags, meta, port, address FROM consul_services WHERE node = ? AND source IS NULL",
            )?
            .query_map([nodename], |row| {
                let tag_json: String = row.get(2)?;
                let meta_json: String = row.get(3)?;
                Ok(AgentService {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    tags: serde_json::from_str(&tag_json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?,
                    meta: serde_json::from_str(&meta_json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?,
                    port: row.get(4)?,
                    address: row.get(5)?,
                })
            })
            .map_err(|e| eyre::eyre!("could not query consul_checks' table_info: {e}"))?
            .collect::<Result<Vec<_>, _>>()?;

        let checks = conn.prepare("SELECT id, name, status, output, service_id, service_name FROM consul_checks WHERE node = ? AND source IS NULL")?
            .query_map([nodename], |row| {
                Ok(AgentCheck{
                    id: row.get(0)?,
                    name: row.get(1)?,
                    status: row.get(2)?,
                    output: row.get(3)?,
                    service_id: row.get(4)?,
                    service_name: row.get(5)?,
                    notes: None,
                })
            })
            .map_err(|e| eyre::eyre!("could not query consul_checks' table_info: {e}"))?
            .collect::<Result<Vec<_>, _>>()?;

        (services, checks)
    };

    let mut statements = Vec::with_capacity(services.len() + checks.len());

    let mut svc_insert = vec![];
    let mut checks_insert = vec![];

    for svc in services {
        if service_hashes.get(&svc.id).is_none() {
            let hash = hash_service(&svc);
            statements.push(Statement::WithParams(
                "INSERT INTO __corro_consul_services ( id, hash ) VALUES (?, ?);".into(),
                vec![svc.id.clone().into(), hash.to_be_bytes().to_vec().into()],
            ));
            svc_insert.push((svc.id, hash));
        }
    }

    for check in checks {
        if check_hashes.get(&check.id).is_none() {
            let hash = hash_check(&check);
            statements.push(Statement::WithParams(
                "INSERT OR REPLACE INTO __corro_consul_checks ( id, hash ) VALUES (?, ?)".into(),
                vec![check.id.clone().into(), hash.to_be_bytes().to_vec().into()],
            ));
            checks_insert.push((check.id, hash));
        }
    }

    if !statements.is_empty() {
        if let Some(e) = corrosion
            .execute(&statements)
            .await?
            .results
            .into_iter()
            .find_map(|res| match res {
                corro_api_types::ExecResult::Execute { .. } => None,
                corro_api_types::ExecResult::Error { error } => Some(error),
            })
        {
            return Err(ExecuteError::Sqlite(e).into());
        }
    }

    for (id, hash) in svc_insert {
        service_hashes.insert(id, hash);
    }
    for (id, hash) in checks_insert {
        check_hashes.insert(id, hash);
    }
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

impl ApplyStats {
    fn is_zero(&self) -> bool {
        self.upserted == 0 && self.deleted == 0
    }
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

fn append_upsert_service_statements(
    statements: &mut Vec<Statement>,
    node: &'static str,
    svc: AgentService,
    hash: u64,
    updated_at: i64,
) {
    // run this by corrosion so it's part of the same transaction
    statements.push(Statement::WithParams(
        "INSERT INTO __corro_consul_services ( id, hash )
    VALUES (?, ?)
    ON CONFLICT (id) DO UPDATE SET
        hash = excluded.hash;"
            .into(),
        vec![svc.id.clone().into(), hash.to_be_bytes().to_vec().into()],
    ));

    // upsert!
    statements.push(Statement::WithParams(
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
        vec![
            node.into(),
            svc.id.into(),
            svc.name.into(),
            serde_json::to_string(&svc.tags)
                .unwrap_or_else(|_| "[]".to_string())
                .into(),
            serde_json::to_string(&svc.meta)
                .unwrap_or_else(|_| "{}".to_string())
                .into(),
            svc.port.into(),
            svc.address.into(),
            updated_at.into(),
        ],
    ));
}

fn append_upsert_check_statements(
    statements: &mut Vec<Statement>,
    node: &'static str,
    check: AgentCheck,
    hash: u64,
    updated_at: i64,
) {
    // run this by corrosion so it's part of the same transaction
    statements.push(Statement::WithParams(
        "INSERT INTO __corro_consul_checks ( id, hash )
    VALUES (?, ?)
    ON CONFLICT (id) DO UPDATE SET
        hash = excluded.hash;"
            .into(),
        vec![check.id.clone().into(), hash.to_be_bytes().to_vec().into()],
    ));

    // upsert!
    statements.push(Statement::WithParams("INSERT INTO consul_checks ( node, id, service_id, service_name, name, status, output, updated_at )
    VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(node, id) DO UPDATE SET
        service_id = excluded.service_id,
        service_name = excluded.service_name,
        name = excluded.name,
        status = excluded.status,
        output = excluded.output,
        updated_at = excluded.updated_at;"
                                          .into(),vec![
                                              node.into(),
                                              check.id.into(),
                                              check.service_id.into(),
                                              check.service_name.into(),
                                              check.name.into(),
                                              check.status.as_str().into(),
                                              check.output.into(),
                                              updated_at.into(),
                                          ]));
}

enum ConsulServiceOp {
    Upsert { svc: AgentService, hash: u64 },
    Delete { id: String },
}

enum ConsulCheckOp {
    Upsert { check: AgentCheck, hash: u64 },
    Delete { id: String },
}

///
fn update_services(
    mut services: HashMap<String, AgentService>,
    hashes: &HashMap<String, u64>,
    skip_hash_check: bool,
) -> Vec<ConsulServiceOp> {
    let mut ops = vec![];

    {
        for (id, old_hash) in hashes.iter() {
            if let Some(svc) = services.remove(id) {
                let hash = hash_service(&svc);
                if skip_hash_check || *old_hash != hash {
                    info!("updating service '{id}'");

                    ops.push(ConsulServiceOp::Upsert { svc, hash });
                }
            } else {
                info!("deleting service: {id}");
                ops.push(ConsulServiceOp::Delete { id: id.clone() });
            }
        }
    }

    // new services
    for (id, svc) in services {
        info!("inserting service '{id}'");

        let hash = hash_service(&svc);
        ops.push(ConsulServiceOp::Upsert { svc, hash });
    }

    ops
}

fn update_checks(
    mut checks: HashMap<String, AgentCheck>,
    hashes: &HashMap<String, u64>,
    skip_hash_check: bool,
) -> Vec<ConsulCheckOp> {
    let mut ops = vec![];

    {
        for (id, old_hash) in hashes.iter() {
            if let Some(check) = checks.remove(id) {
                let hash = hash_check(&check);
                if skip_hash_check || *old_hash != hash {
                    info!("updating check '{id}'");

                    ops.push(ConsulCheckOp::Upsert { check, hash });
                }
            } else {
                info!("deleting check: {id}");
                ops.push(ConsulCheckOp::Delete { id: id.clone() });
            }
        }
    }

    // new checks
    for (id, check) in checks {
        info!("upserting check '{id}'");
        let hash = hash_check(&check);
        ops.push(ConsulCheckOp::Upsert { check, hash });
    }

    ops
}

#[derive(Debug, thiserror::Error)]
enum UpdateError {
    #[error("consul: {0}")]
    Consul(#[from] consul_client::Error),
    #[error("timed out")]
    TimedOut,
    #[error(transparent)]
    Execute(#[from] ExecuteError),
}

async fn update_consul(
    consul: &Client,
    node: &'static str,
    corrosion: &CorrosionClient,
    service_hashes: &mut HashMap<String, u64>,
    check_hashes: &mut HashMap<String, u64>,
    skip_hash_check: bool,
) -> Result<(ApplyStats, ApplyStats), UpdateError> {
    let fut_services = async {
        let start = Instant::now();
        match timeout(Duration::from_secs(5), consul.agent_services()).await {
            Ok(Ok(services)) => {
                histogram!("corro_consul.consul.response.time.seconds")
                    .record(start.elapsed().as_secs_f64());
                Ok(update_services(services, service_hashes, skip_hash_check))
            }
            Ok(Err(e)) => {
                counter!("corro_consul.consul.response.errors", "error" => e.to_string(), "type" => "services").increment(1);
                Err(e.into())
            }
            Err(_e) => {
                counter!("corro_consul.consul.response.errors", "error" => "timed out", "type" => "services").increment(1);
                Err(UpdateError::TimedOut)
            }
        }
    };

    let fut_checks = async {
        let start = Instant::now();
        match timeout(Duration::from_secs(5), consul.agent_checks()).await {
            Ok(Ok(checks)) => {
                histogram!("corro_consul.consul.response.time.seconds")
                    .record(start.elapsed().as_secs_f64());
                Ok(update_checks(checks, check_hashes, skip_hash_check))
            }
            Ok(Err(e)) => {
                counter!("corro_consul.consul.response.errors", "error" => e.to_string(), "type" => "checks").increment(1);
                Err(e.into())
            }
            Err(_e) => {
                counter!("corro_consul.consul.response.errors", "error" => "timed out", "type" => "checks").increment(1);
                Err(UpdateError::TimedOut)
            }
        }
    };

    let (svcs, checks) = tokio::try_join!(fut_services, fut_checks)?;

    execute(node, corrosion, svcs, service_hashes, checks, check_hashes)
        .await
        .map_err(UpdateError::from)
}

#[derive(Debug, thiserror::Error)]
enum ExecuteError {
    #[error("client: {0}")]
    Client(#[from] corro_client::Error),
    #[error("sqlite: {0}")]
    Sqlite(String),
}

async fn execute(
    node: &'static str,
    corrosion: &CorrosionClient,
    svcs: Vec<ConsulServiceOp>,
    service_hashes: &mut HashMap<String, u64>,
    checks: Vec<ConsulCheckOp>,
    check_hashes: &mut HashMap<String, u64>,
) -> Result<(ApplyStats, ApplyStats), ExecuteError> {
    let updated_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("could not get system time")
        .as_millis() as i64;

    let mut statements = Vec::with_capacity(svcs.len() + checks.len());

    let mut svc_to_upsert = vec![];
    let mut svc_to_delete = vec![];

    for op in svcs {
        match op {
            ConsulServiceOp::Upsert { svc, hash } => {
                svc_to_upsert.push((svc.id.clone(), hash));
                append_upsert_service_statements(&mut statements, node, svc, hash, updated_at);
            }
            ConsulServiceOp::Delete { id } => {
                svc_to_delete.push(id.clone());

                statements.push(Statement::WithParams(
                    "DELETE FROM __corro_consul_services WHERE id = ?;".into(),
                    vec![id.clone().into()],
                ));
                statements.push(Statement::WithParams(
                    "DELETE FROM consul_services WHERE node = ? AND id = ?;".into(),
                    vec![node.into(), id.into()],
                ));
            }
        }
    }

    // delete everything that's wrong in the DB! this is useful on restore from a backup...
    statements.push(Statement::WithParams("DELETE FROM consul_services WHERE node = ? AND source IS NULL AND id NOT IN (SELECT id FROM __corro_consul_services)".into(), vec![node.into()]));

    let mut check_to_upsert = vec![];
    let mut check_to_delete = vec![];

    for op in checks {
        match op {
            ConsulCheckOp::Upsert { check, hash } => {
                check_to_upsert.push((check.id.clone(), hash));
                append_upsert_check_statements(&mut statements, node, check, hash, updated_at);
            }
            ConsulCheckOp::Delete { id } => {
                check_to_delete.push(id.clone());
                statements.push(Statement::WithParams(
                    "DELETE FROM __corro_consul_checks WHERE id = ?;".into(),
                    vec![id.clone().into()],
                ));
                statements.push(Statement::WithParams(
                    "DELETE FROM consul_checks WHERE node = ? AND id = ?;".into(),
                    vec![node.into(), id.into()],
                ));
            }
        }
    }

    // delete everything that's wrong in the DB! this is useful on restore from a backup...
    statements.push(Statement::WithParams("DELETE FROM consul_checks WHERE node = ? AND source IS NULL AND id NOT IN (SELECT id FROM __corro_consul_checks)".into(), vec![node.into()]));

    if !statements.is_empty() {
        if let Some(e) = corrosion
            .execute(&statements)
            .await?
            .results
            .into_iter()
            .find_map(|res| match res {
                corro_api_types::ExecResult::Execute { .. } => None,
                corro_api_types::ExecResult::Error { error } => Some(error),
            })
        {
            return Err(ExecuteError::Sqlite(e));
        }
    }

    let mut svc_stats = ApplyStats::default();

    for (id, hash) in svc_to_upsert {
        service_hashes.insert(id, hash);
        svc_stats.upserted += 1;
    }
    for id in svc_to_delete {
        service_hashes.remove(&id);
        svc_stats.deleted += 1;
    }

    let mut check_stats = ApplyStats::default();

    for (id, hash) in check_to_upsert {
        check_hashes.insert(id, hash);
        check_stats.upserted += 1;
    }
    for id in check_to_delete {
        check_hashes.remove(&id);
        check_stats.deleted += 1;
    }

    Ok((svc_stats, check_stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    use corro_tests::launch_test_agent;
    use rusqlite::OptionalExtension;
    use spawn::wait_for_all_pending_handles;
    use tokio::time::sleep;
    use tripwire::Tripwire;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_operations() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let tmpdir = tempfile::TempDir::new()?;
        tokio::fs::write(
            tmpdir.path().join("consul.sql"),
            b"
            CREATE TABLE consul_services (
                node TEXT NOT NULL,
                id TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                tags TEXT NOT NULL DEFAULT '[]',
                meta TEXT NOT NULL DEFAULT '{}',
                port INTEGER NOT NULL DEFAULT 0,
                address TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL DEFAULT 0,
                app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)),        
                source TEXT,

                PRIMARY KEY (node, id)
            );

            CREATE TABLE consul_checks (
                node TEXT NOT NULL,
                id TEXT NOT NULL,
                service_id TEXT NOT NULL DEFAULT '',
                service_name TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                output TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL DEFAULT 0,
                source TEXT,
                PRIMARY KEY (node, id)
            );
        ",
        )
        .await?;

        let ta1 = launch_test_agent(
            |conf| {
                conf.add_schema_path(tmpdir.path().display().to_string())
                    .build()
            },
            tripwire.clone(),
        )
        .await?;
        let ta2 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                    .add_schema_path(tmpdir.path().display().to_string())
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        let ta1_client = CorrosionClient::new(ta1.agent.api_addr(), ta1.agent.db_path());

        setup(&ta1_client).await?;

        let mut svc_hashes = HashMap::new();
        let mut check_hashes = HashMap::new();

        // insert some services in the database
        let svc0 = AgentService {
            id: "service-id0".into(),
            name: "service-name0".into(),
            tags: vec![],
            meta: vec![("app_id".to_string(), "1233".to_string())]
                .into_iter()
                .collect(),
            port: 1337,
            address: "127.0.0.2".into(),
        };

        // let conn = ta1_client.pool().get().await?;
        let svc0_clone = svc0.clone();
        ta1_client
            .execute(&[Statement::WithParams(
                "INSERT INTO consul_services ( node, id, name, tags, meta, port, address)
                     VALUES (?,?,?,?,?,?,?)"
                    .into(),
                vec![
                    "node-1".into(),
                    svc0_clone.id.into(),
                    svc0_clone.name.into(),
                    serde_json::to_string(&svc0_clone.tags).unwrap().into(),
                    serde_json::to_string(&svc0_clone.meta).unwrap().into(),
                    svc0_clone.port.into(),
                    svc0_clone.address.into(),
                ],
            )])
            .await?;

        update_hashes(&ta1_client, "node-1", &mut svc_hashes, &mut check_hashes).await?;

        assert!(!svc_hashes.is_empty());
        {
            let conn = ta1_client.pool().get().await?;
            let hash: Vec<u8> = conn.query_row(
                "SELECT hash FROM __corro_consul_services WHERE id = 'service-id0'",
                (),
                |row| row.get(0),
            )?;
            assert_eq!(hash, hash_service(&svc0).to_be_bytes().to_vec());
        }

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
        services.insert("service-id0".into(), svc0.clone());

        let (applied, check_applied) = execute(
            "node-1",
            &ta1_client,
            update_services(services.clone(), &svc_hashes, false),
            &mut svc_hashes,
            Default::default(),
            &mut check_hashes,
        )
        .await?;

        assert!(check_applied.is_zero());

        assert_eq!(applied.upserted, 1);
        assert_eq!(applied.deleted, 0);

        let svc_hash = hash_service(&svc);

        assert_eq!(svc_hashes.get("service-id"), Some(&svc_hash));

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

        let (applied, _check_applied) = execute(
            "node-1",
            &ta1_client,
            update_services(services, &svc_hashes, false),
            &mut svc_hashes,
            Default::default(),
            &mut check_hashes,
        )
        .await?;

        assert!(check_applied.is_zero());

        assert_eq!(applied.upserted, 0);
        assert_eq!(applied.deleted, 0);

        assert_eq!(svc_hashes.get("service-id"), Some(&hash_service(&svc)));

        let ta2_client = CorrosionClient::new(ta2.agent.api_addr(), ta2.agent.db_path());

        setup(&ta2_client).await?;

        sleep(Duration::from_secs(2)).await;

        {
            let conn = ta2_client.pool().get().await?;
            let app_id: i64 = conn.query_row(
                "SELECT app_id FROM consul_services WHERE id = 'service-id'",
                (),
                |row| row.get(0),
            )?;
            assert_eq!(app_id, 123);
        }

        let (applied, _check_applied) = execute(
            "node-1",
            &ta1_client,
            update_services(HashMap::new(), &svc_hashes, false),
            &mut svc_hashes,
            Default::default(),
            &mut check_hashes,
        )
        .await?;

        assert!(check_applied.is_zero());

        assert_eq!(applied.upserted, 0);
        assert_eq!(applied.deleted, 2);

        assert_eq!(svc_hashes.get("service-id"), None);

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
