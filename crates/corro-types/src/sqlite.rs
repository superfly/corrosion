use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
};

// use bb8::ManageConnection;
use camino::Utf8PathBuf;
use compact_str::CompactString;
use corro_api_types::integer_decode;
use enquote::enquote;
use once_cell::sync::Lazy;
use rusqlite::{params, Connection, Transaction};
use seahash::SeaHasher;
use sqlite_pool::SqliteConn;
use tempfile::TempDir;
use tracing::{error, info, trace};

use crate::schema::NormalizedTable;

pub type SqlitePool = sqlite_pool::Pool<CrConn>;
pub type SqlitePoolError = sqlite_pool::PoolError;

const CRSQL_EXT_GENERIC_NAME: &str = "crsqlite";

#[cfg(target_os = "macos")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.dylib";
#[cfg(target_os = "linux")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.so";

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-darwin-aarch64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-darwin-x86_64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-x86_64.so");
#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-aarch64.so");

// TODO: support windows

// need to keep this alive!
static CRSQL_EXT_DIR: Lazy<TempDir> = Lazy::new(|| {
    let dir = TempDir::new().expect("could not create temp dir!");
    std::fs::write(dir.path().join(CRSQL_EXT_GENERIC_NAME), CRSQL_EXT)
        .expect("could not write crsql ext file");
    dir
});

pub fn rusqlite_to_crsqlite(mut conn: rusqlite::Connection) -> rusqlite::Result<CrConn> {
    init_cr_conn(&mut conn)?;
    setup_conn(&mut conn, &HashMap::new())?;
    Ok(CrConn(conn))
}

#[derive(Debug)]
pub struct CrConn(Connection);

impl CrConn {
    pub fn init(mut conn: Connection) -> Result<Self, rusqlite::Error> {
        init_cr_conn(&mut conn)?;
        Ok(Self(conn))
    }

    pub fn transaction(&mut self) -> rusqlite::Result<Transaction> {
        self.0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
    }
}

impl SqliteConn for CrConn {
    fn conn(&self) -> &rusqlite::Connection {
        &self.0
    }
}

impl Deref for CrConn {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CrConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for CrConn {
    fn drop(&mut self) {
        if let Err(e) = self.execute_batch("select crsql_finalize();") {
            error!("could not crsql_finalize: {e}");
        }
    }
}

fn init_cr_conn(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let ext_dir = &CRSQL_EXT_DIR;
    trace!(
        "loading crsqlite extension from path: {}",
        ext_dir.path().display()
    );
    unsafe {
        trace!("enabled loading extension");
        conn.load_extension_enable()?;
        conn.load_extension(
            ext_dir.path().join(CRSQL_EXT_GENERIC_NAME),
            Some("sqlite3_crsqlite_init"),
        )?;
        conn.load_extension_disable()?;
    }
    trace!("loaded crsqlite extension");

    Ok(())
}

pub type AttachMap = HashMap<Utf8PathBuf, CompactString>;

pub(crate) fn setup_conn(conn: &mut Connection, attach: &AttachMap) -> Result<(), rusqlite::Error> {
    // WAL journal mode and synchronous NORMAL for best performance / crash resilience compromise
    conn.execute_batch(
        r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA recursive_triggers = ON;
        "#,
    )?;

    register_seahash_aggregate(conn)?;
    register_xor_aggregate(conn)?;

    for (path, name) in attach.iter() {
        conn.execute_batch(&format!(
            "ATTACH DATABASE {} AS {}",
            enquote('\'', path.as_str()),
            name
        ))?;
    }

    Ok(())
}

pub trait Migration {
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()>;
}

impl Migration for fn(&Transaction) -> rusqlite::Result<()> {
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()> {
        self(tx)
    }
}

const SCHEMA_VERSION_KEY: &str = "schema_version";

// Read migration version field from the SQLite db
pub fn migration_version(tx: &Transaction) -> Option<usize> {
    #[allow(deprecated)] // To keep compatibility with lower rusqlite versions
    tx.query_row(
        "SELECT value FROM __corro_state WHERE key = ?",
        [SCHEMA_VERSION_KEY],
        |row| row.get::<_, i64>(0),
    )
    .map(|v| v as usize)
    .ok()
}

// Set user version field from the SQLite db
pub fn set_migration_version(tx: &Transaction, v: usize) -> rusqlite::Result<usize> {
    tx.execute(
        "INSERT OR REPLACE INTO __corro_state VALUES (?, ?)",
        params![SCHEMA_VERSION_KEY, v],
    )
}

// should be a noop if up to date!
pub fn migrate(conn: &mut Connection, migrations: Vec<Box<dyn Migration>>) -> rusqlite::Result<()> {
    let target_version = migrations.len();

    let tx = conn.transaction()?;

    // determine how many migrations to skip (skip as many as we are at)
    let skip_n = migration_version(&tx).unwrap_or_default();

    for migration in migrations.into_iter().skip(skip_n) {
        migration.migrate(&tx)?;
    }
    set_migration_version(&tx, target_version)?;

    tx.commit()?;
    Ok(())
}

const CHECKSUM_SEEDS: [u64; 4] = [
    0x16f11fe89b0d677c,
    0xb480a793d8e6c86c,
    0x6fe2e5aaf078ebc9,
    0x14f994a4c5259381,
];

pub fn register_seahash_aggregate(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_aggregate_function(
        "seahash_concat",
        -1,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8
            | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        SeahashAggregate,
    )
}

pub fn register_xor_aggregate(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_aggregate_function(
        "xor",
        -1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        XorAggregate,
    )
}

struct XorAggregate;

impl rusqlite::functions::Aggregate<i64, Option<i64>> for XorAggregate {
    fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> rusqlite::Result<i64> {
        Ok(0)
    }

    fn step(
        &self,
        ctx: &mut rusqlite::functions::Context<'_>,
        xor: &mut i64,
    ) -> rusqlite::Result<()> {
        let param_count = ctx.len();
        if param_count == 0 {
            return Err(rusqlite::Error::InvalidParameterCount(param_count, 1));
        }

        for idx in 0..param_count {
            match ctx.get_raw(idx) {
                rusqlite::types::ValueRef::Text(_v) => Err(
                    rusqlite::Error::InvalidFunctionParameterType(idx, rusqlite::types::Type::Text),
                )?,
                rusqlite::types::ValueRef::Blob(_v) => Err(
                    rusqlite::Error::InvalidFunctionParameterType(idx, rusqlite::types::Type::Blob),
                )?,
                rusqlite::types::ValueRef::Null => {
                    // do nothing
                }
                rusqlite::types::ValueRef::Integer(i) => {
                    println!("xoring {i}");
                    *xor ^= i;
                }
                rusqlite::types::ValueRef::Real(_f) => Err(
                    rusqlite::Error::InvalidFunctionParameterType(idx, rusqlite::types::Type::Real),
                )?,
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _: &mut rusqlite::functions::Context<'_>,
        xor: Option<i64>,
    ) -> rusqlite::Result<Option<i64>> {
        Ok(xor)
    }
}

struct SeahashAggregate;

impl rusqlite::functions::Aggregate<SeaHasher, Option<i64>> for SeahashAggregate {
    fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> rusqlite::Result<SeaHasher> {
        Ok(SeaHasher::with_seeds(
            CHECKSUM_SEEDS[0],
            CHECKSUM_SEEDS[1],
            CHECKSUM_SEEDS[2],
            CHECKSUM_SEEDS[3],
        ))
    }

    fn step(
        &self,
        ctx: &mut rusqlite::functions::Context<'_>,
        hasher: &mut SeaHasher,
    ) -> rusqlite::Result<()> {
        let param_count = ctx.len();
        if param_count == 0 {
            return Err(rusqlite::Error::InvalidParameterCount(param_count, 1));
        }
        for idx in 0..param_count {
            match ctx.get_raw(idx) {
                rusqlite::types::ValueRef::Text(v) | rusqlite::types::ValueRef::Blob(v) => {
                    v.hash(hasher)
                }
                rusqlite::types::ValueRef::Null => {}
                rusqlite::types::ValueRef::Integer(i) => i.hash(hasher),
                rusqlite::types::ValueRef::Real(f) => integer_decode(f).hash(hasher),
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut rusqlite::functions::Context<'_>,
        hasher: Option<SeaHasher>,
    ) -> rusqlite::Result<Option<i64>> {
        Ok(hasher.map(|hasher| 0i64.wrapping_add_unsigned(hasher.finish())))
    }
}

pub const BUCKET_SIZE: u64 = 10240;

pub fn queue_full_table_hash(conn: &Connection, table: &NormalizedTable) -> rusqlite::Result<()> {
    conn.execute_batch(&format!(
        "DELETE FROM {table_name}__corro_buckets",
        table_name = &table.name
    ))?;

    let pks_list = table
        .pk
        .iter()
        .map(|pk| format!("pks.{pk}"))
        .collect::<Vec<_>>()
        .join(",");

    let ins = conn.execute(&format!("INSERT INTO {table_name}__corro_buckets SELECT __crsql_key as key, seahash_concat({pks_list}) - (seahash_concat({pks_list}) % ?) AS bucket FROM {table_name}__crsql_pks AS pks GROUP BY __crsql_key", table_name = &table.name), [BUCKET_SIZE])?;

    info!(
        "inserted {ins} rows into {table_name}__corro_buckets",
        table_name = &table.name
    );

    Ok(())
}

pub fn hash_bucket(
    conn: &Connection,
    table: &NormalizedTable,
    bucket: i64,
) -> rusqlite::Result<()> {
    register_seahash_aggregate(conn)?;

    let mut cols = table.pk.clone();

    for (col, _) in table.columns.iter() {
        cols.insert(col.clone());
    }

    let cols_list = cols
        .iter()
        .map(|col| format!("tbl.{col}"))
        .collect::<Vec<_>>()
        .join(",");

    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {table_name}__corro_hashes
                SELECT buckets.bucket, seahash_concat({cols_list}) AS hash
                    FROM {table_name}__corro_buckets AS buckets
                    LEFT JOIN {table_name}__crsql_pks AS pks ON __crsql_key = buckets.key
                    LEFT JOIN {table_name} AS tbl ON {pks_where}
                    WHERE buckets.bucket = ?
            ",
            table_name = &table.name,
            pks_where = table
                .pk
                .iter()
                .map(|pk| format!("pks.{pk} = tbl.{pk}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        [bucket],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use seahash::SeaHasher;
    use tokio::task::block_in_place;

    use crate::schema::{apply_schema, parse_sql, NormalizedSchema};

    use super::*;

    #[test]
    fn test_full_table_hash() -> Result<(), Box<dyn std::error::Error>> {
        let create_table =
            "CREATE TABLE foo (a INTEGER PRIMARY KEY NOT NULL, b INTEGER, c TEXT, d BLOB, e REAL);";
        let mut new_schema = parse_sql(create_table)?;

        let mut conn = rusqlite_to_crsqlite(rusqlite::Connection::open_in_memory()?)?;

        conn.execute_batch(create_table)?;

        conn.execute_batch(
            "INSERT INTO foo VALUES (1, 1, \"one\", x'01', 1.0), (2, 2, \"two\", x'02', 2.0), (3, NULL, \"three\", x'03', 3.3);",
        )?;

        {
            let tx = conn.transaction()?;
            apply_schema(&tx, &NormalizedSchema::default(), &mut new_schema)?;

            let tbl = new_schema.tables.get("foo").unwrap();

            // change 2 rows to put them in the same bucket
            tx.execute_batch(
                "
                UPDATE foo__corro_buckets SET bucket = 123456 WHERE key = 1 OR key = 3;
            ",
            )?;

            hash_bucket(&tx, tbl, 123456)?;

            tx.commit()?;
        }

        let mut prepped = conn.prepare("SELECT * FROM foo__corro_buckets")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            if row.is_none() {
                break;
            }

            println!("BUCKET ROW: {row:?}");
        }

        let mut prepped = conn.prepare("SELECT * FROM foo__corro_hashes")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            if row.is_none() {
                break;
            }

            println!("HASH ROW: {row:?}");
        }

        Ok(())
    }

    #[test]
    fn test_seahash_concat() -> Result<(), Box<dyn std::error::Error>> {
        let conn = rusqlite::Connection::open_in_memory()?;

        register_seahash_aggregate(&conn)?;

        conn.execute_batch(
            "CREATE TABLE foo (a INTEGER PRIMARY KEY NOT NULL, b INTEGER, c TEXT, d BLOB, e REAL);",
        )?;

        conn.execute_batch(
            "INSERT INTO foo VALUES (1, 1, \"one\", x'01', 1.0), (2, 2, \"two\", x'02', 2.0), (3, NULL, \"three\", x'03', 3.3);",
        )?;

        let hash: i64 =
            conn.query_row("SELECT seahash_concat(a,b,c,d,e) FROM foo;", [], |row| {
                row.get(0)
            })?;

        println!("HASH: {hash}");

        let mut hasher = SeaHasher::with_seeds(
            CHECKSUM_SEEDS[0],
            CHECKSUM_SEEDS[1],
            CHECKSUM_SEEDS[2],
            CHECKSUM_SEEDS[3],
        );

        1i64.hash(&mut hasher);
        1i64.hash(&mut hasher);
        b"one".hash(&mut hasher);
        [1u8].hash(&mut hasher);
        integer_decode(1.0).hash(&mut hasher);

        2i64.hash(&mut hasher);
        2i64.hash(&mut hasher);
        b"two".hash(&mut hasher);
        [2u8].hash(&mut hasher);
        integer_decode(2.0).hash(&mut hasher);

        3i64.hash(&mut hasher);
        b"three".hash(&mut hasher);
        [3u8].hash(&mut hasher);
        integer_decode(3.3).hash(&mut hasher);

        let expected_hash = hasher.finish();

        assert_eq!(hash, 0i64.wrapping_add_unsigned(expected_hash));

        assert_eq!(hash.to_ne_bytes(), expected_hash.to_ne_bytes());

        let hashes: Vec<(i64, i64)> = conn
            .prepare("SELECT a, seahash_concat(a,b,c,d,e) FROM foo GROUP BY a;")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<(i64, i64)>>>()?;

        let mut hasher = SeaHasher::with_seeds(
            CHECKSUM_SEEDS[0],
            CHECKSUM_SEEDS[1],
            CHECKSUM_SEEDS[2],
            CHECKSUM_SEEDS[3],
        );

        1i64.hash(&mut hasher);
        1i64.hash(&mut hasher);
        b"one".hash(&mut hasher);
        [1u8].hash(&mut hasher);
        integer_decode(1.0).hash(&mut hasher);

        let expected_hash_1 = hasher.finish();

        assert_eq!(hashes[0].1, 0i64.wrapping_add_unsigned(expected_hash_1));
        assert_eq!(hashes[0].1, -3801874191463215300);

        let mut hasher = SeaHasher::with_seeds(
            CHECKSUM_SEEDS[0],
            CHECKSUM_SEEDS[1],
            CHECKSUM_SEEDS[2],
            CHECKSUM_SEEDS[3],
        );

        2i64.hash(&mut hasher);
        2i64.hash(&mut hasher);
        b"two".hash(&mut hasher);
        [2u8].hash(&mut hasher);
        integer_decode(2.0).hash(&mut hasher);

        let expected_hash_2 = hasher.finish();

        assert_eq!(hashes[1].1, 0i64.wrapping_add_unsigned(expected_hash_2));

        register_xor_aggregate(&conn)?;

        let xored: i64 = conn.query_row(
            "SELECT xor(hash) FROM (SELECT seahash_concat(a,b,c,d,e) as hash FROM foo GROUP BY a ORDER BY a ASC);",
            [],
            |row| row.get(0),
        )?;

        println!("xor: {xored}");

        let xored2: i64 = conn.query_row(
            "SELECT xor(hash) FROM (SELECT seahash_concat(a,b,c,d,e) as hash FROM foo GROUP BY a ORDER BY a DESC);",
            [],
            |row| row.get(0),
        )?;

        println!("xor2: {xored2}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::TempDir::new()?;

        let pool = sqlite_pool::Config::new(tmpdir.path().join("test.db"))
            .max_size(1)
            .create_pool_transform(rusqlite_to_crsqlite)?;

        {
            let conn = pool.get().await?;

            conn.execute_batch(
                "
                CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
                SELECT crsql_as_crr('foo');
            ",
            )?;
        }

        let total: i64 = 1000;
        let per_worker: i64 = 5;

        let futs = FuturesUnordered::from_iter((0..total).map(|_| {
            let pool = pool.clone();
            async move {
                tokio::spawn(async move {
                    FuturesUnordered::from_iter((0..per_worker).map(|_| {
                        let pool = pool.clone();
                        async move {
                            let conn = pool.get().await?;
                            block_in_place(|| {
                                conn.prepare_cached(
                                    "INSERT INTO foo (a, b) VALUES (random(), random())",
                                )?
                                .execute(())?;
                                Ok::<_, TestError>(())
                            })?;
                            Ok::<_, TestError>(())
                        }
                    }))
                    .try_collect()
                    .await?;
                    Ok::<_, TestError>(())
                })
                .await??;
                Ok::<_, TestError>(())
            }
        }));

        futs.try_collect().await?;

        let conn = pool.get().await?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM foo;", (), |row| row.get(0))?;

        assert_eq!(count, total * per_worker);

        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Rusqlite(#[from] rusqlite::Error),
        #[error(transparent)]
        Bb8Rusqlite(#[from] SqlitePoolError),
        #[error(transparent)]
        Join(#[from] tokio::task::JoinError),
    }
}
