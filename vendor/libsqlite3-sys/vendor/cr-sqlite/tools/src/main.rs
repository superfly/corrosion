use std::time::{Duration, Instant};

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rusqlite::{types::Value, Connection};
use tempfile::tempdir;

// trials = 100
// batch_size = 1000

fn main() {
    let tmp = tempdir().unwrap();

    let mut args = std::env::args();
    let ext_file = args.nth(1).unwrap_or("../core/dist/crsqlite".to_string());

    let mut conn = rusqlite::Connection::open(tmp.path().join("perf.db")).unwrap();

    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
    ",
    )
    .unwrap();

    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(&ext_file, None).unwrap();
    }

    let version = conn.query_row("SELECT value from crsql_master where key = 'crsqlite_version'", (), |row| row.get::<_, i32>(0))
        .unwrap();
    let use_ts = version == 171000;
    create_crr(&conn);

    println!("Using ext_file: {ext_file}, set_ts: {use_ts}");

    // create vanilla tables
    conn.execute_batch(
        "
        CREATE TABLE vuser (id primary key, name);
        CREATE TABLE vdeck (id primary key, owner_id, title);
        CREATE TABLE vslide (id primary key, deck_id, \"order\");
        CREATE TABLE vcomponent (id primary key, type, slide_id, content);
    ",
    )
    .unwrap();

    let mut trials = 100;
    let mut batch_size = 1000;

    let mut count = 5;

    // conn.trace(Some(|sql| println!("{sql}")));

    // inserts
    let mut times = Vec::new();
    for j in 0..count {
        let start = Instant::now();
        for i in 0..trials {
            // let start = Instant::now();
            insert(&mut conn, "v", batch_size, batch_size * ( i + (j * trials)), use_ts);
            // let elapsed = start.elapsed();
            // println!("insert #{i} done in {elapsed:?}");
        }
        times.push(start.elapsed());
    }
    let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
    let max_time = times.iter().max().unwrap();
    let min_time = times.iter().min().unwrap();
    println!("insert (vanilla) avg time: {:?}, max: {:?}, min: {:?}", avg_time, max_time, min_time);

    let mut times = Vec::new();
    for j in 0..count {
        let start = Instant::now();
        for i in 0..trials {
            let start = Instant::now();
            insert(&mut conn, "", batch_size, batch_size * ( i + (j * trials)), use_ts);
            let elapsed = start.elapsed();
            // println!("insert #{i} done in {elapsed:?}");
        }
        times.push(start.elapsed());
    }
    let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
    let max_time = times.iter().max().unwrap();
    let min_time = times.iter().min().unwrap();
    println!("insert avg time: {:?}, max: {:?}, min: {:?}", avg_time, max_time, min_time);

    // updates
    let mut times = Vec::new();
    for j in 0..count {
        let start = Instant::now();
        for i in 0..trials {
            let start = Instant::now();
            update(&mut conn, "v", batch_size, batch_size * ( i + (j * trials)), use_ts);
            let elapsed = start.elapsed();
        }
        times.push(start.elapsed());
        // println!("update #{i} done in {elapsed:?}");
    }
    let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
    let max_time = times.iter().max().unwrap();
    let min_time = times.iter().min().unwrap();
    println!("update (vanilla) avg time: {:?}, max: {:?}, min: {:?}", avg_time, max_time, min_time);

    let mut times = Vec::new();
    for j in 0..count {
        let start = Instant::now();
        for i in 0..trials {
            let start = Instant::now();
            update(&mut conn, "", batch_size, batch_size * ( i + (j * trials)), use_ts);
            let elapsed = start.elapsed();
        }
        times.push(start.elapsed());
        // println!("update #{i} done in {elapsed:?}");
    }
    let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
    let max_time = times.iter().max().unwrap();
    let min_time = times.iter().min().unwrap();
    println!("update avg time: {:?}, max: {:?}, min: {:?}", avg_time, max_time, min_time);

    // // single insert
    // let start = Instant::now();
    // for i in 0..trials {
    //     let start = Instant::now();
    //     single_stmt_insert(&mut conn, "", batch_size, batch_size * i);
    //     let elapsed = start.elapsed();
    //     // println!("single_stmt_insert #{i} done in {elapsed:?}");
    // }
    // println!("single_stmt_insert total time: {:?}", start.elapsed());

    // // single insert vanilla
    // let start = Instant::now();
    // for i in 0..trials {
    //     let start = Instant::now();
    //     single_stmt_insert(&mut conn, "v", batch_size, batch_size * i);
    //     let elapsed = start.elapsed();
    //     // println!("single_stmt_insert #{i} done in {elapsed:?}");
    // }
    // println!(
    //     "single_stmt_insert (vanilla) total time: {:?}",
    //     start.elapsed()
    // );

    // let start = Instant::now();
    // for i in 0..100 {
    //     let start = Instant::now();
    //     read_changes(&conn, "", 100, 100 * i);
    //     let elapsed = start.elapsed();
    //     // println!("read_changes #{i} done in {elapsed:?}");
    // }
    // println!("read_changes total time: {:?}", start.elapsed());

    // let start = Instant::now();
    // for i in 0..100 {
    //     let start = Instant::now();
    //     read_changes(&conn, "v", 100, 100 * i);
    //     let elapsed = start.elapsed();
    //     // println!("read_changes #{i} done in {elapsed:?}");
    // }
    // println!("read_changes (vanilla) total time: {:?}", start.elapsed());

    // let mut merge_from = setup_merge_test_db();
    // let mut merge_to = setup_merge_test_db();

    // let start = Instant::now();
    // for t in 0..100 {
    //     let start = Instant::now();
    //     merge(&merge_from, &mut merge_to, "", 100, 100 * t);
    //     let elapsed = start.elapsed();
    //     // println!("merge #{t} done in {elapsed:?}");
    // }
    // println!("merge total time: {:?}", start.elapsed());

    // // let count: i64 = merge_from
    // //     .query_row("SELECT count(*) FROM crsql_changes", [], |row| row.get(0))
    // //     .unwrap();
    // // println!("count from: {count}");

    // // let count: i64 = merge_to
    // //     .query_row("SELECT count(*) FROM crsql_changes", [], |row| row.get(0))
    // //     .unwrap();
    // // println!("count to: {count}");

    // let start = Instant::now();
    // for t in 0..100 {
    //     let start = Instant::now();
    //     normal_insert(&merge_from, &mut merge_to, "", 100, 100 * t);
    //     let elapsed = start.elapsed();
    //     // println!("normal_insert #{t} done in {elapsed:?}");
    // }
    // println!("normal_insert total time: {:?}", start.elapsed());

    // modify_rows(&mut merge_from);

    // let start = Instant::now();
    // for t in 0..100 {
    //     let start = Instant::now();
    //     merge(&merge_from, &mut merge_to, "", 100, 100 * t);
    //     let elapsed = start.elapsed();
    //     // println!("merge #{t} (dirty) done in {elapsed:?}");
    // }
    // println!("merge (dirty) total time: {:?}", start.elapsed());

    // // let count: i64 = merge_from
    // //     .query_row("SELECT count(*) FROM crsql_changes", [], |row| row.get(0))
    // //     .unwrap();
    // // println!("count from: {count}");

    // // let count: i64 = merge_to
    // //     .query_row("SELECT count(*) FROM crsql_changes", [], |row| row.get(0))
    // //     .unwrap();
    // // println!("count to: {count}");

    // let start = Instant::now();
    // for t in 0..100 {
    //     let start = Instant::now();
    //     normal_insert(&merge_from, &mut merge_to, "", 100, 100 * t);
    //     let elapsed = start.elapsed();
    //     // println!("normal_insert (dirty) #{t} done in {elapsed:?}");
    // }
    // println!("normal_insert (dirty) total time: {:?}", start.elapsed());

    // merge_from.execute_batch("SELECT crsql_finalize()").unwrap();
    // drop(merge_from);
    // merge_to.execute_batch("SELECT crsql_finalize()").unwrap();
    // drop(merge_to);
}

fn create_crr(conn: &Connection) {
    conn.execute_batch("CREATE TABLE user (id primary key not null, name)")
        .unwrap();
    conn.execute_batch("CREATE TABLE deck (id primary key not null, owner_id, title)")
        .unwrap();
    conn.execute_batch("CREATE TABLE slide (id primary key not null, deck_id, \"order\")")
        .unwrap();
    conn.execute_batch("CREATE TABLE component (id primary key not null, type, slide_id, content)")
        .unwrap();

    conn.execute_batch("select crsql_as_crr('user')").unwrap();
    conn.execute_batch("select crsql_as_crr('deck')").unwrap();
    conn.execute_batch("select crsql_as_crr('slide')").unwrap();
    conn.execute_batch("select crsql_as_crr('component')")
        .unwrap();
}

fn create_merge_control(conn: &mut Connection) {
    conn.execute_batch(
        "
        CREATE TABLE merge_control (id PRIMARY KEY, tx INTEGER, content TEXT);
        CREATE INDEX merge_control_tx ON merge_control (tx);
    ",
    )
    .unwrap();
}

fn setup_merge_test_db() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension("../core/dist/crsqlite", None).unwrap();
    }
    create_crr(&conn);
    create_merge_control(&mut conn);

    for t in 0..100 {
        let offset = t * 100;
        for i in 0..100 {
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO user VALUES (?, ?)", (i + offset, random_str()))
                .unwrap();
            tx.execute(
                "INSERT INTO deck VALUES (?, ?, ?)",
                (i + offset, i + offset, random_str()),
            )
            .unwrap();
            tx.execute(
                "INSERT INTO slide VALUES (?, ?, ?)",
                (i + offset, i + offset, i),
            )
            .unwrap();
            tx.execute(
                "INSERT INTO component VALUES (?, ?, ?, ?)",
                (i + offset, "text", i + offset, random_str()),
            )
            .unwrap();

            let j = i * 4;
            tx.execute(
                "INSERT INTO merge_control VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                (
                    j + offset * 4,
                    t,
                    random_str(),
                    j + 1 + offset * 4,
                    t,
                    random_str(),
                    j + 2 + offset * 4,
                    t,
                    random_str(),
                    j + 3 + offset * 4,
                    t,
                    random_str(),
                ),
            )
            .unwrap();

            tx.commit().unwrap();
        }
    }
    conn
}

fn modify_rows(conn: &mut Connection) {
    for t in 0..100 {
        let offset = t * 100;
        for i in 0..100 {
            let tx = conn.transaction().unwrap();

            tx.execute(
                "UPDATE user SET name = ? WHERE id = ?",
                (random_str(), i + offset),
            )
            .unwrap();
            tx.execute(
                "UPDATE deck SET title = ? WHERE id = ?",
                (random_str(), i + offset),
            )
            .unwrap();
            tx.execute(
                "UPDATE component SET content = ? WHERE id = ?",
                (random_str(), i + offset),
            )
            .unwrap();

            tx.commit().unwrap();
        }
    }
}

fn merge(from: &Connection, to: &mut Connection, pfx: &str, count: usize, offset: usize) {
    let mut prepped = from
        .prepare_cached("SELECT * FROM crsql_changes WHERE db_version = ?")
        .unwrap();
    let col_count = prepped.column_count();
    let mut rows = prepped.query((offset / count + 1,)).unwrap();
    let tx = to.transaction().unwrap();

    let insert = format!(
        "INSERT INTO crsql_changes VALUES ({})",
        (0..col_count)
            .map(|_| "?".to_owned())
            .collect::<Vec<_>>()
            .join(", ")
    );

    loop {
        let row = rows.next().unwrap();
        let row = match row {
            Some(row) => row,
            None => break,
        };

        let mut prepped = tx.prepare_cached(&insert).unwrap();
        prepped
            .execute(rusqlite::params_from_iter(
                (0..col_count).map(|i| Value::from(row.get_ref_unwrap(i))),
            ))
            .unwrap();
    }
    tx.commit().unwrap();
}

fn normal_insert(from: &Connection, to: &mut Connection, pfx: &str, count: usize, offset: usize) {
    let mut prepped = from
        .prepare_cached("SELECT * FROM merge_control WHERE tx = ?")
        .unwrap();
    let col_count = prepped.column_count();
    let mut rows = prepped.query((offset / count,)).unwrap();
    let tx = to.transaction().unwrap();

    loop {
        let row = rows.next().unwrap();
        let row = match row {
            Some(row) => row,
            None => break,
        };

        let mut prepped = tx
            .prepare_cached("INSERT OR REPLACE INTO merge_control VALUES (?, ?, ?)")
            .unwrap();
        prepped
            .execute(rusqlite::params_from_iter(
                (0..col_count).map(|i| Value::from(row.get_ref_unwrap(i))),
            ))
            .unwrap();
    }
    tx.commit().unwrap();
}

fn random_str() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from) // From link above, this is needed in later versions
        .collect()
}

fn insert(conn: &mut Connection, pfx: &str, count: usize, offset: usize, use_ts: bool) {

    for i in 0..count {
        let tx = conn.transaction().unwrap();
        if use_ts && pfx == "" {
            let sql = format!("SELECT crsql_set_ts('{}')", i + offset);
            tx.query_row(&sql, (), |row| row.get::<_, String>(0))
                .unwrap();
        }
        tx.execute(
            &format!("INSERT INTO {pfx}user VALUES (?, ?)"),
            (i + offset, random_str()),
        )
        .unwrap();
        tx.execute(
            &format!("INSERT INTO {pfx}deck VALUES (?, ?, ?)"),
            (i + offset, i + offset, random_str()),
        )
        .unwrap();
        tx.execute(
            &format!("INSERT INTO {pfx}slide VALUES (?, ?, ?)"),
            (i + offset, i + offset, i),
        )
        .unwrap();
        tx.execute(
            &format!("INSERT INTO {pfx}component VALUES (?, ?, ?, ?)"),
            (i + offset, "text", i + offset, random_str()),
        )
        .unwrap();
        tx.commit().unwrap();
    }

}

// def update(pfx, count, offset):
//   for i in range(count):
//     c.execute("UPDATE {pfx}user SET name = ? WHERE id = ?".format(pfx = pfx), (random_str(), i + offset))
//     c.execute("UPDATE {pfx}deck SET title = ? WHERE id = ?".format(pfx = pfx), (random_str(), i + offset))
//     c.execute("UPDATE {pfx}slide SET \"order\" = ? WHERE id = ?".format(pfx = pfx), (i + 1, i + offset))
//     c.execute("UPDATE {pfx}component SET content = ? WHERE id = ?".format(pfx = pfx), (random_str(), i + offset))
//     if c.in_transaction:
//       c.commit()

fn update(conn: &mut Connection, pfx: &str, count: usize, offset: usize, use_ts: bool) {
    let tx = conn.transaction().unwrap();
    for i in 0..count {
        if use_ts && pfx == "" {
            let sql = format!("SELECT crsql_set_ts('{}')", i + offset);
            tx.query_row(&sql, (), |row| row.get::<_, String>(0))
                .unwrap();
        }
        tx.execute(
            &format!("UPDATE {pfx}user SET name = ? WHERE id = ?"),
            (random_str(), i + offset),
        )
        .unwrap();
        tx.execute(
            &format!("UPDATE {pfx}deck SET title = ? WHERE id = ?"),
            (random_str(), i + offset),
        )
        .unwrap();
        tx.execute(
            &format!("UPDATE {pfx}slide SET \"order\" = ? WHERE id = ?"),
            (i + 1, i + offset),
        )
        .unwrap();
        tx.execute(
            &format!("UPDATE {pfx}component SET content = ? WHERE id = ?"),
            (random_str(), i + offset),
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn single_stmt_insert(conn: &mut Connection, pfx: &str, count: usize, offset: usize) {
    let offset = offset + 1000000;
    let values = (0..count)
        .map(|i| format!("({}, '{}', {}, '{}')", i + offset, "text", i, random_str()))
        .collect::<Vec<_>>();
    let tx = conn.transaction().unwrap();
    // tx.query_row("SELECT crsql_set_ts('18446744073709551615')", (), |row| row.get::<_, String>(0))
    //     .unwrap();
    tx.execute_batch(&format!(
        "INSERT INTO {pfx}component VALUES {values}",
        values = values.join(", ")
    ))
    .unwrap();
    tx.commit().unwrap();
}

fn read_changes(conn: &Connection, pfx: &str, count: usize, offset: usize) {
    for i in 0..count {
        if pfx == "v" {
            let mut prepped = conn
                .prepare_cached("SELECT id FROM component WHERE id > ? LIMIT 100")
                .unwrap();
            let mut rows = prepped.query((i + offset,)).unwrap();
            while let Ok(Some(_row)) = rows.next() {
                // drained!
            }
        } else {
            let mut prepped = conn
                .prepare_cached(
                    "SELECT db_version FROM crsql_changes WHERE db_version > ? LIMIT 100",
                )
                .unwrap();
            let mut rows = prepped.query((i + offset,)).unwrap();
            while let Ok(Some(_row)) = rows.next() {
                // drained!
            }
        }
    }
}
