extern crate alloc;
use alloc::{ffi::CString, format, string::String};
use core::ffi::c_char;
use crsql_bundle::test_exports;
use sqlite::{Connection, ResultCode};
use sqlite_nostd::{self as sqlite, ManagedStmt};

fn make_site() -> *mut c_char {
    let inner_ptr: *mut c_char = CString::new("0000000000000000").unwrap().into_raw();
    inner_ptr
}

fn get_site_id(db: *mut sqlite::sqlite3) -> *mut c_char {
    let stmt = db
        .prepare_v2("SELECT crsql_site_id();")
        .expect("failed to prepare crsql_site_id stmt");

    stmt.step().expect("failed to execute crsql_site_id query");

    let blob_ptr = stmt.column_blob(0).expect("failed to get site_id");

    // use vec_unchecked because `new` errors if there's a 0 byte in the vec.
    let cstring = unsafe { CString::from_vec_unchecked(blob_ptr.to_vec()) };
    cstring.into_raw() as *mut c_char
}

fn test_fetch_db_version_from_storage() -> Result<ResultCode, String> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    let raw_db = db.db;

    let site_id = get_site_id(raw_db);

    let ext_data = unsafe { test_exports::c::crsql_newExtData(raw_db) };
    let rc = unsafe { test_exports::c::crsql_initSiteIdExt(raw_db, ext_data, site_id) };
    assert_eq!(rc, 0);

    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // no clock tables, no version.
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // this was a bug where calling twice on a fresh db would fail the second
    // time.
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // should still return same data on a subsequent call with no schema
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // create some schemas
    db.exec_safe("CREATE TABLE foo (a primary key not null, b);")
        .expect("made foo");
    db.exec_safe("SELECT crsql_as_crr('foo');")
        .expect("made foo crr");
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // still v0 since no rows are inserted
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // version is bumped due to insert
    db.exec_safe("INSERT INTO foo (a, b) VALUES (1, 2);")
        .expect("inserted");
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(1, unsafe { (*ext_data).dbVersion });

    db.exec_safe("CREATE TABLE bar (a primary key not null, b);")
        .expect("created bar");
    db.exec_safe("SELECT crsql_as_crr('bar');")
        .expect("bar as crr");
    db.exec_safe("INSERT INTO bar VALUES (1, 2)")
        .expect("inserted into bar");

    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(2, unsafe { (*ext_data).dbVersion });

    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(2, unsafe { (*ext_data).dbVersion });

    unsafe {
        test_exports::c::crsql_freeExtData(ext_data);
    };

    Ok(ResultCode::OK)
}

fn test_next_db_version() -> Result<(), String> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    let raw_db = db.db;
    let ext_data = unsafe { test_exports::c::crsql_newExtData(raw_db) };
    let rc = unsafe { test_exports::c::crsql_initSiteIdExt(raw_db, ext_data, make_site()) };
    assert_eq!(rc, 0);

    // is current + 1
    // doesn't bump forward on successive calls
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data)?
    );
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data)?
    );
    // doesn't roll back with new provideds
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data)?
    );
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data)?
    );

    // existing db version not touched
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    unsafe {
        test_exports::c::crsql_freeExtData(ext_data);
    };
    Ok(())
}

fn test_get_or_set_site_ordinal() -> Result<(), ResultCode> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    db.db
        .exec_safe("CREATE TABLE foo (a primary key not null, b);")?;

    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;

    db.db.exec_safe("BEGIN TRANSACTION;")?;

    let other_site_id = "other_site_id".as_bytes();

    let update_ordinal_stmt = db
        .db
        .prepare_v2("INSERT OR REPLACE INTO crsql_site_id (site_id, ordinal) VALUES (?, ?);")?;

    update_ordinal_stmt.bind_blob(1, other_site_id, sqlite::Destructor::STATIC)?;
    update_ordinal_stmt.bind_int64(2, 2)?;
    update_ordinal_stmt.step()?;

    // test ordinal is set
    assert_eq!(2, get_cache_ordinal(db.db, other_site_id)?);

    let delete_ordinal_stmt = db.prepare_v2("DELETE FROM crsql_site_id WHERE site_id = ?;")?;
    delete_ordinal_stmt.bind_blob(1, other_site_id, sqlite::Destructor::STATIC)?;
    delete_ordinal_stmt.step()?;

    assert_eq!(-1, get_cache_ordinal(db.db, other_site_id)?);

    db.db.exec_safe("SAVEPOINT test;")?;

    // new site_id in crsql_changes table
    let pk: [u8; 3] = [1, 9, 1];
    let site_id3 = "second_site_id".as_bytes();
    let stmt = db
        .db
        .prepare_v2("INSERT INTO crsql_changes VALUES ('foo', ?, 'b', 1, 1, 1, ?, 1, 0, 0);")?;
    stmt.bind_blob(1, &pk, sqlite::Destructor::STATIC)?;
    stmt.bind_blob(2, &site_id3, sqlite::Destructor::STATIC)?;
    stmt.step()?;
    stmt.reset()?;

    assert_eq!(1, get_cache_ordinal(db.db, site_id3)?);
    db.db.exec_safe("RELEASE SAVEPOINT test;")?;

    assert_eq!(1, get_cache_ordinal(db.db, site_id3)?);

    db.db.exec_safe("SAVEPOINT test;")?;
    let pk: [u8; 3] = [1, 9, 2];
    let site_id4 = "third_site_id".as_bytes();
    stmt.bind_blob(1, &pk, sqlite::Destructor::STATIC)?;
    stmt.bind_blob(2, &site_id4, sqlite::Destructor::STATIC)?;
    stmt.step()?;

    assert_eq!(1, get_cache_ordinal(db.db, site_id3)?);
    assert_eq!(2, get_cache_ordinal(db.db, site_id4)?);

    // sp rollback (when crsql_changes vtab is called) clears the cache
    db.db.exec_safe("ROLLBACK TO SAVEPOINT test;")?;
    assert_eq!(-1, get_cache_ordinal(db.db, site_id4)?);

    db.db.exec_safe("COMMIT;")?;

    Ok(())
}

fn test_get_or_set_pk_cl() -> Result<(), ResultCode> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    db.db
        .exec_safe("CREATE TABLE foo (a primary key not null, b);")?;

    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;

    let insert_foo_stmt = db.db.prepare_v2("INSERT INTO foo VALUES (?, ?);")?;

    let get_pk_key_stmt = db
        .db
        .prepare_v2("SELECT __crsql_key from foo__crsql_pks where a = ?;")?;

    let get_cache_cl_stmt = db.db.prepare_v2("SELECT crsql_cache_pk_cl(?, ?);")?;

    db.db.exec_safe("BEGIN TRANSACTION;")?;

    // insert row, pl cache doesn't get updated on new insert

    insert_foo_row(&insert_foo_stmt, 1, "b")?;

    insert_foo_row(&insert_foo_stmt, 2, "c")?;

    insert_foo_row(&insert_foo_stmt, 4, "d")?;

    let key1 = get_pk_key(&get_pk_key_stmt, 1).expect("get pk key");
    let key2 = get_pk_key(&get_pk_key_stmt, 2).expect("get pk key");

    let delete_foo_stmt = db
        .db
        .prepare_v2("DELETE FROM foo WHERE a = ?;")
        .expect("prepare delete foo");
    delete_foo_stmt.bind_int64(1, 1)?;
    delete_foo_stmt.step()?;

    // replace pk 2 with pk 3
    let update_foo_stmt = db
        .db
        .prepare_v2("UPDATE foo SET a = ? WHERE a = ?;")
        .expect("prepare update foo");
    update_foo_stmt.bind_int64(1, 3)?;
    update_foo_stmt.bind_int64(2, 2)?;
    update_foo_stmt.step()?;

    // pk 1 and 2 should have a cl of 2 since they have been deleted
    assert_eq!(2, get_cache_cl(&get_cache_cl_stmt, "foo", key1)?);
    assert_eq!(2, get_cache_cl(&get_cache_cl_stmt, "foo", key2)?);

    // reinsert pk 2, check cl is 3
    insert_foo_row(&insert_foo_stmt, 2, "d")?;
    assert_eq!(3, get_cache_cl(&get_cache_cl_stmt, "foo", key2)?);

    // check insert or replace updates the cache
    let insert_or_replace = db
        .db
        .prepare_v2("INSERT OR REPLACE INTO foo VALUES (?, ?);")?;
    insert_or_replace.bind_int64(1, 4)?;
    insert_or_replace.bind_text(2, "c", sqlite::Destructor::STATIC)?;
    insert_or_replace.step()?;
    reset_cached_stmt(&insert_or_replace)?;

    // insert of pk with no clock row gets no update
    let key4 = get_pk_key(&get_pk_key_stmt, 4).expect("get pk key");
    assert_eq!(-1, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    db.db.exec_safe("COMMIT;")?;

    // commit clears the cache
    assert_eq!(-1, get_cache_cl(&get_cache_cl_stmt, "foo", key1)?);
    assert_eq!(-1, get_cache_cl(&get_cache_cl_stmt, "foo", key2)?);

    db.db.exec_safe("BEGIN TRANSACTION;")?;
    db.db.exec_safe("SAVEPOINT test;")?;

    // new site_id in crsql_changes table
    // pk number is 4
    let pk: [u8; 3] = [1, 9, 4];

    // insert should update the cache.
    insert_crsql_changes_row(db.db, &pk, "b", "e", 1, 1, 1).expect("insert crsql changes row");
    let key4 = get_pk_key(&get_pk_key_stmt, 4).expect("get pk key");
    assert_eq!(1, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    // a delete should also update the cache.
    insert_crsql_changes_row(db.db, &pk, "-1", "", 2, 2, 2)?;
    assert_eq!(2, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    // test that a resurrected cache would also get updated.
    insert_crsql_changes_row(db.db, &pk, "b", "f", 1, 3, 5)?;
    assert_eq!(5, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    // a lower cl should not update the cache
    insert_crsql_changes_row(db.db, &pk, "b", "e", 1, 3, 3)?;
    assert_eq!(5, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    db.db.exec_safe("ROLLBACK TO SAVEPOINT test;")?;

    assert_eq!(-1, get_cache_cl(&get_cache_cl_stmt, "foo", key4)?);

    Ok(())
}

fn insert_crsql_changes_row(
    db: *mut sqlite::sqlite3,
    pk: &[u8],
    cid: &str,
    val: &str,
    col_version: i64,
    db_version: i64,
    cl: i64,
) -> Result<(), ResultCode> {
    let stmt = db.prepare_v2(
        "INSERT INTO crsql_changes VALUES ('foo', ?, ?, ?, ?, ?, X'0000000000000000', ?, 0, 0);",
    )?;
    stmt.bind_blob(1, pk, sqlite::Destructor::STATIC)?;
    stmt.bind_text(2, cid, sqlite::Destructor::STATIC)?;
    if cid == "-1" {
        stmt.bind_null(3)?;
    } else {
        stmt.bind_text(3, val, sqlite::Destructor::STATIC)?;
    }
    stmt.bind_int64(4, col_version)?;
    stmt.bind_int64(5, db_version)?;
    stmt.bind_int64(6, cl)?;
    stmt.step()?;
    Ok(())
}

fn insert_foo_row(stmt: &ManagedStmt, col1: i64, col2: &str) -> Result<(), ResultCode> {
    stmt.bind_int64(1, col1)?;
    stmt.bind_text(2, col2, sqlite::Destructor::STATIC)?;
    stmt.step()?;

    reset_cached_stmt(stmt)?;
    Ok(())
}

fn get_pk_key(stmt: &ManagedStmt, pk_value: i64) -> Result<i64, ResultCode> {
    stmt.bind_int64(1, pk_value)?;
    let res = stmt.step()?;
    match res {
        ResultCode::ROW => {
            let key = stmt.column_int64(0);
            reset_cached_stmt(stmt)?;
            Ok(key)
        }
        _ => {
            reset_cached_stmt(stmt)?;
            Err(ResultCode::ERROR)
        }
    }
}

fn get_cache_cl(stmt: &ManagedStmt, table_name: &str, pk_key: i64) -> Result<i64, ResultCode> {
    stmt.bind_text(1, table_name, sqlite::Destructor::STATIC)
        .expect("bind table name");
    stmt.bind_int64(2, pk_key).expect("bind pk key");
    let res = stmt.step();
    match res {
        Ok(ResultCode::ROW) => {
            let key = stmt.column_int64(0);
            reset_cached_stmt(stmt).expect("reset cached stmt");
            Ok(key)
        }
        _ => {
            reset_cached_stmt(stmt).expect("reset cached stmt");
            Err(ResultCode::ERROR)
        }
    }
}

fn get_cache_ordinal(db: *mut sqlite::sqlite3, site_id: &[u8]) -> Result<i64, ResultCode> {
    let stmt = db.prepare_v2("SELECT crsql_cache_site_ordinal(?);")?;
    stmt.bind_blob(1, site_id, sqlite::Destructor::STATIC)?;
    stmt.step()?;
    Ok(stmt.column_int64(0))
}

pub fn run_suite() -> Result<(), String> {
    test_fetch_db_version_from_storage()?;
    test_next_db_version()?;
    test_get_or_set_site_ordinal()
        .map_err(|e| format!("test_get_or_set_site_ordinal failed: {:?}", e))?;
    test_get_or_set_pk_cl().map_err(|e| format!("test_get_or_set_pk_cl failed: {:?}", e))?;
    Ok(())
}

pub fn reset_cached_stmt(stmt: &ManagedStmt) -> Result<ResultCode, ResultCode> {
    stmt.clear_bindings()?;
    stmt.reset()
}
