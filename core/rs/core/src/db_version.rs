use core::ffi::c_void;
use core::mem;

use crate::alloc::string::ToString;
use crate::alloc::{boxed::Box, vec::Vec};
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use core::ffi::{c_char, c_int};
use sqlite::ResultCode;
use sqlite::StrRef;
use sqlite::{sqlite3, Stmt};
use sqlite_nostd as sqlite;

use crate::c::crsql_ExtData;
use crate::c::crsql_fetchPragmaDataVersion;
use crate::consts::MIN_POSSIBLE_DB_VERSION;
use crate::consts::SITE_ID_LEN;
use crate::stmt_cache::reset_cached_stmt;

#[no_mangle]
pub extern "C" fn crsql_fill_db_version_if_needed(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    errmsg: *mut *mut c_char,
) -> c_int {
    match fill_db_version_if_needed(db, ext_data) {
        Ok(rc) => rc as c_int,
        Err(msg) => {
            errmsg.set(&msg);
            ResultCode::ERROR as c_int
        }
    }
}

#[no_mangle]
pub extern "C" fn crsql_next_db_version(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    errmsg: *mut *mut c_char,
) -> sqlite::int64 {
    match next_db_version(db, ext_data) {
        Ok(version) => version,
        Err(msg) => {
            errmsg.set(&msg);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn crsql_peek_next_db_version(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    errmsg: *mut *mut c_char,
) -> sqlite::int64 {
    match peek_next_db_version(db, ext_data) {
        Ok(version) => version,
        Err(msg) => {
            errmsg.set(&msg);
            -1
        }
    }
}

pub fn peek_next_db_version(db: *mut sqlite3, ext_data: *mut crsql_ExtData) -> Result<i64, String> {
    fill_db_version_if_needed(db, ext_data)?;

    let mut ret = unsafe { (*ext_data).dbVersion + 1 };
    if ret < unsafe { (*ext_data).pendingDbVersion } {
        ret = unsafe { (*ext_data).pendingDbVersion };
    }
    Ok(ret)
}

/**
 * Given this needs to do a pragma check, invoke it as little as possible.
 * TODO: We could optimize to only do a pragma check once per transaction.
 * Need to save some bit that states we checked the pragma already and reset on tx commit or rollback.
 */
pub fn next_db_version(db: *mut sqlite3, ext_data: *mut crsql_ExtData) -> Result<i64, String> {
    fill_db_version_if_needed(db, ext_data)?;

    let mut ret = unsafe { (*ext_data).dbVersion + 1 };
    if ret < unsafe { (*ext_data).pendingDbVersion } {
        ret = unsafe { (*ext_data).pendingDbVersion };
    }

    // update db_version in db if it changed
    if ret != unsafe { (*ext_data).pendingDbVersion } {
        unsafe {
            let site_id_slice =
                core::slice::from_raw_parts((*ext_data).siteId, SITE_ID_LEN as usize);

            let bind_result = (*ext_data)
                .pSetDbVersionStmt
                .bind_blob(1, site_id_slice, sqlite_nostd::Destructor::STATIC)
                .and_then(|_| (*ext_data).pSetDbVersionStmt.bind_int64(2, ret));

            if bind_result.is_err() {
                return Err("failed binding to pSetDbVersionStmt".into());
            }

            let res = (*ext_data).pSetDbVersionStmt.step();

            reset_cached_stmt((*ext_data).pSetDbVersionStmt)
                .map_err(|_| "failed to reset cached pSetDbVersionStmt")?;

            if res.is_err() {
                return Err("failed to insert db_version for current site ID".into());
            }
            (*ext_data).pendingDbVersion = ret;
        }
    }

    Ok(ret)
}

pub fn fill_db_version_if_needed(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
) -> Result<ResultCode, String> {
    unsafe {
        let rc = crsql_fetchPragmaDataVersion(db, ext_data);
        if rc == -1 {
            return Err("failed to fetch PRAGMA data_version".to_string());
        }
        if (*ext_data).dbVersion != -1 && rc == 0 {
            return Ok(ResultCode::OK);
        }
        fetch_db_version_from_storage(db, ext_data)
    }
}

pub fn fetch_db_version_from_storage(
    _db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
) -> Result<ResultCode, String> {
    unsafe {
        let site_id_slice = core::slice::from_raw_parts((*ext_data).siteId, SITE_ID_LEN as usize);

        let db_version_stmt = (*ext_data).pDbVersionStmt;

        let bind_result = (*ext_data).pDbVersionStmt.bind_blob(
            1,
            site_id_slice,
            sqlite_nostd::Destructor::STATIC,
        );

        if bind_result.is_err() {
            return Err("failed binding to db_version_stmt".into());
        }
        let rc = db_version_stmt.step();
        match rc {
            // no rows? We're a fresh db with the min starting version
            Ok(ResultCode::DONE) => {
                db_version_stmt.reset().or_else(|rc| {
                    Err(format!(
                        "failed to reset db version stmt after DONE: {}",
                        rc
                    ))
                })?;
                (*ext_data).dbVersion = MIN_POSSIBLE_DB_VERSION;
                Ok(ResultCode::OK)
            }
            // got a row? It is our db version.
            Ok(ResultCode::ROW) => {
                (*ext_data).dbVersion = db_version_stmt.column_int64(0);
                db_version_stmt
                    .reset()
                    .or_else(|rc| Err(format!("failed to reset db version stmt after ROW: {}", rc)))
            }
            // Not row or done? Something went wrong.
            Ok(rc) | Err(rc) => {
                db_version_stmt.reset().or_else(|rc| {
                    Err(format!("failed to reset db version stmt after ROW: {}", rc))
                })?;
                Err(format!("failed to step db version stmt: {}", rc))
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn crsql_init_last_db_versions_map(ext_data: *mut crsql_ExtData) {
    let map: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    unsafe { (*ext_data).lastDbVersions = Box::into_raw(Box::new(map)) as *mut c_void }
}

#[no_mangle]
pub extern "C" fn crsql_init_ordinal_map(ext_data: *mut crsql_ExtData) {
    let map: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    unsafe { (*ext_data).ordinalMap = Box::into_raw(Box::new(map)) as *mut c_void }
}

#[no_mangle]
pub extern "C" fn crsql_drop_last_db_versions_map(ext_data: *mut crsql_ExtData) {
    unsafe {
        drop(Box::from_raw(
            (*ext_data).lastDbVersions as *mut BTreeMap<Vec<u8>, i64>,
        ));
    }
}

#[no_mangle]
pub extern "C" fn crsql_drop_ordinal_map(ext_data: *mut crsql_ExtData) {
    unsafe {
        drop(Box::from_raw(
            (*ext_data).ordinalMap as *mut BTreeMap<Vec<u8>, i64>,
        ));
    }
}

pub fn insert_db_version(
    ext_data: *mut crsql_ExtData,
    insert_site_id: &[u8],
    insert_db_vrsn: i64,
) -> Result<(), ResultCode> {
    unsafe {
        // we can get a more recent db_version
        let mut last_db_versions: mem::ManuallyDrop<Box<BTreeMap<Vec<u8>, i64>>> =
            mem::ManuallyDrop::new(Box::from_raw(
                (*ext_data).lastDbVersions as *mut BTreeMap<Vec<u8>, i64>,
            ));

        if let Some(db_v) = last_db_versions.get(insert_site_id) {
            if *db_v >= insert_db_vrsn {
                // already inserted a greater or equal db version!
                return Ok(());
            }
        }

        // ensure the site_id exists in the crsql_site_id table
        let ordinal = get_or_set_site_ordinal(ext_data, insert_site_id)?;
        if ordinal == 0 {
            // we manage our own db_version internally but only error if we get a bigger db_version
            // cause we can get our own rows from other nodes but the version should never be greater than our own.
            let db_version = (*ext_data).dbVersion;
            if insert_db_vrsn > db_version {
                return Err(ResultCode::ERROR);
            }
            return Ok(());
        }

        let bind_result = (*ext_data)
            .pSetDbVersionStmt
            .bind_blob(1, insert_site_id, sqlite::Destructor::STATIC)
            .and_then(|_| (*ext_data).pSetDbVersionStmt.bind_int64(2, insert_db_vrsn));

        if let Err(rc) = bind_result {
            reset_cached_stmt((*ext_data).pSetDbVersionStmt)?;
            return Err(rc);
        }
        match (*ext_data).pSetDbVersionStmt.step() {
            Ok(ResultCode::ROW) => {
                last_db_versions.insert(
                    insert_site_id.to_vec(),
                    (*ext_data).pSetDbVersionStmt.column_int64(0),
                );
            }
            Ok(_) => {}
            Err(rc) => {
                reset_cached_stmt((*ext_data).pSetDbVersionStmt)?;
                return Err(rc);
            }
        }
        reset_cached_stmt((*ext_data).pSetDbVersionStmt)?;
    }
    Ok(())
}

pub unsafe fn get_or_set_site_ordinal(
    ext_data: *mut crsql_ExtData,
    site_id: &[u8],
) -> Result<i64, ResultCode> {
    // check the cache first
    let mut ordinals: mem::ManuallyDrop<Box<BTreeMap<Vec<u8>, i64>>> = mem::ManuallyDrop::new(
        Box::from_raw((*ext_data).ordinalMap as *mut BTreeMap<Vec<u8>, i64>),
    );

    if let Some(ordinal) = ordinals.get(site_id) {
        return Ok(*ordinal);
    }

    let bind_result =
        (*ext_data)
            .pSelectSiteIdOrdinalStmt
            .bind_blob(1, site_id, sqlite::Destructor::STATIC);

    if let Err(rc) = bind_result {
        reset_cached_stmt((*ext_data).pSelectSiteIdOrdinalStmt)?;
        return Err(rc);
    }

    let ordinal = match (*ext_data).pSelectSiteIdOrdinalStmt.step() {
        Ok(ResultCode::ROW) => {
            let ordinal = (*ext_data).pSelectSiteIdOrdinalStmt.column_int64(0);
            reset_cached_stmt((*ext_data).pSelectSiteIdOrdinalStmt)?;
            ordinal
        }
        Ok(_) => {
            reset_cached_stmt((*ext_data).pSelectSiteIdOrdinalStmt)?;
            // site id had no ordinal yet.
            // set one and return the ordinal.
            let bind_result =
                (*ext_data)
                    .pSetSiteIdOrdinalStmt
                    .bind_blob(1, site_id, sqlite::Destructor::STATIC);

            if let Err(rc) = bind_result {
                reset_cached_stmt((*ext_data).pSetSiteIdOrdinalStmt)?;
                return Err(rc);
            }

            match (*ext_data).pSetSiteIdOrdinalStmt.step() {
                Ok(ResultCode::DONE) => {
                    reset_cached_stmt((*ext_data).pSetSiteIdOrdinalStmt)?;
                    return Err(ResultCode::ABORT);
                }
                Ok(_) => {
                    let ordinal = (*ext_data).pSetSiteIdOrdinalStmt.column_int64(0);
                    reset_cached_stmt((*ext_data).pSetSiteIdOrdinalStmt)?;
                    ordinal
                }
                Err(rc) => {
                    reset_cached_stmt((*ext_data).pSetSiteIdOrdinalStmt)?;
                    return Err(rc);
                }
            }
        }
        Err(rc) => {
            reset_cached_stmt((*ext_data).pSetSiteIdOrdinalStmt)?;
            return Err(rc);
        }
    };
    ordinals.insert(site_id.to_vec(), ordinal);
    Ok(ordinal)
}
