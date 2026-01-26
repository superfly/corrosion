#![cfg_attr(not(test), no_std)]
#![feature(vec_into_raw_parts)]

// TODO: these pub mods are exposed for the integration testing
// we should re-export in a `test` mod such that they do not become public apis
mod alter;
mod automigrate;
mod backfill;
#[cfg(feature = "test")]
pub mod bootstrap;
#[cfg(not(feature = "test"))]
mod bootstrap;
#[cfg(feature = "test")]
pub mod c;
#[cfg(not(feature = "test"))]
mod c;
mod changes_vtab;
mod changes_vtab_read;
mod changes_vtab_write;
mod commit;
mod compare_values;
mod config;
mod consts;
mod create_cl_set_vtab;
mod create_crr;
#[cfg(feature = "test")]
pub mod db_version;
#[cfg(not(feature = "test"))]
mod db_version;
mod debug;
mod ext_data;
mod is_crr;
mod local_writes;
#[cfg(feature = "test")]
pub mod pack_columns;
#[cfg(not(feature = "test"))]
mod pack_columns;
mod sha;
mod stmt_cache;
#[cfg(feature = "test")]
pub mod tableinfo;
#[cfg(not(feature = "test"))]
mod tableinfo;
mod teardown;
#[cfg(feature = "test")]
pub mod test_exports;
mod triggers;
mod unpack_columns_vtab;
mod util;

use alloc::format;
use alloc::string::ToString;
use alloc::{borrow::Cow, boxed::Box, collections::BTreeMap, vec::Vec};
use core::ffi::c_char;
use core::mem;
use core::ptr::null_mut;
extern crate alloc;
use alter::crsql_compact_post_alter;
use automigrate::*;
use backfill::*;
use c::{crsql_freeExtData, crsql_initSiteIdExt, crsql_newExtData};
use config::{crsql_config_get, crsql_config_set};
use core::ffi::{c_int, c_void, CStr};
use create_crr::create_crr;
use db_version::{
    crsql_fill_db_version_if_needed, crsql_next_db_version, crsql_peek_next_db_version,
    insert_db_version,
};
use is_crr::*;
use local_writes::after_delete::x_crsql_after_delete;
use local_writes::after_insert::x_crsql_after_insert;
use local_writes::after_update::x_crsql_after_update;
use sqlite::{Destructor, ResultCode};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};
#[cfg(feature = "test")]
use tableinfo::TableInfo;
use tableinfo::{crsql_ensure_table_infos_are_up_to_date, is_table_compatible, pull_table_info};
use teardown::*;
use triggers::create_triggers;

pub use debug::debug_log;

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn crsql_as_table(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = sqlite::args!(argc, argv);
    let db = ctx.db_handle();
    let table = args[0].text();

    if let Err(_) = db.exec_safe("SAVEPOINT as_table;") {
        ctx.result_error("failed to start as_table savepoint");
        return;
    }

    if let Err(_) = crsql_as_table_impl(db, table) {
        ctx.result_error("failed to downgrade the crr");
        if let Err(_) = db.exec_safe("ROLLBACK") {
            // fine.
        }
        return;
    }

    if let Err(_) = db.exec_safe("RELEASE as_table;") {
        // fine
    }
}

fn crsql_as_table_impl(db: *mut sqlite::sqlite3, table: &str) -> Result<ResultCode, ResultCode> {
    remove_crr_clock_table_if_exists(db, table)?;
    remove_crr_triggers_if_exist(db, table)
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn sqlite3_crsqlcore_init(
    db: *mut sqlite::sqlite3,
    err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> *mut c_void {
    sqlite::EXTENSION_INIT2(api);

    let rc = db
        .create_function_v2(
            "crsql_set_debug",
            1,
            sqlite::UTF8 | sqlite::DIRECTONLY,
            None,
            Some(debug::x_crsql_set_debug),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_automigrate",
            -1,
            sqlite::UTF8,
            None,
            Some(crsql_automigrate),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_pack_columns",
            -1,
            sqlite::UTF8,
            None,
            Some(pack_columns::crsql_pack_columns),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_as_table",
            1,
            sqlite::UTF8,
            None,
            Some(crsql_as_table),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = unpack_columns_vtab::create_module(db).unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = create_cl_set_vtab::create_module(db).unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = crate::bootstrap::crsql_init_peer_tracking_table(db);
    if rc != ResultCode::OK as c_int {
        return null_mut();
    }

    let rc = crate::bootstrap::crsql_init_db_versions_table(db);
    if rc != ResultCode::OK as c_int {
        return null_mut();
    }

    let sync_bit_ptr = sqlite::malloc(mem::size_of::<c_int>()) as *mut c_int;
    unsafe {
        *sync_bit_ptr = 0;
    }
    // Function to allow us to disable triggers when syncing remote changes
    // to base tables.
    let rc = db
        .create_function_v2(
            "crsql_internal_sync_bit",
            -1,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(sync_bit_ptr as *mut c_void),
            Some(x_crsql_sync_bit),
            None,
            None,
            Some(crsql_sqlite_free),
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return null_mut();
    }

    let rc = crate::bootstrap::crsql_maybe_update_db(db, err_msg);
    if rc != ResultCode::OK as c_int {
        return null_mut();
    }

    // allocate ext data earlier in the init process because we need its
    // pointer to be available for the crsql_update_site_id function.
    let ext_data = unsafe { crsql_newExtData(db) };
    if ext_data.is_null() {
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_update_site_id",
            2,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_update_site_id),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    // TODO: convert this function to a proper rust function
    // and have rust free:
    // 1. site_id_buffer
    // 2. ext_data
    // automatically.

    let site_id_buffer =
        sqlite::malloc((consts::SITE_ID_LEN as usize) * mem::size_of::<*const c_char>());
    let rc = crate::bootstrap::crsql_init_site_id(db, site_id_buffer);
    if rc != ResultCode::OK as c_int {
        sqlite::free(site_id_buffer as *mut c_void);
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = unsafe { crsql_initSiteIdExt(db, ext_data, site_id_buffer as *mut c_char) };
    if rc != ResultCode::OK as c_int {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    if let Err(_) = crate::bootstrap::create_site_id_triggers(db) {
        sqlite::free(site_id_buffer as *mut c_void);
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_site_id",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_site_id),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_db_version",
            0,
            sqlite::INNOCUOUS | sqlite::UTF8,
            Some(ext_data as *mut c_void),
            Some(x_crsql_db_version),
            None,
            None,
            Some(x_free_connection_ext_data),
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_next_db_version",
            -1,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_next_db_version),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_peek_next_db_version",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_peek_next_db_version),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_sha",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            None,
            Some(x_crsql_sha),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_version",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            None,
            Some(x_crsql_version),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_increment_and_get_seq",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_increment_and_get_seq),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_get_seq",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_get_seq),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_as_crr",
            -1,
            sqlite::UTF8 | sqlite::DETERMINISTIC,
            None,
            Some(x_crsql_as_crr),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_set_ts",
            1,
            sqlite::UTF8 | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_set_ts),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    #[cfg(feature = "test")]
    if let Err(_) = db.create_function_v2(
        "crsql_cache_site_ordinal",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        Some(ext_data as *mut c_void),
        Some(x_crsql_cache_site_ordinal),
        None,
        None,
        None,
    ) {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    #[cfg(feature = "test")]
    if let Err(_) = db.create_function_v2(
        "crsql_cache_pk_cl",
        2,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        Some(ext_data as *mut c_void),
        Some(x_crsql_cache_pk_cl),
        None,
        None,
        None,
    ) {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_set_db_version",
            -1,
            sqlite::UTF8 | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_set_db_version),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_get_ts",
            -1,
            sqlite::UTF8 | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(x_crsql_get_ts),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_begin_alter",
            -1,
            sqlite::UTF8 | sqlite::DIRECTONLY,
            None,
            Some(x_crsql_begin_alter),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_commit_alter",
            -1,
            sqlite::UTF8 | sqlite::DIRECTONLY,
            Some(ext_data as *mut c_void),
            Some(x_crsql_commit_alter),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_finalize",
            -1,
            sqlite::UTF8 | sqlite::DIRECTONLY,
            Some(ext_data as *mut c_void),
            Some(x_crsql_finalize),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_after_update",
            -1,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_after_update),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_after_insert",
            -1,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_after_insert),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_after_delete",
            -1,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_after_delete),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_rows_impacted",
            0,
            sqlite::UTF8 | sqlite::INNOCUOUS,
            Some(ext_data as *mut c_void),
            Some(x_crsql_rows_impacted),
            None,
            None,
            None,
        )
        .unwrap_or(ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_config_set",
            2,
            sqlite::UTF8,
            Some(ext_data as *mut c_void),
            Some(crsql_config_set),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    let rc = db
        .create_function_v2(
            "crsql_config_get",
            1,
            sqlite::UTF8 | sqlite::INNOCUOUS | sqlite::DETERMINISTIC,
            Some(ext_data as *mut c_void),
            Some(crsql_config_get),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        unsafe { crsql_freeExtData(ext_data) };
        return null_mut();
    }

    return ext_data as *mut c_void;
}

/**
 * return the uuid which uniquely identifies this database.
 *
 * `select crsql_site_id()`
 */
unsafe extern "C" fn x_crsql_site_id(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let site_id = (*ext_data).siteId;
    sqlite::result_blob(ctx, site_id, consts::SITE_ID_LEN, Destructor::STATIC);
}

/**
 * update in-memory map of site ids to ordinals. Only valid within a transaction.
 *
 * `select crsql_update_site_id(site_id, ordinal)`
 */
unsafe extern "C" fn x_crsql_update_site_id(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let args = sqlite::args!(argc, argv);
    let site_id = args[0].blob();
    let ordinal = args[1].int64();
    let mut ordinals: mem::ManuallyDrop<Box<BTreeMap<Vec<u8>, i64>>> = mem::ManuallyDrop::new(
        Box::from_raw((*ext_data).ordinalMap as *mut BTreeMap<Vec<u8>, i64>),
    );

    if ordinal == -1 {
        ordinals.remove(&site_id.to_vec());
    } else {
        ordinals.insert(site_id.to_vec(), ordinal);
    }
    ctx.result_text_static("OK");
}

unsafe extern "C" fn x_crsql_finalize(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    c::crsql_finalize(ext_data);
    ctx.result_text_static("finalized");
}

/**
 * Takes a table name and turns it into a CRR.
 *
 * This allows users to create and modify tables as normal.
 */
unsafe extern "C" fn x_crsql_as_crr(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc == 0 {
        ctx.result_error(
            "Wrong number of args provided to crsql_as_crr. Provide the schema
          name and table name or just the table name.",
        );
        return;
    }

    let args = sqlite::args!(argc, argv);
    let (schema_name, table_name) = if argc == 2 {
        (args[0].text(), args[1].text())
    } else {
        ("main\0", args[0].text())
    };

    let db = ctx.db_handle();
    let mut err_msg = null_mut();
    let rc = db.exec_safe("SAVEPOINT as_crr");
    if rc.is_err() {
        ctx.result_error("failed to start as_crr savepoint");
        return;
    }

    let rc = crsql_create_crr(
        db,
        schema_name.as_ptr() as *const c_char,
        table_name.as_ptr() as *const c_char,
        0,
        0,
        &mut err_msg as *mut _,
    );
    if rc != ResultCode::OK as c_int {
        sqlite::result_error(ctx, err_msg, -1);
        sqlite::result_error_code(ctx, rc);
        let _ = db.exec_safe("ROLLBACK");
        return;
    }

    let rc = db.exec_safe("RELEASE as_crr");
    if rc.is_err() {
        ctx.result_error("failed to release as_crr savepoint");
        return;
    }
    ctx.result_text_static("OK");
}

unsafe extern "C" fn x_crsql_rows_impacted(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let rows_impacted = (*ext_data).rowsImpacted;
    sqlite::result_int(ctx, rows_impacted);
}

unsafe extern "C" fn x_crsql_begin_alter(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc == 0 {
        ctx.result_error(
            "Wrong number of args provided to crsql_begin_alter. Provide the
          schema name and table name or just the table name.",
        );
        return;
    }

    let args = sqlite::args!(argc, argv);
    // TODO: use schema name!
    let (_schema_name, table_name) = if argc == 2 {
        (args[0].text(), args[1].text())
    } else {
        ("main\0", args[0].text())
    };

    let db = ctx.db_handle();
    let rc = db.exec_safe("SAVEPOINT alter_crr");
    if rc.is_err() {
        ctx.result_error("failed to start alter_crr savepoint");
        return;
    }
    let rc = remove_crr_triggers_if_exist(db, table_name);
    if rc.is_err() {
        sqlite::result_error_code(ctx, rc.unwrap_err() as c_int);
        let _ = db.exec_safe("ROLLBACK");
        return;
    }
    ctx.result_text_static("OK");
}

unsafe extern "C" fn x_crsql_commit_alter(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc == 0 {
        ctx.result_error(
            "Wrong number of args provided to crsql_commit_alter. Provide the
          schema name and table name or just the table name.",
        );
        return;
    }

    let args = sqlite::args!(argc, argv);
    let (schema_name, table_name) = if argc >= 2 {
        (args[0].text(), args[1].text())
    } else {
        ("main\0", args[0].text())
    };

    let non_destructive = if argc >= 3 { args[2].int() == 1 } else { false };

    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let mut err_msg = null_mut();
    let db = ctx.db_handle();

    let rc = if non_destructive {
        match pull_table_info(db, table_name, &mut err_msg as *mut _) {
            Ok(table_info) => {
                match create_triggers(db, &table_info, &mut err_msg) {
                    Ok(ResultCode::OK) => {
                        // need to ensure the right table infos in ext data
                        crsql_ensure_table_infos_are_up_to_date(
                            db,
                            ext_data,
                            &mut err_msg as *mut _,
                        )
                    }
                    Ok(rc) | Err(rc) => rc as c_int,
                }
            }
            Err(rc) => rc as c_int,
        }
    } else {
        let rc = crsql_compact_post_alter(
            db,
            table_name.as_ptr() as *const c_char,
            ext_data,
            &mut err_msg as *mut _,
        );

        if rc == ResultCode::OK as c_int {
            crsql_create_crr(
                db,
                schema_name.as_ptr() as *const c_char,
                table_name.as_ptr() as *const c_char,
                1,
                0,
                &mut err_msg as *mut _,
            )
        } else {
            rc
        }
    };

    let rc = if rc == ResultCode::OK as c_int {
        db.exec_safe("RELEASE alter_crr")
            .unwrap_or(ResultCode::ERROR) as c_int
    } else {
        rc
    };
    if rc != ResultCode::OK as c_int {
        // TODO: use err_msg
        let error_str = if !err_msg.is_null() {
            unsafe { CStr::from_ptr(err_msg).to_string_lossy() }
        } else {
            Cow::Borrowed("Hello World")
        };
        ctx.result_error(&format!(
            "failed compacting tables post alteration: {}",
            error_str
        ));
        let _ = db.exec_safe("ROLLBACK");
        return;
    }
}

unsafe extern "C" fn x_crsql_get_seq(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    ctx.result_int((*ext_data).seq);
}

unsafe extern "C" fn x_crsql_increment_and_get_seq(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    ctx.result_int((*ext_data).seq);
    (*ext_data).seq += 1;
}

/**
 * Set the timestamp for the current transaction.
 */
unsafe extern "C" fn x_crsql_set_ts(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc == 0 {
        ctx.result_error("Wrong number of args provided to x_crsql_set_ts. Provide the timestamp.");
        return;
    }

    let args = sqlite::args!(argc, argv);
    let ts = args[0].text();
    // we expect a string that we can parse as a u64
    let ts_u64 = match ts.parse::<u64>() {
        Ok(ts_u64) => ts_u64,
        Err(_) => {
            ctx.result_error("Timestamp cannot be parsed as a valid u64");
            return;
        }
    };
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    (*ext_data).timestamp = ts_u64;
    ctx.result_text_static("OK");
}

/**
 * Get the site ordinal cached in the ext data for the current transaction.
 * only used for test to inspect the ordinal map.
 */
#[cfg(feature = "test")]
unsafe extern "C" fn x_crsql_cache_site_ordinal(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc == 0 {
        ctx.result_error(
            "Wrong number of args provided to crsql_cache_site_ordinal. Provide the site id.",
        );
        return;
    }

    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let args = sqlite::args!(argc, argv);
    let site_id = args[0].blob();

    let ord_map = mem::ManuallyDrop::new(Box::from_raw(
        (*ext_data).ordinalMap as *mut BTreeMap<Vec<u8>, i64>,
    ));
    let res = ord_map.get(site_id).cloned().unwrap_or(-1);
    sqlite::result_int64(ctx, res);
}

/**
 * Get the pk cl cached in the ext data for the current transaction.
 * only used for test to inspect the cl cache.
 */
#[cfg(feature = "test")]
unsafe extern "C" fn x_crsql_cache_pk_cl(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc < 2 {
        ctx.result_error(
            "Wrong number of args provided to crsql_cache_pk_cl. Provide the table name and pk key.",
        );
        return;
    }

    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let args = sqlite::args!(argc, argv);
    let table_name = args[0].text();
    let pk_key = args[1].int64();

    let table_infos =
        mem::ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>));
    let table_info = table_infos.iter().find(|t| t.tbl_name == table_name);

    if let Some(table_info) = table_info {
        let cl = table_info.get_cl(pk_key).cloned().unwrap_or(-1);
        sqlite::result_int64(ctx, cl);
    } else {
        ctx.result_error("table not found");
    }
}

/**
 * Return the timestamp for the current transaction.
 */
unsafe extern "C" fn x_crsql_get_ts(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let ts = (*ext_data).timestamp.to_string();
    ctx.result_text_transient(&ts);
}

/**
 * Return the current version of the database.
 *
 * `select crsql_db_version()`
 */
unsafe extern "C" fn x_crsql_db_version(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let db = ctx.db_handle();
    let mut err_msg = null_mut();
    let rc = crsql_fill_db_version_if_needed(db, ext_data, &mut err_msg as *mut _);
    if rc != ResultCode::OK as c_int {
        // TODO: pass err_msg!
        ctx.result_error("failed to fill db version");
        return;
    }
    sqlite::result_int64(ctx, (*ext_data).dbVersion);
}

/**
 * Return the next version of the database for use in inserts/updates/deletes
 *
 * `select crsql_next_db_version()`
 *
 * Nit: this should be same as `crsql_db_version`
 * If you change this behavior you need to change trigger behaviors
 * as each invocation to `nextVersion` should return the same version
 * when in the same transaction.
 */
unsafe extern "C" fn x_crsql_next_db_version(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let db = ctx.db_handle();
    let mut err_msg = null_mut();

    let ret = crsql_next_db_version(db, ext_data, &mut err_msg as *mut _);
    if ret < 0 {
        // TODO: use err_msg!
        ctx.result_error("Unable to determine the next db version");
        return;
    }

    ctx.result_int64(ret);
}

/**
 * Return the next version of the database for use in inserts/updates/deletes
 *
 * `select crsql_set_db_version()`
 *
 * This is used to set the db version for a particular site_id.
 * This is useful for exposing a way for the application to set a db_version
 * that might not get passed to crsqlite.
 */
unsafe extern "C" fn x_crsql_set_db_version(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    if argc < 2 {
        ctx.result_error(
            "Wrong number of args provided to crsql_set_db_version. Provide the
        site id and db version.",
        );
        return;
    }

    let args = sqlite::args!(argc, argv);
    let (site_id, db_version) = (args[0].blob(), args[1].int64());
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;

    let ret = insert_db_version(ext_data, site_id, db_version);
    if ret.is_err() {
        ctx.result_error("Unable to set the db version");
        return;
    }
    ctx.result_text_static("OK");
}

/**
 * Return the next version of the database for use in inserts/updates/deletes
 * without updating the the database or value in ext_data.
 *
 * `select crsql_peek_next_db_version()`
 */
unsafe extern "C" fn x_crsql_peek_next_db_version(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    let ext_data = ctx.user_data() as *mut c::crsql_ExtData;
    let db = ctx.db_handle();
    let mut err_msg = null_mut();

    let ret = crsql_peek_next_db_version(db, ext_data, &mut err_msg as *mut _);
    if ret < 0 {
        // TODO: use err_msg!
        ctx.result_error("Unable to determine the next db version");
        return;
    }

    ctx.result_int64(ret);
}

/**
 * The sha of the commit that this version of crsqlite was built from.
 */
unsafe extern "C" fn x_crsql_sha(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    ctx.result_text_static(sha::SHA);
}

unsafe extern "C" fn x_crsql_version(
    ctx: *mut sqlite::context,
    _argc: i32,
    _argv: *mut *mut sqlite::value,
) {
    ctx.result_int64(consts::CRSQLITE_VERSION as i64);
}

unsafe extern "C" fn x_free_connection_ext_data(data: *mut c_void) {
    let ext_data = data as *mut c::crsql_ExtData;
    crsql_freeExtData(ext_data);
}

pub unsafe extern "C" fn crsql_sqlite_free(ptr: *mut c_void) {
    sqlite::free(ptr);
}

unsafe extern "C" fn x_crsql_sync_bit(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let sync_bit_ptr = ctx.user_data() as *mut c_int;
    if argc != 1 {
        ctx.result_int(*sync_bit_ptr);
        return;
    }

    let args = sqlite::args!(argc, argv);
    let new_value = args[0].int();
    *sync_bit_ptr = new_value;

    ctx.result_int(*sync_bit_ptr);
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn crsql_is_crr(db: *mut sqlite::sqlite3, table: *const c_char) -> c_int {
    if let Ok(table) = unsafe { CStr::from_ptr(table).to_str() } {
        match is_crr(db, table) {
            Ok(b) => {
                if b {
                    1
                } else {
                    0
                }
            }
            Err(c) => (c as c_int) * -1,
        }
    } else {
        (ResultCode::NOMEM as c_int) * -1
    }
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn crsql_is_table_compatible(
    db: *mut sqlite::sqlite3,
    table: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    if let Ok(table) = unsafe { CStr::from_ptr(table).to_str() } {
        is_table_compatible(db, table, err)
            .map(|x| x as c_int)
            .unwrap_or_else(|err| (err as c_int) * -1)
    } else {
        (ResultCode::NOMEM as c_int) * -1
    }
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn crsql_create_crr(
    db: *mut sqlite::sqlite3,
    schema: *const c_char,
    table: *const c_char,
    is_commit_alter: c_int,
    no_tx: c_int,
    err: *mut *mut c_char,
) -> c_int {
    let schema = unsafe { CStr::from_ptr(schema).to_str() };
    let table = unsafe { CStr::from_ptr(table).to_str() };

    match (table, schema) {
        (Ok(table), Ok(schema)) => {
            create_crr(db, schema, table, is_commit_alter != 0, no_tx != 0, err)
                .unwrap_or_else(|err| err) as c_int
        }
        _ => ResultCode::NOMEM as c_int,
    }
}
