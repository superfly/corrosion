use alloc::string::String;
use alloc::string::ToString;
use core::ffi::c_int;
use sqlite::sqlite3;
use sqlite::value;
use sqlite::Context;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;

use crate::{c::crsql_ExtData, tableinfo::TableInfo};

use super::bump_seq;
use super::trigger_fn_preamble;

/**
 * crsql_after_insert("table", pk_values...)
 */
pub unsafe extern "C" fn x_crsql_after_insert(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let result = trigger_fn_preamble(ctx, argc, argv, |table_info, values, ext_data| {
        after_insert(ctx.db_handle(), ext_data, table_info, &values[1..])
    });

    match result {
        Ok(_) => {
            ctx.result_int64(0);
        }
        Err(msg) => {
            ctx.result_error(&msg);
        }
    }
}

fn after_insert(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &mut TableInfo,
    pks_new: &[*mut value],
) -> Result<ResultCode, String> {
    let ts = unsafe { (*ext_data).timestamp.to_string() };

    let db_version = crate::db_version::next_db_version(db, ext_data)?;
    let (create_record_existed, key_new) = tbl_info
        .get_or_create_key_for_insert(db, pks_new)
        .map_err(|_| "failed getting or creating lookaside key")?;

    let cl = if tbl_info.non_pks.is_empty() {
        let seq = bump_seq(ext_data);
        // just a sentinel record
        let cl = super::mark_new_pk_row_created(db, tbl_info, key_new, db_version, seq, &ts)?;
        Some(cl)
    } else {
        let cl = if create_record_existed {
            // update the create record since it already exists.
            let seq = bump_seq(ext_data);
            update_create_record(db, tbl_info, key_new, db_version, seq, &ts)?
        } else {
            None
        };
        super::mark_locally_inserted(db, ext_data, tbl_info, key_new, db_version, &ts)?;
        cl
    };

    if let Some(cl) = cl {
        tbl_info.set_cl(key_new, cl);
    }

    Ok(ResultCode::OK)
}

fn update_create_record(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    db_version: sqlite::int64,
    seq: i32,
    ts: &str,
) -> Result<Option<i64>, String> {
    let update_create_record_stmt_ref = tbl_info
        .get_maybe_mark_locally_reinserted_stmt(db)
        .map_err(|_e| "failed to get update_create_record_stmt")?;
    let update_create_record_stmt = update_create_record_stmt_ref
        .as_ref()
        .ok_or("Failed to deref update_create_record_stmt")?;

    update_create_record_stmt
        .bind_int64(1, db_version)
        .and_then(|_| update_create_record_stmt.bind_int(2, seq))
        .and_then(|_| update_create_record_stmt.bind_text(3, ts, sqlite::Destructor::STATIC))
        .and_then(|_| update_create_record_stmt.bind_int64(4, new_key))
        .and_then(|_| {
            update_create_record_stmt.bind_text(
                5,
                crate::c::INSERT_SENTINEL,
                sqlite::Destructor::STATIC,
            )
        })
        .map_err(|_e| "failed binding to update_create_record_stmt")?;

    let res = update_create_record_stmt.step();
    let result = match res {
        Ok(ResultCode::ROW) => {
            let col_version = update_create_record_stmt.column_int64(0);
            Ok(Some(col_version))
        }
        Ok(ResultCode::DONE) => Ok(None),
        _ => Err("failed to step update_create_record_stmt".to_string()),
    };
    super::reset_cached_stmt(update_create_record_stmt.stmt)
        .map_err(|_e| "failed to reset cached stmt")?;
    result
}
