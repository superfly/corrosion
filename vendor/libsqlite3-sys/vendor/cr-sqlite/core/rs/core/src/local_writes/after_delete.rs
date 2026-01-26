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
use super::mark_locally_deleted;
use super::trigger_fn_preamble;

/**
 * crsql_after_delete("table", old_pk_values...)
 */
pub unsafe extern "C" fn x_crsql_after_delete(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let result = trigger_fn_preamble(ctx, argc, argv, |table_info, values, ext_data| {
        after_delete(ctx.db_handle(), ext_data, table_info, &values[1..])
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

fn after_delete(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &mut TableInfo,
    pks_old: &[*mut value],
) -> Result<ResultCode, String> {
    let ts = unsafe { (*ext_data).timestamp.to_string() };
    let db_version = crate::db_version::next_db_version(db, ext_data)?;
    let seq = bump_seq(ext_data);
    let key = tbl_info
        .get_or_create_key_via_raw_values(db, pks_old)
        .map_err(|_| "failed getting or creating lookaside key")?;

    let cl = mark_locally_deleted(db, tbl_info, key, db_version, seq, &ts)?;

    {
        // now actually delete the row metadata
        let drop_clocks_stmt_ref = tbl_info
            .get_merge_delete_drop_clocks_stmt(db)
            .map_err(|_e| "failed to get mark_locally_deleted_stmt")?;
        let drop_clocks_stmt = drop_clocks_stmt_ref
            .as_ref()
            .ok_or("Failed to deref sentinel stmt")?;

        drop_clocks_stmt
            .bind_int64(1, key)
            .map_err(|_e| "failed to bind pks to drop_clocks_stmt")?;
        super::step_trigger_stmt(drop_clocks_stmt)?;
    }

    tbl_info.set_cl(key, cl);

    Ok(ResultCode::OK)
}
