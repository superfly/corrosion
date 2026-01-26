use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;

use crate::alloc::string::ToString;
use crate::c::crsql_ExtData;
use crate::stmt_cache::reset_cached_stmt;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use sqlite::sqlite3;
use sqlite::{Context, ManagedStmt, Value};
use sqlite_nostd::ResultCode;
use sqlite_nostd::{self as sqlite, changes64};

use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, ColumnInfo, TableInfo};

pub mod after_delete;
pub mod after_insert;
pub mod after_update;

fn trigger_fn_preamble<F>(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    f: F,
) -> Result<ResultCode, String>
where
    F: Fn(&mut TableInfo, &[*mut sqlite::value], *mut crsql_ExtData) -> Result<ResultCode, String>,
{
    if argc < 1 {
        return Err("expected at least 1 argument".to_string());
    }

    let values = sqlite::args!(argc, argv);
    let ext_data = sqlite::user_data(ctx) as *mut crsql_ExtData;
    let mut inner_err: *mut c_char = core::ptr::null_mut();
    let outer_err: *mut *mut c_char = &mut inner_err;

    let rc = crsql_ensure_table_infos_are_up_to_date(ctx.db_handle(), ext_data, outer_err);
    if rc != ResultCode::OK as c_int {
        return Err(format!(
            "failed to ensure table infos are up to date: {}",
            rc
        ));
    }

    let mut table_infos =
        unsafe { ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>)) };
    let table_name = values[0].text();
    let table_info = match table_infos.iter_mut().find(|t| &(t.tbl_name) == table_name) {
        Some(t) => t,
        None => {
            return Err(format!("table {} not found", table_name));
        }
    };

    f(table_info, values, ext_data)
}

fn step_trigger_stmt(stmt: &ManagedStmt) -> Result<ResultCode, String> {
    match stmt.step() {
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(stmt.stmt)
                .map_err(|_e| "done -- unable to reset cached trigger stmt")?;
            Ok(ResultCode::OK)
        }
        Ok(code) | Err(code) => {
            reset_cached_stmt(stmt.stmt).map_err(|_e| {
                format!("error -- unable to reset cached trigger stmt, code {code}")
            })?;
            Err(format!(
                "unexpected result code from tigger_stmt.step: {}",
                code
            ))
        }
    }
}

fn mark_new_pk_row_created(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key_new: sqlite::int64,
    db_version: i64,
    seq: i32,
    ts: &str,
) -> Result<i64, String> {
    let mark_locally_created_stmt_ref = tbl_info
        .get_mark_locally_created_stmt(db)
        .map_err(|_e| "failed to get mark_locally_created_stmt")?;
    let mark_locally_created_stmt = mark_locally_created_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;

    mark_locally_created_stmt
        .bind_int64(1, key_new)
        .and_then(|_| mark_locally_created_stmt.bind_int64(2, db_version))
        .and_then(|_| mark_locally_created_stmt.bind_int(3, seq))
        .and_then(|_| mark_locally_created_stmt.bind_text(4, ts, sqlite::Destructor::STATIC))
        .map_err(|_| "failed binding to mark_locally_created_stmt")?;
    let stmt_res = mark_locally_created_stmt.step();
    let result = match stmt_res {
        Ok(ResultCode::ROW) => {
            let cl = mark_locally_created_stmt.column_int64(0);
            Ok(cl)
        }
        _ => Err("failed to step mark locally created stmt".to_string()),
    };
    reset_cached_stmt(mark_locally_created_stmt.stmt)
        .map_err(|_e| "failed to reset cached stmt")?;
    result
}

fn bump_seq(ext_data: *mut crsql_ExtData) -> c_int {
    unsafe {
        (*ext_data).seq += 1;
        (*ext_data).seq - 1
    }
}

#[allow(non_snake_case)]
fn mark_locally_inserted(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    db_version: sqlite::int64,
    ts: &str,
) -> Result<ResultCode, String> {
    let mut last_seq = None;
    let mut to_insert = Vec::with_capacity(tbl_info.non_pks.len());

    let update_clock_stmt_ref = tbl_info
        .get_update_clock_stmt(db)
        .map_err(|_e| "failed to get update_clock_stmt")?;
    let update_clock_stmt = update_clock_stmt_ref
        .as_ref()
        .ok_or("Failed to deref update_clock_stmt")?;

    for (i, col) in tbl_info.non_pks.iter().enumerate() {
        let seq = last_seq.take().unwrap_or_else(|| bump_seq(ext_data));
        update_clock_stmt
            .bind_int64(1, db_version)
            .and_then(|_| update_clock_stmt.bind_int(2, seq))
            .and_then(|_| update_clock_stmt.bind_text(3, ts, sqlite::Destructor::STATIC))
            .and_then(|_| update_clock_stmt.bind_int64(4, new_key))
            .and_then(|_| update_clock_stmt.bind_text(5, &col.name, sqlite::Destructor::STATIC))
            .map_err(|_| "failed binding to update_clock_stmt")?;

        step_trigger_stmt(update_clock_stmt)?;

        if changes64(db) == 0 {
            // keep last seq to reuse
            last_seq = Some(seq);
            to_insert.push(i);
        }
    }

    if !to_insert.is_empty() {
        if to_insert.len() == tbl_info.non_pks.len() {
            // fast path, insert all at once!
            let combo_insert_clock_stmt_ref = tbl_info
                .get_combo_insert_clock_stmt(db)
                .map_err(|_e| "failed to get combo_insert_clock_stmt")?;
            let combo_insert_clock_stmt = combo_insert_clock_stmt_ref
                .as_ref()
                .ok_or("Failed to deref combo_insert_clock_stmt")?;
            for (i, col) in tbl_info.non_pks.iter().enumerate() {
                let seq = last_seq.take().unwrap_or_else(|| bump_seq(ext_data));
                let offset = i as i32 * 5;

                combo_insert_clock_stmt
                    .bind_int64(offset + 1, new_key)
                    .and_then(|_| {
                        combo_insert_clock_stmt.bind_text(
                            offset + 2,
                            &col.name,
                            sqlite::Destructor::STATIC,
                        )
                    })
                    .and_then(|_| combo_insert_clock_stmt.bind_int64(offset + 3, db_version))
                    .and_then(|_| combo_insert_clock_stmt.bind_int(offset + 4, seq))
                    .and_then(|_| {
                        combo_insert_clock_stmt.bind_text(
                            offset + 5,
                            ts,
                            sqlite::Destructor::STATIC,
                        )
                    })
                    .map_err(|code| {
                        format!("failed binding to combo_insert_clock_stmt, code: {code}")
                    })?;
            }

            step_trigger_stmt(combo_insert_clock_stmt)?;
        } else {
            // mildly slower path...
            let insert_clock_stmt_ref = tbl_info
                .get_insert_clock_stmt(db)
                .map_err(|_e| "failed to get insert_clock_stmt")?;
            let insert_clock_stmt = insert_clock_stmt_ref
                .as_ref()
                .ok_or("Failed to deref insert_clock_stmt")?;

            for col_index in to_insert {
                let col = tbl_info
                    .non_pks
                    .get(col_index)
                    .ok_or("cannot find col for index...")?;
                let seq = last_seq.take().unwrap_or_else(|| bump_seq(ext_data));

                insert_clock_stmt
                    .bind_int64(1, new_key)
                    .and_then(|_| {
                        insert_clock_stmt.bind_text(2, &col.name, sqlite::Destructor::STATIC)
                    })
                    .and_then(|_| insert_clock_stmt.bind_int64(3, db_version))
                    .and_then(|_| insert_clock_stmt.bind_int(4, seq))
                    .and_then(|_| insert_clock_stmt.bind_text(5, ts, sqlite::Destructor::STATIC))
                    .map_err(|_| "failed binding to insert_clock_stmt")?;

                step_trigger_stmt(insert_clock_stmt)?;
            }
        }
    }

    Ok(ResultCode::OK)
}

#[allow(non_snake_case)]
fn mark_locally_updated(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    col_info: &ColumnInfo,
    db_version: sqlite::int64,
    seq: i32,
    ts: &str,
) -> Result<ResultCode, String> {
    let update_clock_stmt_ref = tbl_info
        .get_update_clock_stmt(db)
        .map_err(|_e| "failed to get update_clock_stmt")?;
    let update_clock_stmt = update_clock_stmt_ref
        .as_ref()
        .ok_or("Failed to deref update_clock_stmt")?;

    update_clock_stmt
        .bind_int64(1, db_version)
        .and_then(|_| update_clock_stmt.bind_int(2, seq))
        .and_then(|_| update_clock_stmt.bind_text(3, ts, sqlite::Destructor::STATIC))
        .and_then(|_| update_clock_stmt.bind_int64(4, new_key))
        .and_then(|_| update_clock_stmt.bind_text(5, &col_info.name, sqlite::Destructor::STATIC))
        .map_err(|_| "failed binding to update_clock_stmt")?;

    step_trigger_stmt(update_clock_stmt)?;

    if changes64(db) == 0 {
        let insert_clock_stmt_ref = tbl_info
            .get_insert_clock_stmt(db)
            .map_err(|_e| "failed to get insert_clock_stmt")?;
        let insert_clock_stmt = insert_clock_stmt_ref
            .as_ref()
            .ok_or("Failed to deref insert_clock_stmt")?;

        insert_clock_stmt
            .bind_int64(1, new_key)
            .and_then(|_| {
                insert_clock_stmt.bind_text(2, &col_info.name, sqlite::Destructor::STATIC)
            })
            .and_then(|_| insert_clock_stmt.bind_int64(3, db_version))
            .and_then(|_| insert_clock_stmt.bind_int(4, seq))
            .and_then(|_| insert_clock_stmt.bind_text(5, ts, sqlite::Destructor::STATIC))
            .map_err(|_| "failed binding to insert_clock_stmt")?;

        step_trigger_stmt(insert_clock_stmt)?;
    }

    Ok(ResultCode::OK)
}

fn mark_locally_deleted(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    db_version: sqlite::int64,
    seq: i32,
    ts: &str,
) -> Result<i64, String> {
    let mark_locally_deleted_stmt_ref = tbl_info
        .get_mark_locally_deleted_stmt(db)
        .map_err(|_e| "failed to get mark_locally_deleted_stmt")?;

    let mark_locally_deleted_stmt = mark_locally_deleted_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;

    mark_locally_deleted_stmt
        .bind_int64(1, key)
        .and_then(|_| mark_locally_deleted_stmt.bind_int64(2, db_version))
        .and_then(|_| mark_locally_deleted_stmt.bind_int(3, seq))
        .and_then(|_| mark_locally_deleted_stmt.bind_text(4, &ts, sqlite::Destructor::STATIC))
        .map_err(|_| "failed binding to mark locally deleted stmt")?;

    let stmt_res = mark_locally_deleted_stmt.step();

    let result = match stmt_res {
        Ok(ResultCode::ROW) => {
            let cl = mark_locally_deleted_stmt.column_int64(0);
            Ok(cl)
        }
        _ => Err("failed to step mark locally deleted stmt".to_string()),
    };

    reset_cached_stmt(mark_locally_deleted_stmt.stmt)
        .map_err(|_e| "failed to reset cached stmt")?;

    result
}
