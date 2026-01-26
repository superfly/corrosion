use core::ffi::c_int;

use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use sqlite::{sqlite3, value, Context, ResultCode};
use sqlite_nostd as sqlite;

use crate::compare_values::crsql_compare_sqlite_values;
use crate::{c::crsql_ExtData, tableinfo::TableInfo};

use super::trigger_fn_preamble;

pub unsafe extern "C" fn x_crsql_after_update(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let result = trigger_fn_preamble(ctx, argc, argv, |table_info, values, ext_data| {
        let (pks_new, pks_old, non_pks_new, non_pks_old) =
            partition_values(values, 1, table_info.pks.len(), table_info.non_pks.len())?;

        after_update(
            ctx.db_handle(),
            ext_data,
            table_info,
            pks_new,
            pks_old,
            non_pks_new,
            non_pks_old,
        )
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

fn partition_values<T>(
    values: &[T],
    offset: usize,
    num_pks: usize,
    num_non_pks: usize,
) -> Result<(&[T], &[T], &[T], &[T]), String> {
    let expected_len = offset + num_pks * 2 + num_non_pks * 2;
    if values.len() != expected_len {
        return Err(format!(
            "expected {} values, got {}",
            expected_len,
            values.len()
        ));
    }
    Ok((
        &values[offset..num_pks + offset],
        &values[num_pks + offset..num_pks * 2 + offset],
        &values[num_pks * 2 + offset..num_pks * 2 + num_non_pks + offset],
        &values[num_pks * 2 + num_non_pks + offset..],
    ))
}

fn after_update(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &mut TableInfo,
    pks_new: &[*mut value],
    pks_old: &[*mut value],
    non_pks_new: &[*mut value],
    non_pks_old: &[*mut value],
) -> Result<ResultCode, String> {
    let ts = unsafe { (*ext_data).timestamp.to_string() };
    let next_db_version = crate::db_version::peek_next_db_version(db, ext_data)?;
    let new_key = tbl_info
        .get_or_create_key_via_raw_values(db, pks_new)
        .map_err(|_| "failed getting or creating lookaside key")?;

    let mut changed = false;
    // Changing a primary key column to a new value is the same thing as deleting the row
    // previously identified by the primary key.
    let cl_info = {
        if crate::compare_values::any_value_changed(pks_new, pks_old)? {
            let old_key = tbl_info
                .get_or_create_key_via_raw_values(db, pks_old)
                .map_err(|_| "failed getting or creating lookaside key")?;
            let next_seq = super::bump_seq(ext_data);
            changed = true;
            // Record the delete of the row identified by the old primary keys
            let cl =
                super::mark_locally_deleted(db, tbl_info, old_key, next_db_version, next_seq, &ts)?;
            let next_seq = super::bump_seq(ext_data);
            // todo: we don't need to this, if there's no existing row (cl is assumed to be 1).
            super::mark_new_pk_row_created(db, tbl_info, new_key, next_db_version, next_seq, &ts)?;
            for col in tbl_info.non_pks.iter() {
                let next_seq = super::bump_seq(ext_data);
                after_update__move_non_pk_col(
                    db,
                    tbl_info,
                    new_key,
                    old_key,
                    &col.name,
                    next_db_version,
                    &ts,
                    next_seq,
                )?;
            }
            Some((old_key, cl))
        } else {
            None
        }
    };
    // now for each non_pk_col we need to do an insert
    // where new value is not old value
    for ((new, old), col_info) in non_pks_new
        .iter()
        .zip(non_pks_old.iter())
        .zip(tbl_info.non_pks.iter())
    {
        if crsql_compare_sqlite_values(*new, *old) != 0 {
            changed = true;
            let next_seq = super::bump_seq(ext_data);
            // we had a difference in new and old values
            // we need to track crdt metadata
            super::mark_locally_updated(
                db,
                tbl_info,
                new_key,
                col_info,
                next_db_version,
                next_seq,
                &ts,
            )?;
        }
    }

    // actually set the db_version
    if changed {
        // TODO: assert this is same as next_db_version
        crate::db_version::next_db_version(db, ext_data)?;
    }

    if let Some((old_key, cl)) = cl_info {
        tbl_info.set_cl(old_key, cl);
    }

    Ok(ResultCode::OK)
}

#[allow(non_snake_case)]
fn after_update__move_non_pk_col(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    old_key: sqlite::int64,
    col_name: &str,
    db_version: sqlite::int64,
    ts: &str,
    seq: i32,
) -> Result<ResultCode, String> {
    let move_non_pk_col_stmt_ref = tbl_info
        .get_move_non_pk_col_stmt(db)
        .or_else(|_e| Err("failed to get move_non_pk_col_stmt"))?;
    let move_non_pk_col_stmt = move_non_pk_col_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;
    move_non_pk_col_stmt
        .bind_int64(1, new_key)
        .and_then(|_| move_non_pk_col_stmt.bind_int64(2, db_version))
        .and_then(|_| move_non_pk_col_stmt.bind_int(3, seq))
        .and_then(|_| move_non_pk_col_stmt.bind_text(4, ts, sqlite::Destructor::STATIC))
        .and_then(|_| move_non_pk_col_stmt.bind_int64(5, old_key))
        .and_then(|_| move_non_pk_col_stmt.bind_text(6, col_name, sqlite::Destructor::STATIC))
        .or_else(|_| Err("failed binding to move_non_pk_col_stmt"))?;
    super::step_trigger_stmt(move_non_pk_col_stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_values() {
        let values1 = vec!["tbl", "pk.new", "pk.old", "c.new", "c.old"];
        let values2 = vec!["tbl", "pk.new", "pk.old"];
        let values3 = vec!["tbl", "pk1.new", "pk2.new", "pk1.old", "pk2.old"];
        let values4 = vec![
            "tbl", "pk1.new", "pk2.new", "pk1.old", "pk2.old", "c.new", "d.new", "c.old", "d.old",
        ];

        assert_eq!(
            partition_values(&values1, 1, 1, 1),
            Ok((
                &["pk.new"] as &[&str],
                &["pk.old"] as &[&str],
                &["c.new"] as &[&str],
                &["c.old"] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values2, 1, 1, 0),
            Ok((
                &["pk.new"] as &[&str],
                &["pk.old"] as &[&str],
                &[] as &[&str],
                &[] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values3, 1, 2, 0),
            Ok((
                &["pk1.new", "pk2.new"] as &[&str],
                &["pk1.old", "pk2.old"] as &[&str],
                &[] as &[&str],
                &[] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values4, 1, 2, 2),
            Ok((
                &["pk1.new", "pk2.new"] as &[&str],
                &["pk1.old", "pk2.old"] as &[&str],
                &["c.new", "d.new"] as &[&str],
                &["c.old", "d.old"] as &[&str]
            ))
        );
    }
}
