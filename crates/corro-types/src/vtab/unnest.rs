//! Unnest Virtual Table - PostgreSQL-style unnest for multiple arrays
//!
//! This module provides a virtual table that mimics PostgreSQL's `unnest()` function,
//! allowing multiple arrays to be expanded into rows with multiple columns.
//!
//! # Example
//!
//! ```rust,no_run
//! # use rusqlite::{types::Value, Connection, Result, params};
//! # use std::rc::Rc;
//! fn example(db: &Connection) -> Result<()> {
//!     // Load the unnest module
//!     db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;
//!     
//!     let arr1 = Rc::new(vec![Value::from(1i64), Value::from(2i64), Value::from(3i64)]);
//!     let arr2 = Rc::new(vec![Value::from("a"), Value::from("b"), Value::from("c")]);
//!     
//!     // Query with multiple arrays - like PostgreSQL's unnest(array1, array2)
//!     let mut stmt = db.prepare("SELECT * FROM unnest(?1, ?2);")?;
//!     let rows = stmt.query_map([arr1, arr2], |row| {
//!         Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
//!     })?;
//!     
//!     for row in rows {
//!         let (num, text) = row?;
//!         println!("{}, {}", num, text);
//!     }
//!     Ok(())
//! }
//! ```

use std::ffi::c_int;
use std::marker::PhantomData;
use std::os::raw::c_char;

use rusqlite::ffi;
use rusqlite::types::Value;
use rusqlite::vtab::{
    array::Array, Context, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor, Values, Filters
};
use rusqlite::Result;

// The array type identifier used by rusqlite's array module
// This must match rusqlite's internal ARRAY_TYPE constant
const ARRAY_TYPE: *const c_char = c"rarray".as_ptr();
// Maximum number of arrays supported
const MAX_ARRAYS: usize = 32;
lazy_static::lazy_static! {
    static ref UNNEST_SCHEMA: String =
        format!("CREATE TABLE x({}, {})",
            (0..MAX_ARRAYS).map(|i| format!("value{i}")).collect::<Vec<_>>().join(", "),
            (0..MAX_ARRAYS).map(|i| format!("ptr{i} hidden")).collect::<Vec<_>>().join(", ")
        );
}

/// Extension trait to access the private `get_array` method on `Values`
/// This is a workaround since rusqlite doesn't expose this method publicly
/// TODO: Ask them to make get_array public
trait ValuesExt {
    fn get_array_hack(&self, idx: usize) -> Option<Array>;
}

impl ValuesExt for Values<'_> {
    fn get_array_hack(&self, idx: usize) -> Option<Array> {
        // SAFETY: This is safe because:
        // 1. Values is a transparent wrapper around &[*mut sqlite3_value]
        // 2. We're accessing the same field that the private get_array method accesses
        // 3. The implementation is identical to rusqlite's get_array
        unsafe {
            // Assert at compile time that layouts match
            const _: () = assert!(
                std::mem::size_of::<Values>() == std::mem::size_of::<&[*mut ffi::sqlite3_value]>()
            );
            const _: () = assert!(
                std::mem::align_of::<Values>()
                    == std::mem::align_of::<&[*mut ffi::sqlite3_value]>()
            );

            // Values has a single field: args: &[*mut sqlite3_value]
            let args_ptr = self as *const Values as *const &[*mut ffi::sqlite3_value];
            let args = *args_ptr;

            if idx >= args.len() {
                return None;
            }

            let arg = args[idx];
            let ptr = ffi::sqlite3_value_pointer(arg, ARRAY_TYPE);
            if ptr.is_null() {
                None
            } else {
                let ptr = ptr as *const Vec<Value>;
                Array::increment_strong_count(ptr); // don't consume it
                Some(Array::from_raw(ptr))
            }
        }
    }
}

/// An instance of the Unnest virtual table
#[repr(C)]
pub struct UnnestTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
}

const FOUND_ARRAY_INDEX: i32 = 1;
const NO_ARRAYS_INDEX: i32 = 0;
unsafe impl<'vtab> VTab<'vtab> for UnnestTab {
    type Aux = ();
    type Cursor = UnnestTabCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> Result<(String, Self)> {
        let vtab = Self {
            base: ffi::sqlite3_vtab::default(),
        };
        Ok((UNNEST_SCHEMA.clone(), vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        let mut array_arguments_count: i32 = 0;

        for (constraint, mut constraint_usage) in info.constraints_and_usages() {
            if !constraint.is_usable() {
                continue;
            }
            if constraint.operator() != IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }

            // Check if this is a pointer column
            let col = constraint.column();
            if col >= MAX_ARRAYS as i32 && col < MAX_ARRAYS as i32 * 2 {
                array_arguments_count += 1;
                constraint_usage.set_argv_index(array_arguments_count);
                constraint_usage.set_omit(true);
            }
        }

        if array_arguments_count > 0 {
            info.set_estimated_cost(1_f64);
            info.set_estimated_rows(100);
            info.set_idx_num(FOUND_ARRAY_INDEX); // SUCCESS
        } else {
            // No arrays provided - this is an error case
            info.set_estimated_cost(2_147_483_647_f64);
            info.set_estimated_rows(0);
            info.set_idx_num(NO_ARRAYS_INDEX); // FAILURE
        }
        Ok(())
    }

    fn open(&mut self) -> Result<UnnestTabCursor<'_>> {
        Ok(UnnestTabCursor::new())
    }
}

/// A cursor for the Unnest virtual table
#[repr(C)]
pub struct UnnestTabCursor<'vtab> {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    /// Pointers to the arrays
    arrays: Vec<Option<Array>>,
    /// Maximum length among all arrays
    max_len: i64,
    phantom: PhantomData<&'vtab UnnestTab>,
}

impl UnnestTabCursor<'_> {
    fn new<'vtab>() -> UnnestTabCursor<'vtab> {
        UnnestTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            row_id: 0,
            arrays: Vec::new(),
            max_len: 0,
            phantom: PhantomData,
        }
    }

    fn compute_max_len(&self) -> i64 {
        self.arrays
            .iter()
            .filter_map(|opt_arr| opt_arr.as_ref().map(|arr| arr.len() as i64))
            .max()
            .unwrap_or(0)
    }
}

unsafe impl VTabCursor for UnnestTabCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        self.arrays.clear();
        self.row_id = 1;

        if idx_num == FOUND_ARRAY_INDEX {
            // idx_num tells us how many arrays were provided
            let num_arrays = args.len();

            for i in 0..num_arrays {
                // Extract array from arguments using our hack
                let array_opt = args.get_array_hack(i);
                self.arrays.push(array_opt);
            }

            self.max_len = self.compute_max_len();
        } else if idx_num == NO_ARRAYS_INDEX {
            self.max_len = 0;
        } else {
            unreachable!("Invalid idx_num: {idx_num}");
        }
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_id > self.max_len
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        // Columns 0..MAX_ARRAYS are value columns
        // Columns MAX_ARRAYS..2*MAX_ARRAYS are pointer columns (hidden)
        if i >= MAX_ARRAYS as i32 {
            // Pointer column - return NULL (these are hidden and only used for constraints)
            Ok(())
        } else {
            // Value column
            let array_idx = i as usize;
            if array_idx < self.arrays.len() {
                if let Some(ref array) = self.arrays[array_idx] {
                    let value_idx = (self.row_id - 1) as usize;
                    if value_idx < array.len() {
                        let value = &array[value_idx];
                        ctx.set_result(value)
                    } else {
                        // This array is shorter than max_len, return NULL
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            } else {
                // No array for this column
                Ok(())
            }
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_id)
    }
}

#[cfg(test)]
mod test {
    use rusqlite::types::Value;
    use rusqlite::vtab::eponymous_only_module;
    use rusqlite::Result;

    use crate::vtab::unnest::UnnestTab;
    use rusqlite::Connection;
    use std::rc::Rc;

    #[test]
    fn test_unnest_single_array() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

        let v = vec![1i64, 2, 3, 4];
        let values: Vec<Value> = v.into_iter().map(Value::from).collect();
        let ptr = Rc::new(values);

        let mut stmt = db.prepare("SELECT value0 FROM unnest(?1);")?;
        let rows = stmt.query_map([&ptr], |row| row.get::<_, i64>(0))?;

        let results: Vec<i64> = rows.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(results, vec![1, 2, 3, 4]);

        Ok(())
    }

    #[test]
    fn test_unnest_multiple_arrays_same_length() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

        let arr1 = Rc::new(vec![
            Value::from(1i64),
            Value::from(2i64),
            Value::from(3i64),
        ]);
        let arr2 = Rc::new(vec![
            Value::from("a".to_string()),
            Value::from("b".to_string()),
            Value::from("c".to_string()),
        ]);

        let mut stmt = db.prepare("SELECT value0, value1 FROM unnest(?1, ?2);")?;
        let rows = stmt.query_map([arr1, arr2], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let results: Vec<(i64, String)> = rows.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results,
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_unnest_multiple_arrays_different_lengths() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

        let arr1 = Rc::new(vec![Value::from(1i64), Value::from(2i64)]);
        let arr2 = Rc::new(vec![
            Value::from("a".to_string()),
            Value::from("b".to_string()),
            Value::from("c".to_string()),
            Value::from("d".to_string()),
        ]);

        let mut stmt = db.prepare("SELECT value0, value1 FROM unnest(?1, ?2);")?;
        let rows = stmt.query_map([arr1, arr2], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })?;

        let results: Vec<(Option<i64>, Option<String>)> = rows.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results,
            vec![
                (Some(1), Some("a".to_string())),
                (Some(2), Some("b".to_string())),
                (None, Some("c".to_string())),
                (None, Some("d".to_string())),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_unnest_three_arrays() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

        let arr1 = Rc::new(vec![Value::from(1i64), Value::from(2i64)]);
        let arr2 = Rc::new(vec![
            Value::from("x".to_string()),
            Value::from("y".to_string()),
            Value::from("z".to_string()),
        ]);
        let arr3 = Rc::new(vec![Value::from(10.5), Value::from(20.5)]);

        let mut stmt = db.prepare("SELECT value0, value1, value2 FROM unnest(?1, ?2, ?3);")?;
        let rows = stmt.query_map([arr1, arr2, arr3], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        })?;

        let results: Vec<(Option<i64>, Option<String>, Option<f64>)> =
            rows.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results,
            vec![
                (Some(1), Some("x".to_string()), Some(10.5)),
                (Some(2), Some("y".to_string()), Some(20.5)),
                (None, Some("z".to_string()), None),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_unnest_empty_array() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

        let arr1: Rc<Vec<Value>> = Rc::new(vec![]);

        let mut stmt = db.prepare("SELECT value0 FROM unnest(?1);")?;
        let rows = stmt.query_map([arr1], |row| row.get::<_, i64>(0))?;

        let results: Vec<i64> = rows.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(results, Vec::<i64>::new());

        Ok(())
    }
}
