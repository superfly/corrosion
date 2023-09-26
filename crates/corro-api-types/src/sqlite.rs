use rusqlite::types::{FromSql, FromSqlError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, strum::FromRepr)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

impl FromSql for ChangeType {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Integer(i) => Ok(ChangeType::from_repr(
                i.try_into().map_err(|e| FromSqlError::Other(Box::new(e)))?,
            )
            .ok_or_else(|| FromSqlError::OutOfRange(i))?),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}
