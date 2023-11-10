use rusqlite::{
    types::{FromSql, FromSqlError},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};

#[derive(
    Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Readable, Writable, strum::FromRepr,
)]
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

impl ToSql for ChangeType {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Owned(
            rusqlite::types::Value::Integer((*self as u8) as i64),
        ))
    }
}
