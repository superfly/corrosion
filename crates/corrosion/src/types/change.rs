use rusqlite::{
    types::{FromSql, ToSqlOutput, Value, ValueRef},
    ToSql,
};
use speedy::{Readable, Writable};

#[derive(Debug, Clone, Readable, Writable)]
pub struct Change {
    pub table: String,
    pub pk: String,
    pub cid: String,
    pub val: SqliteValue,
    pub col_version: i64,
    pub db_version: i64,
    pub site_id: [u8; 16],
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl FromSql for SqliteValue {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(i) => SqliteValue::Integer(i),
            ValueRef::Real(f) => SqliteValue::Real(f),
            ValueRef::Text(t) => SqliteValue::Text(t.into()),
            ValueRef::Blob(b) => SqliteValue::Blob(b.into()),
        })
    }
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            SqliteValue::Null => ToSqlOutput::Owned(Value::Null),
            SqliteValue::Integer(i) => ToSqlOutput::Owned(Value::Integer(*i)),
            SqliteValue::Real(f) => ToSqlOutput::Owned(Value::Real(*f)),
            SqliteValue::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_slice())),
            SqliteValue::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b.as_slice())),
        })
    }
}
