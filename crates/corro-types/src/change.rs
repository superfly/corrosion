use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef},
    ToSql,
};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqliteValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> SqliteValueRef<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, SqliteValueRef::Null)
    }

    pub fn as_integer(&self) -> Option<&i64> {
        match self {
            SqliteValueRef::Integer(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<&f64> {
        match self {
            SqliteValueRef::Real(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            SqliteValueRef::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            SqliteValueRef::Blob(b) => Some(b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Readable, Writable, PartialEq)]
#[serde(untagged)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    pub fn as_str(&self) -> Option<&str> {
        if let Self::Text(ref s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_ref<'a>(&'a self) -> SqliteValueRef<'a> {
        match self {
            SqliteValue::Null => SqliteValueRef::Null,
            SqliteValue::Integer(i) => SqliteValueRef::Integer(*i),
            SqliteValue::Real(f) => SqliteValueRef::Real(*f),
            SqliteValue::Text(s) => SqliteValueRef::Text(s.as_str()),
            SqliteValue::Blob(v) => SqliteValueRef::Blob(v.as_slice()),
        }
    }
}

impl From<&str> for SqliteValue {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<String> for SqliteValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<u16> for SqliteValue {
    fn from(value: u16) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<i64> for SqliteValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl FromSql for SqliteValue {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(i) => SqliteValue::Integer(i),
            ValueRef::Real(f) => SqliteValue::Real(f),
            ValueRef::Text(t) => SqliteValue::Text(
                String::from_utf8(t.into()).map_err(|e| FromSqlError::Other(Box::new(e)))?,
            ),
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
            SqliteValue::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_bytes())),
            SqliteValue::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b.as_slice())),
        })
    }
}
