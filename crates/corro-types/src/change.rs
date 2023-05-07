use std::ops::Deref;

use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput, Value, ValueRef},
    ToSql,
};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};

use crate::filters::parse_sqlite_quoted_str;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    pub fn to_owned(&self) -> SqliteValue {
        match self {
            SqliteValueRef::Null => SqliteValue::Null,
            SqliteValueRef::Integer(v) => SqliteValue::Integer(*v),
            SqliteValueRef::Real(v) => SqliteValue::Real(*v),
            SqliteValueRef::Text(v) => SqliteValue::Text((*v).to_owned()),
            SqliteValueRef::Blob(v) => SqliteValue::Blob(v.to_vec()),
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

    pub fn is_null(&self) -> bool {
        matches!(self, SqliteValue::Null)
    }

    pub fn as_integer(&self) -> Option<&i64> {
        match self {
            SqliteValue::Integer(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<&f64> {
        match self {
            SqliteValue::Real(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            SqliteValue::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            SqliteValue::Blob(b) => Some(b),
            _ => None,
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

#[derive(Debug, Clone, Readable, Writable)]
pub struct SqliteQuotedValue {
    pub value: SqliteValue,
    src: String,
}

impl Deref for SqliteQuotedValue {
    type Target = SqliteValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl FromSql for SqliteQuotedValue {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value {
            ValueRef::Text(t) => {
                let src = String::from_utf8_lossy(t).into_owned();
                Self {
                    value: parse_sqlite_quoted_str(&src).ok_or(FromSqlError::InvalidType)?,
                    src,
                }
            }
            _ => return Err(FromSqlError::InvalidType),
        })
    }
}

impl ToSql for SqliteQuotedValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.src.as_bytes())))
    }
}
