use std::collections::HashMap;

use rusqlite::{types::ToSqlOutput, ToSql};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Statement {
    Simple(String),
    WithNamedParams(String, HashMap<String, SqliteParam>),
    WithParams(Vec<SqliteParam>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RqliteResponse {
    pub results: Vec<RqliteResult>,
    pub time: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RqliteResult {
    Execute {
        rows_affected: usize,
        time: Option<f64>,
    },
    Query {
        types: HashMap<String, String>,
        rows: Vec<HashMap<String, Value>>,
        time: Option<f64>,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqliteParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteParam {
    pub fn as_str(&self) -> Option<&str> {
        if let Self::Text(ref s) = self {
            Some(s)
        } else {
            None
        }
    }
}

impl From<&str> for SqliteParam {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl From<Vec<u8>> for SqliteParam {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<String> for SqliteParam {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<u16> for SqliteParam {
    fn from(value: u16) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<i64> for SqliteParam {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl ToSql for SqliteParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(match self {
            SqliteParam::Null => ToSqlOutput::Owned(rusqlite::types::Value::Null),
            SqliteParam::Integer(i) => ToSqlOutput::Owned(rusqlite::types::Value::Integer(*i)),
            SqliteParam::Real(f) => ToSqlOutput::Owned(rusqlite::types::Value::Real(*f)),
            SqliteParam::Text(s) => {
                ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(s.as_bytes()))
            }
            SqliteParam::Blob(v) => {
                ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Blob(v.as_slice()))
            }
        })
    }
}
