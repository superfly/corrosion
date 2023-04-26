use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::change::SqliteValue;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Statement {
    Simple(String),
    WithNamedParams(String, HashMap<String, SqliteValue>),
    WithParams(Vec<SqliteValue>),
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
