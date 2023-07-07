use std::collections::HashMap;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

use crate::{change::SqliteValue, schema::SqliteType};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Statement {
    Simple(String),
    WithNamedParams(String, HashMap<String, SqliteValue>),
    WithParams(String, Vec<SqliteValue>),
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
        columns: Vec<CompactString>,
        types: Vec<Option<CompactString>>,
        values: Vec<Vec<SqliteValue>>,
        time: Option<f64>,
    },
    QueryAssociative {
        types: HashMap<CompactString, Option<CompactString>>,
        rows: Vec<HashMap<CompactString, SqliteValue>>,
        time: Option<f64>,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryResult {
    Columns(Vec<CompactString>),
    Row(Vec<SqliteValue>),
    Error(CompactString),
}

#[derive(Default)]
pub struct QueryResultBuilder {
    pub columns: Vec<CompactString>,
    pub types: Vec<Option<CompactString>>,
    pub values: Vec<Vec<SqliteValue>>,
    pub time: Option<f64>,
}

impl QueryResultBuilder {
    pub fn new(
        columns: Vec<CompactString>,
        types: Vec<Option<CompactString>>,
        time: Option<f64>,
    ) -> Self {
        Self {
            columns,
            types,
            time,
            ..Default::default()
        }
    }
    pub fn add_row<'stmt>(&mut self, row: &rusqlite::Row<'stmt>) -> rusqlite::Result<()> {
        self.values.push(
            (0..self.columns.len())
                .map(|idx| row.get(idx))
                .collect::<rusqlite::Result<_>>()?,
        );
        Ok(())
    }

    pub fn build_associative(self) -> RqliteResult {
        RqliteResult::QueryAssociative {
            types: self
                .columns
                .iter()
                .enumerate()
                .map(|(i, n)| (n.clone(), self.types.get(i).cloned().flatten()))
                .collect(),
            rows: self
                .values
                .into_iter()
                .map(|values| {
                    self.columns
                        .iter()
                        .enumerate()
                        .filter_map(|(i, n)| values.get(i).map(|v| (n.clone(), v.clone())))
                        .collect()
                })
                .collect(),
            time: self.time,
        }
    }

    pub fn build(self) -> RqliteResult {
        RqliteResult::Query {
            columns: self.columns,
            types: self.types,
            values: self.values,
            time: self.time,
        }
    }
}
