use compact_str::CompactString;
pub use corro_api_types::{Real, SqliteValue};

pub use corro_api_types::{QueryEvent, RqliteResponse, RqliteResult, Statement};

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
    pub fn add_row(&mut self, row: &rusqlite::Row) -> rusqlite::Result<()> {
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
