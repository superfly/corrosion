use corro_api_types::{sqlite::ChangeType, ColumnName, RowId, SqliteValue};
use eyre::Result;
use rusqlite::{Connection, ToSql};
use std::path::Path;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

pub struct SubscriptionDb {
    conn: Connection,
    sub_id: Uuid,
    columns: Option<Vec<String>>,
    table_name: String,
}

impl SubscriptionDb {
    pub fn new(
        db_path: impl AsRef<Path>,
        sub_id: Uuid,
        sql: &str,
        table_name: &str,
    ) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode for better concurrency
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )?;

        conn.execute("DROP TABLE IF EXISTS meta;", [])?;
        conn.execute(
            "CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value);",
            [],
        )?;

        conn.execute("INSERT INTO meta (key, value) VALUES ('sql', ?)", [sql])?;

        Ok(Self {
            conn,
            sub_id,
            columns: None,
            table_name: table_name.to_string(),
        })
    }

    pub fn initialize_columns(&mut self, columns: Vec<ColumnName>) -> Result<()> {
        let column_names: Vec<String> = columns.iter().map(|c| c.as_str().to_string()).collect();
        self.columns = Some(column_names.clone());

        let column_defs: Vec<String> = column_names
            .iter()
            .map(|col| format!("{} TEXT", Self::sanitize_column_name(col)))
            .collect();

        self.conn
            .execute_batch(&format!("DROP TABLE IF EXISTS {}", self.table_name))?;

        let create_table_sql = format!(
            r#"
            CREATE TABLE {} (
                __corro_rowid INTEGER PRIMARY KEY,
                {}
            )
            "#,
            self.table_name,
            column_defs.join(", ")
        );

        self.conn.execute_batch(&create_table_sql)?;

        info!(
            sub_id = %self.sub_id,
            table = %self.table_name,
            "Created subscription table with {} columns",
            column_names.len()
        );

        Ok(())
    }

    pub fn insert_row(&self, row_id: RowId, values: &[SqliteValue]) -> Result<()> {
        if self.columns.is_none() {
            return Err(eyre::eyre!("Columns not initialized"));
        }

        let placeholders: Vec<String> = (0..self.columns.as_ref().unwrap().len())
            .map(|_| "?".to_string())
            .collect();
        let sql = format!(
            "INSERT INTO {} (__corro_rowid, {}) VALUES (?, {})",
            self.table_name,
            self.columns.as_ref().unwrap().join(", "),
            placeholders.join(", ")
        );

        let mut stmt = self.conn.prepare(&sql)?;

        let mut params: Vec<&dyn ToSql> = vec![&row_id.0];
        for value in values {
            params.push(value);
        }

        stmt.execute(rusqlite::params_from_iter(params.iter()))?;

        debug!(
            sub_id = %self.sub_id,
            row_id = row_id.0,
            "Inserted row"
        );

        Ok(())
    }

    pub fn update_row(&self, row_id: RowId, values: &[SqliteValue]) -> Result<()> {
        if self.columns.is_none() {
            return Err(eyre::eyre!("Columns not initialized"));
        }

        let set_clauses: Vec<String> = self
            .columns
            .as_ref()
            .unwrap()
            .iter()
            .map(|col| format!("{} = ?", Self::sanitize_column_name(col)))
            .collect();

        let sql = format!(
            "UPDATE {} SET {} WHERE __corro_rowid = ?",
            self.table_name,
            set_clauses.join(", ")
        );

        let mut stmt = self.conn.prepare(&sql)?;

        let mut params: Vec<&dyn ToSql> = vec![];
        for value in values {
            params.push(value);
        }
        params.push(&row_id.0);

        let rows_affected = stmt.execute(rusqlite::params_from_iter(params.iter()))?;

        if rows_affected == 0 {
            warn!(
                sub_id = %self.sub_id,
                row_id = row_id.0,
                "Update affected 0 rows - row may not exist"
            );
        } else {
            trace!(
                sub_id = %self.sub_id,
                row_id = row_id.0,
                "Updated row"
            );
        }

        Ok(())
    }

    pub fn delete_row(&self, row_id: RowId, _values: &[SqliteValue]) -> Result<()> {
        let sql = format!("DELETE FROM {} WHERE __corro_rowid = ?", self.table_name);

        let mut stmt = self.conn.prepare(&sql)?;
        let rows_affected = stmt.execute([row_id.0])?;

        if rows_affected == 0 {
            warn!(
                sub_id = %self.sub_id,
                row_id = row_id.0,
                "Delete affected 0 rows - row may not exist"
            );
        } else {
            debug!(
                sub_id = %self.sub_id,
                row_id = row_id.0,
                "Deleted row"
            );
        }

        Ok(())
    }

    pub fn handle_change(
        &self,
        change_type: ChangeType,
        row_id: RowId,
        values: &[SqliteValue],
    ) -> Result<()> {
        match change_type {
            ChangeType::Insert => self.insert_row(row_id, values),
            ChangeType::Update => self.update_row(row_id, values),
            ChangeType::Delete => self.delete_row(row_id, values),
        }
    }

    fn sanitize_column_name(name: &str) -> String {
        // SQLite allows most identifiers, but we'll quote them to be safe
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
