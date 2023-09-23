use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use bytes::Buf;
use compact_str::{CompactString, ToCompactString};
use corro_api_types::{Change, ColumnType, SqliteValue, SqliteValueRef};
use enquote::unquote;
use fallible_iterator::FallibleIterator;
use indexmap::IndexMap;
use itertools::Itertools;
use parking_lot::Mutex;
use rusqlite::{params_from_iter, Connection};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, JoinConstraint, Name, OneSelect, Operator, QualifiedName, ResultColumn,
        Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
};
use tokio::{sync::mpsc, task::block_in_place};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::{
    api::QueryEvent,
    schema::{NormalizedSchema, NormalizedTable},
};

pub use corro_api_types::sqlite::ChangeType;

#[derive(Debug, thiserror::Error)]
pub enum NormalizeStatementError {
    #[error(transparent)]
    Parse(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("unexpected statement: {0}")]
    UnexpectedStatement(Cmd),
    #[error("only 1 statement is supported")]
    Multiple,
    #[error("at least 1 statement is required")]
    NoStatement,
}

pub fn normalize_sql(sql: &str) -> Result<String, NormalizeStatementError> {
    let mut parser = Parser::new(sql.as_bytes());

    let stmt = match parser.next()? {
        Some(Cmd::Stmt(stmt)) => stmt,
        Some(cmd) => {
            return Err(NormalizeStatementError::UnexpectedStatement(cmd));
        }
        None => {
            return Err(NormalizeStatementError::NoStatement);
        }
    };

    if parser.next()?.is_some() {
        return Err(NormalizeStatementError::Multiple);
    }

    Ok(Cmd::Stmt(stmt).to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum UnpackError {
    #[error("abort")]
    Abort,
    #[error("misuse")]
    Misuse,
}

pub fn unpack_columns(mut buf: &[u8]) -> Result<Vec<SqliteValueRef>, UnpackError> {
    let mut ret = vec![];
    let num_columns = buf.get_u8();

    for _i in 0..num_columns {
        if !buf.has_remaining() {
            return Err(UnpackError::Abort);
        }
        let column_type_and_maybe_intlen = buf.get_u8();
        let column_type = ColumnType::from_u8(column_type_and_maybe_intlen & 0x07);
        let intlen = (column_type_and_maybe_intlen >> 3) as usize;

        match column_type {
            Some(ColumnType::Blob) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef::Blob(&buf[0..len]));
                buf.advance(len);
            }
            Some(ColumnType::Float) => {
                if buf.remaining() < 8 {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef::Real(buf.get_f64()));
            }
            Some(ColumnType::Integer) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef::Integer(buf.get_int(intlen)));
            }
            Some(ColumnType::Null) => {
                ret.push(SqliteValueRef::Null);
            }
            Some(ColumnType::Text) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef::Text(unsafe {
                    std::str::from_utf8_unchecked(&buf[0..len])
                }));
                buf.advance(len);
            }
            None => return Err(UnpackError::Misuse),
        }
    }

    Ok(ret)
}

pub enum MatcherCmd {
    ProcessChange(IndexMap<CompactString, Vec<Vec<SqliteValue>>>),
}

#[derive(Clone)]
pub struct MatcherHandle(Arc<InnerMatcherHandle>);

struct InnerMatcherHandle {
    cmd_tx: mpsc::Sender<MatcherCmd>,
    parsed: ParsedSelect,
    qualified_table_name: String,
    col_names: Vec<CompactString>,
}

impl MatcherHandle {
    pub fn process_change(&self, changes: &[Change]) -> Result<(), MatcherError> {
        // println!("{:?}", self.0.parsed);

        let mut candidates: IndexMap<CompactString, Vec<Vec<SqliteValue>>> = IndexMap::new();

        let grouped = changes
            .iter()
            .filter(|change| {
                self.0
                    .parsed
                    .table_columns
                    .contains_key(change.table.as_str())
            })
            .group_by(|change| (change.table.as_str(), change.pk.as_slice()));

        for ((table, pk), _) in grouped.into_iter() {
            let pks = unpack_columns(pk)?
                .into_iter()
                .map(|v| v.to_owned())
                .collect();
            if let Some(v) = candidates.get_mut(table) {
                v.push(pks);
            } else {
                candidates.insert(table.to_compact_string(), vec![pks]);
            }
        }

        self.0
            .cmd_tx
            .try_send(MatcherCmd::ProcessChange(candidates))
            .map_err(|_| MatcherError::ChangeQueueClosedOrFull)?;

        Ok(())
    }

    pub fn table_name(&self) -> &str {
        &self.0.qualified_table_name
    }

    pub fn parsed_columns(&self) -> &[ResultColumn] {
        &self.0.parsed.columns
    }

    pub fn col_names(&self) -> &[CompactString] {
        &self.0.col_names
    }
}

#[derive(Debug)]
pub struct Matcher {
    pub id: Uuid,
    pub query: Stmt,
    pub statements: HashMap<String, MatcherStmt>,
    pub pks: IndexMap<String, Vec<String>>,
    pub parsed: ParsedSelect,
    pub query_table: String,
    pub qualified_table_name: String,
    pub evt_tx: mpsc::Sender<QueryEvent>,
    pub col_names: Vec<CompactString>,
    pub last_rowid: Mutex<i64>,
}

#[derive(Debug, Clone)]
pub struct MatcherStmt {
    new_query: String,
    temp_query: String,
}

impl Matcher {
    pub fn restore(
        id: Uuid,
        schema: &NormalizedSchema,
        mut conn: Connection,
        sql: &str,
    ) -> Result<MatcherHandle, MatcherError> {
        todo!()
    }

    pub fn create(
        id: Uuid,
        schema: &NormalizedSchema,
        mut conn: Connection,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<MatcherHandle, MatcherError> {
        let col_names: Vec<CompactString> = {
            conn.prepare(sql)?
                .column_names()
                .into_iter()
                .map(|s| s.to_compact_string())
                .collect()
        };

        let mut parser = Parser::new(sql.as_bytes());

        let (mut stmt, parsed) = match parser.next()?.ok_or(MatcherError::StatementRequired)? {
            Cmd::Stmt(stmt) => {
                let parsed = match stmt {
                    Stmt::Select(ref select) => extract_select_columns(select, schema)?,
                    _ => return Err(MatcherError::UnsupportedStatement),
                };

                (stmt, parsed)
            }
            _ => return Err(MatcherError::StatementRequired),
        };

        // println!("{stmt:#?}");
        // println!("parsed: {parsed:#?}");

        if parsed.table_columns.is_empty() {
            return Err(MatcherError::TableRequired);
        }

        let mut statements = HashMap::new();

        let mut pks = IndexMap::default();

        match &mut stmt {
            Stmt::Select(select) => match &mut select.body.select {
                OneSelect::Select { columns, .. } => {
                    let mut new_cols = parsed
                        .table_columns
                        .iter()
                        .filter_map(|(tbl_name, _cols)| {
                            schema.tables.get(tbl_name).map(|table| {
                                let tbl_name = parsed
                                    .aliases
                                    .iter()
                                    .find_map(|(alias, actual)| {
                                        (actual == tbl_name).then_some(alias)
                                    })
                                    .unwrap_or(tbl_name);
                                table
                                    .pk
                                    .iter()
                                    .map(|pk| {
                                        let alias = format!("__corro_pk_{tbl_name}_{pk}");
                                        let entry: &mut Vec<String> =
                                            pks.entry(table.name.clone()).or_default();
                                        entry.push(alias.clone());

                                        ResultColumn::Expr(
                                            Expr::Qualified(
                                                Name(tbl_name.clone()),
                                                Name(pk.clone()),
                                            ),
                                            Some(As::As(Name(alias))),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                        })
                        .flatten()
                        .collect::<Vec<_>>();

                    new_cols.append(&mut parsed.columns.clone());
                    *columns = new_cols;
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }

        let query_table = format!("query_{}", id.as_simple());

        for (tbl_name, _cols) in parsed.table_columns.iter() {
            let expr = table_to_expr(
                &parsed.aliases,
                schema
                    .tables
                    .get(tbl_name)
                    .expect("this should not happen, missing table in schema"),
                tbl_name,
                id,
            )?;

            let mut stmt = stmt.clone();

            if let Stmt::Select(select) = &mut stmt {
                if let OneSelect::Select { where_clause, .. } = &mut select.body.select {
                    *where_clause = if let Some(prev) = where_clause.take() {
                        Some(Expr::Binary(Box::new(expr), Operator::And, Box::new(prev)))
                    } else {
                        Some(expr)
                    };
                }
            }

            let mut new_query = Cmd::Stmt(stmt).to_string();
            new_query.pop();

            let mut tmp_cols = pks.values().flatten().cloned().collect::<Vec<String>>();
            for i in 0..(parsed.columns.len()) {
                tmp_cols.push(format!("col_{i}"));
            }

            let pk_cols = pks
                .get(tbl_name)
                .cloned()
                .ok_or(MatcherError::MissingPrimaryKeys)?
                .to_vec()
                .join(",");

            statements.insert(
                tbl_name.clone(),
                MatcherStmt {
                    new_query,
                    temp_query: format!(
                        "SELECT {} FROM {} WHERE ({}) IN subscription_{}_{}",
                        tmp_cols.join(","),
                        query_table,
                        pk_cols,
                        id.as_simple(),
                        tbl_name,
                    ),
                },
            );
        }

        let qualified_table_name = format!("subscriptions.{query_table}");

        let (cmd_tx, mut cmd_rx) = mpsc::channel(512);

        let handle = MatcherHandle(Arc::new(InnerMatcherHandle {
            cmd_tx,
            parsed: parsed.clone(),
            qualified_table_name: qualified_table_name.clone(),
            col_names: col_names.clone(),
        }));

        let matcher = Self {
            id,
            query: stmt,
            statements,
            pks,
            parsed,
            qualified_table_name,
            query_table,
            evt_tx: evt_tx.clone(),
            col_names: col_names.clone(),
            last_rowid: Mutex::new(0),
        };

        let mut tmp_cols = matcher
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        for i in 0..(matcher.parsed.columns.len()) {
            tmp_cols.push(format!("col_{i}"));
        }

        block_in_place(|| {
            let create_temp_table = format!(
                "CREATE TABLE IF NOT EXISTS {} (__corro_rowid INTEGER PRIMARY KEY AUTOINCREMENT, {});
                 CREATE UNIQUE INDEX IF NOT EXISTS subscriptions.index_{}_pk ON {} ({});",
                matcher.qualified_table_name,
                tmp_cols.join(","),
                matcher.id.as_simple(),
                matcher.query_table,
                matcher
                    .pks
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(","),
            );

            conn.execute_batch(&create_temp_table)
        })?;

        tokio::spawn({
            async move {
                async {
                    if let Err(e) = evt_tx.send(QueryEvent::Columns(col_names)).await {
                        error!("could not send back columns, probably means no receivers! {e}");
                        return;
                    }

                    let mut query_cols = vec![];
                    for i in 0..(matcher.parsed.columns.len()) {
                        query_cols.push(format!("col_{i}"));
                    }

                    let res = block_in_place(|| {
                        let tx = conn.transaction()?;

                        let mut stmt_str = Cmd::Stmt(matcher.query.clone()).to_string();
                        stmt_str.pop();

                        let insert_into = format!(
                            "INSERT INTO {} ({}) {} RETURNING __corro_rowid,{}",
                            matcher.qualified_table_name,
                            tmp_cols.join(","),
                            stmt_str,
                            query_cols.join(","),
                        );

                        let mut last_rowid = 0;

                        let elapsed = {
                            let mut prepped = tx.prepare(&insert_into)?;

                            let start = Instant::now();
                            let mut rows = prepped.query(())?;
                            let elapsed = start.elapsed();

                            loop {
                                match rows.next() {
                                    Ok(Some(row)) => {
                                        let rowid: i64 = row.get(0)?;
                                        let cells = (1..=query_cols.len())
                                            .map(|i| row.get::<_, SqliteValue>(i))
                                            .collect::<rusqlite::Result<Vec<_>>>()?;

                                        if let Err(e) =
                                            evt_tx.blocking_send(QueryEvent::Row(rowid, cells))
                                        {
                                            error!("could not send back row: {e}");
                                            return Err(MatcherError::EventReceiverClosed);
                                        }

                                        last_rowid = cmp::max(rowid, last_rowid);
                                    }
                                    Ok(None) => {
                                        // done!
                                        break;
                                    }
                                    Err(e) => {
                                        return Err(e.into());
                                    }
                                }
                            }
                            elapsed
                        };

                        tx.commit()?;

                        *matcher.last_rowid.lock() = last_rowid;

                        Ok::<_, MatcherError>(elapsed)
                    });

                    match res {
                        Ok(elapsed) => {
                            if let Err(e) = evt_tx
                                .send(QueryEvent::EndOfQuery {
                                    time: elapsed.as_secs_f64(),
                                })
                                .await
                            {
                                error!("could not return end of query event: {e}");
                                return;
                            }
                        }
                        Err(e) => {
                            _ = evt_tx.send(QueryEvent::Error(e.to_compact_string())).await;
                            return;
                        }
                    }

                    if let Err(e) = res {
                        _ = evt_tx.send(QueryEvent::Error(e.to_compact_string())).await;
                        return;
                    }

                    while let Some(req) = cmd_rx.recv().await {
                        match req {
                            MatcherCmd::ProcessChange(candidates) => {
                                if let Err(e) =
                                    block_in_place(|| matcher.handle_change(&mut conn, candidates))
                                {
                                    if matches!(e, MatcherError::EventReceiverClosed) {
                                        break;
                                    }
                                    error!("could not handle change: {e}");
                                }
                            }
                        }
                    }
                }
                .await;

                debug!(id = %id, "matcher loop is done");

                // TODO: make this run if anything fails after the table was created!
                if let Err(e) =
                    conn.execute_batch(&format!("DROP TABLE {}", matcher.qualified_table_name))
                {
                    warn!(
                        "could not clean up temporary table {} => {e}",
                        matcher.qualified_table_name
                    );
                } else {
                    debug!(
                        "cleaned up subscription table {}",
                        matcher.qualified_table_name,
                    );
                }
            }
        });

        Ok(handle)
    }

    fn handle_change(
        &self,
        conn: &mut Connection,
        candidates: IndexMap<CompactString, Vec<Vec<SqliteValue>>>,
    ) -> Result<(), MatcherError> {
        let tx = conn.transaction()?;

        let tables = candidates.keys().cloned().collect::<Vec<_>>();

        for (table, pks) in candidates {
            // create a temporary table to mix and match the data
            tx.prepare_cached(
                // TODO: cache the statement's string somewhere, it's always the same!
                &format!(
                    "CREATE TEMP TABLE subscription_{}_{} ({})",
                    self.id.as_simple(),
                    table,
                    self.pks
                        .get(table.as_str())
                        .ok_or_else(|| MatcherError::MissingPrimaryKeys)?
                        .to_vec()
                        .join(",")
                ),
            )?
            .execute(())?;

            for pks in pks {
                tx.prepare_cached(&format!(
                    "INSERT INTO subscription_{}_{} VALUES ({})",
                    self.id.as_simple(),
                    table,
                    (0..pks.len()).map(|_i| "?").collect::<Vec<_>>().join(",")
                ))?
                .execute(params_from_iter(pks))?;
            }
        }

        let last_rowid = { *self.last_rowid.lock() };
        let mut new_last_rowid = last_rowid;

        for table in tables.iter() {
            let stmt = match self.statements.get(table.as_str()) {
                Some(stmt) => stmt,
                None => {
                    continue;
                }
            };

            let mut actual_cols = vec![];
            let mut tmp_cols = self
                .pks
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<String>>();
            for i in 0..(self.parsed.columns.len()) {
                let col_name = format!("col_{i}");
                tmp_cols.push(col_name.clone());
                actual_cols.push(col_name);
            }

            let sql = format!(
                "INSERT INTO {} ({})
                            SELECT * FROM (
                                {}
                                EXCEPT
                                {}
                            ) WHERE 1
                        ON CONFLICT({})
                            DO UPDATE SET
                                {}
                        RETURNING __corro_rowid,{}",
                // insert into
                self.qualified_table_name,
                tmp_cols.join(","),
                stmt.new_query,
                stmt.temp_query,
                self.pks
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(","),
                (0..(self.parsed.columns.len()))
                    .map(|i| format!("col_{i} = excluded.col_{i}"))
                    .collect::<Vec<_>>()
                    .join(","),
                actual_cols.join(",")
            );

            // println!("sql: {sql}");

            let insert_prepped = tx.prepare_cached(&sql)?;

            let sql = format!(
                "
                    DELETE FROM {} WHERE ({}) in (SELECT {} FROM (
                        {}
                        EXCEPT
                        {}
                    )) RETURNING __corro_rowid,{}
                ",
                // delete from
                self.qualified_table_name,
                self.pks
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(","),
                self.pks
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<Vec<String>>()
                    .join(","),
                stmt.temp_query,
                stmt.new_query,
                actual_cols.join(",")
            );

            let delete_prepped = tx.prepare_cached(&sql)?;

            for (mut change_type, mut prepped) in [
                (None, insert_prepped),
                (Some(ChangeType::Delete), delete_prepped),
            ] {
                let col_count = prepped.column_count();

                let mut rows = prepped.raw_query();

                while let Ok(Some(row)) = rows.next() {
                    let rowid: i64 = row.get(0)?;

                    let change_type = change_type.take().unwrap_or({
                        if rowid > last_rowid {
                            ChangeType::Insert
                        } else {
                            ChangeType::Update
                        }
                    });

                    new_last_rowid = cmp::max(new_last_rowid, rowid);

                    match (1..col_count)
                        .map(|i| row.get::<_, SqliteValue>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                    {
                        Ok(cells) => {
                            if let Err(e) = self.evt_tx.blocking_send(QueryEvent::Change(
                                change_type,
                                rowid,
                                cells,
                            )) {
                                debug!("could not send back row to matcher sub sender: {e}");
                                return Err(MatcherError::EventReceiverClosed);
                            }
                        }
                        Err(e) => {
                            error!("could not deserialize row's cells: {e}");
                            return Ok(());
                        }
                    }
                }
            }
        }

        // clean up temporary tables immediately
        for table in tables {
            tx.prepare_cached(&format!(
                "DROP TABLE subscription_{}_{table}",
                self.id.as_simple(),
            ))?
            .execute(())?;
            trace!("cleaned up subscription_{}_{table}", self.id.as_simple());
        }

        tx.commit()?;

        *self.last_rowid.lock() = new_last_rowid;

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct ParsedSelect {
    table_columns: IndexMap<String, HashSet<String>>,
    aliases: HashMap<String, String>,
    pub columns: Vec<ResultColumn>,
    children: Vec<ParsedSelect>,
}

fn extract_select_columns(
    select: &Select,
    schema: &NormalizedSchema,
) -> Result<ParsedSelect, MatcherError> {
    let mut parsed = ParsedSelect::default();

    if let OneSelect::Select {
        ref from,
        ref columns,
        ref where_clause,
        ..
    } = select.body.select
    {
        let from_table = match from {
            Some(from) => {
                let from_table = match &from.select {
                    Some(table) => match table.as_ref() {
                        SelectTable::Table(name, alias, _) => {
                            if schema.tables.contains_key(name.name.0.as_str()) {
                                if let Some(As::As(alias) | As::Elided(alias)) = alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                } else if let Some(ref alias) = name.alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                }
                                parsed.table_columns.entry(name.name.0.clone()).or_default();
                                Some(&name.name)
                            } else {
                                return Err(MatcherError::TableNotFound(name.name.0.clone()));
                            }
                        }
                        // TODO: add support for:
                        // TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
                        // Select(Select, Option<As>),
                        // Sub(FromClause, Option<As>),
                        t => {
                            warn!("ignoring {t:?}");
                            None
                        }
                    },
                    _ => {
                        // according to the sqlite3-parser docs, this can't really happen
                        // ignore!
                        unreachable!()
                    }
                };
                if let Some(ref joins) = from.joins {
                    for join in joins.iter() {
                        // let mut tbl_name = None;
                        let tbl_name = match &join.table {
                            SelectTable::Table(name, alias, _) => {
                                if let Some(As::As(alias) | As::Elided(alias)) = alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                } else if let Some(ref alias) = name.alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                }
                                parsed.table_columns.entry(name.name.0.clone()).or_default();
                                &name.name
                            }
                            // TODO: add support for:
                            // TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
                            // Select(Select, Option<As>),
                            // Sub(FromClause, Option<As>),
                            t => {
                                warn!("ignoring JOIN's non-SelectTable::Table:  {t:?}");
                                continue;
                            }
                        };
                        // ON or USING
                        if let Some(constraint) = &join.constraint {
                            match constraint {
                                JoinConstraint::On(expr) => {
                                    extract_expr_columns(expr, schema, &mut parsed)?;
                                }
                                JoinConstraint::Using(names) => {
                                    let entry =
                                        parsed.table_columns.entry(tbl_name.0.clone()).or_default();
                                    for name in names.iter() {
                                        entry.insert(name.0.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(expr) = where_clause {
                    extract_expr_columns(expr, schema, &mut parsed)?;
                }
                from_table
            }
            _ => None,
        };

        extract_columns(columns.as_slice(), from_table, schema, &mut parsed)?;
    }

    Ok(parsed)
}

fn extract_expr_columns(
    expr: &Expr,
    schema: &NormalizedSchema,
    parsed: &mut ParsedSelect,
) -> Result<(), MatcherError> {
    match expr {
        // simplest case
        Expr::Qualified(tblname, colname) => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            parsed
                .table_columns
                .entry(resolved_name.clone())
                .or_default()
                .insert(colname.0.clone());
        }
        // simplest case but also mentioning the schema
        Expr::DoublyQualified(schema_name, tblname, colname) if schema_name.0 == "main" => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            parsed
                .table_columns
                .entry(resolved_name.clone())
                .or_default()
                .insert(colname.0.clone());
        }

        Expr::Name(colname) => {
            let check_col_name = unquote(&colname.0).ok().unwrap_or(colname.0.clone());

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&check_col_name) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                parsed
                    .table_columns
                    .entry(found.to_owned())
                    .or_default()
                    .insert(check_col_name);
            } else {
                return Err(MatcherError::TableForColumnNotFound {
                    col_name: check_col_name,
                });
            }
        }

        Expr::Id(colname) => {
            let check_col_name = unquote(&colname.0).ok().unwrap_or(colname.0.clone());

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&check_col_name) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                parsed
                    .table_columns
                    .entry(found.to_owned())
                    .or_default()
                    .insert(colname.0.clone());
            } else {
                if colname.0.starts_with('"') {
                    return Ok(());
                }
                return Err(MatcherError::TableForColumnNotFound {
                    col_name: colname.0.clone(),
                });
            }
        }

        Expr::Between { lhs, .. } => extract_expr_columns(lhs, schema, parsed)?,
        Expr::Binary(lhs, _, rhs) => {
            extract_expr_columns(lhs, schema, parsed)?;
            extract_expr_columns(rhs, schema, parsed)?;
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(expr) = base {
                extract_expr_columns(expr, schema, parsed)?;
            }
            for (when_expr, _then_expr) in when_then_pairs.iter() {
                // NOTE: should we also parse the then expr?
                extract_expr_columns(when_expr, schema, parsed)?;
            }
            if let Some(expr) = else_expr {
                extract_expr_columns(expr, schema, parsed)?;
            }
        }
        Expr::Cast { expr, .. } => extract_expr_columns(expr, schema, parsed)?,
        Expr::Collate(expr, _) => extract_expr_columns(expr, schema, parsed)?,
        Expr::Exists(select) => {
            parsed
                .children
                .push(extract_select_columns(select, schema)?);
        }
        Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for expr in args.iter() {
                    extract_expr_columns(expr, schema, parsed)?;
                }
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    extract_expr_columns(expr, schema, parsed)?;
                }
            }
        }
        Expr::InSelect { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            parsed.children.push(extract_select_columns(rhs, schema)?);
        }
        expr @ Expr::InTable { .. } => {
            return Err(MatcherError::UnsupportedExpr { expr: expr.clone() })
        }
        Expr::IsNull(expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }
        Expr::Like { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            extract_expr_columns(rhs, schema, parsed)?;
        }

        Expr::NotNull(expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }
        Expr::Parenthesized(parens) => {
            for expr in parens.iter() {
                extract_expr_columns(expr, schema, parsed)?;
            }
        }
        Expr::Subquery(select) => {
            parsed
                .children
                .push(extract_select_columns(select, schema)?);
        }
        Expr::Unary(_, expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }

        // no column names in there...
        // Expr::FunctionCallStar { name, filter_over } => todo!(),
        // Expr::Id(_) => todo!(),
        // Expr::Literal(_) => todo!(),
        // Expr::Raise(_, _) => todo!(),
        // Expr::Variable(_) => todo!(),
        _ => {}
    }

    Ok(())
}

fn extract_columns(
    columns: &[ResultColumn],
    from: Option<&Name>,
    schema: &NormalizedSchema,
    parsed: &mut ParsedSelect,
) -> Result<(), MatcherError> {
    let mut i = 0;
    for col in columns.iter() {
        match col {
            ResultColumn::Expr(expr, _) => {
                // println!("extracting col: {expr:?} (as: {maybe_as:?})");
                extract_expr_columns(expr, schema, parsed)?;
                parsed.columns.push(ResultColumn::Expr(
                    expr.clone(),
                    Some(As::As(Name(format!("col_{i}")))),
                ));
                i += 1;
            }
            ResultColumn::Star => {
                if let Some(tbl_name) = from {
                    if let Some(table) = schema.tables.get(&tbl_name.0) {
                        let entry = parsed.table_columns.entry(table.name.clone()).or_default();
                        for col in table.columns.keys() {
                            entry.insert(col.clone());
                            parsed.columns.push(ResultColumn::Expr(
                                Expr::Name(Name(col.clone())),
                                Some(As::As(Name(format!("col_{i}")))),
                            ));
                            i += 1;
                        }
                    } else {
                        return Err(MatcherError::TableStarNotFound {
                            tbl_name: tbl_name.0.clone(),
                        });
                    }
                } else {
                    unreachable!()
                }
            }
            ResultColumn::TableStar(tbl_name) => {
                let name = parsed
                    .aliases
                    .get(tbl_name.0.as_str())
                    .unwrap_or(&tbl_name.0);
                if let Some(table) = schema.tables.get(name) {
                    let entry = parsed.table_columns.entry(table.name.clone()).or_default();
                    for col in table.columns.keys() {
                        entry.insert(col.clone());
                        parsed.columns.push(ResultColumn::Expr(
                            Expr::Qualified(tbl_name.clone(), Name(col.clone())),
                            Some(As::As(Name(format!("col_{i}")))),
                        ));
                        i += 1;
                    }
                } else {
                    return Err(MatcherError::TableStarNotFound {
                        tbl_name: name.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn table_to_expr(
    aliases: &HashMap<String, String>,
    tbl: &NormalizedTable,
    table: &str,
    id: Uuid,
) -> Result<Expr, MatcherError> {
    let tbl_name = aliases
        .iter()
        .find_map(|(alias, actual)| (actual == table).then_some(alias))
        .cloned()
        .unwrap_or_else(|| table.to_owned());

    let expr = Expr::in_table(
        Expr::Parenthesized(
            tbl.pk
                .iter()
                .map(|pk| Expr::Qualified(Name(tbl_name.clone()), Name(pk.to_owned())))
                .collect(),
        ),
        false,
        QualifiedName::single(Name(format!("subscription_{}_{table}", id.as_simple()))),
        None,
    );

    Ok(expr)
}

#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    #[error(transparent)]
    Lexer(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("one statement is required for matching")]
    StatementRequired,
    #[error("unsupported statement")]
    UnsupportedStatement,
    #[error("at least 1 table is required in FROM / JOIN clause")]
    TableRequired,
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("table not found in schema: {0}")]
    TableNotFound(String),
    #[error("no primary key for table: {0}")]
    NoPrimaryKey(String),
    #[error("aggregate missing primary key {0}.{1}")]
    AggPrimaryKeyMissing(String, String),
    #[error("JOIN .. ON expression is not supported for join on table '{table}': {expr:?}")]
    JoinOnExprUnsupported { table: String, expr: Expr },
    #[error("expression is not supported: {expr:?}")]
    UnsupportedExpr { expr: Expr },
    #[error("could not find table for {tbl_name}.* in corrosion's schema")]
    TableStarNotFound { tbl_name: String },
    #[error("<tbl>.{col_name} qualification required for ambiguous column name")]
    QualificationRequired { col_name: String },
    #[error("could not find table for column {col_name}")]
    TableForColumnNotFound { col_name: String },
    #[error("missing primary keys, this shouldn't happen")]
    MissingPrimaryKeys,
    #[error("change queue has been closed or is full")]
    ChangeQueueClosedOrFull,
    #[error("change receiver is closed")]
    EventReceiverClosed,
    #[error(transparent)]
    Unpack(#[from] UnpackError),
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use corro_api_types::row_to_change;
    use rusqlite::params;

    use crate::{
        schema::{make_schema_inner, parse_sql},
        sqlite::{setup_conn, CrConn},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_matcher() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let schema_sql = "CREATE TABLE sw (pk TEXT primary key, sandwich TEXT);";
        let mut schema = parse_sql(schema_sql)?;

        let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

        let id = Uuid::new_v4();

        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        let mut conn = CrConn::init(rusqlite::Connection::open(&db_path)?)?;

        setup_conn(
            &mut conn,
            &[(
                tmpdir
                    .path()
                    .join("subscriptions.db")
                    .display()
                    .to_string()
                    .into(),
                "subscriptions".into(),
            )]
            .into(),
        )?;

        {
            let tx = conn.transaction()?;
            make_schema_inner(&tx, &NormalizedSchema::default(), &mut schema)?;
            tx.commit()?;
        }

        let mut matcher_conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        setup_conn(
            &mut matcher_conn,
            &[(
                tmpdir
                    .path()
                    .join("subscriptions.db")
                    .display()
                    .to_string()
                    .into(),
                "subscriptions".into(),
            )]
            .into(),
        )?;

        let (tx, _rx) = mpsc::channel(1);
        let _matcher = Matcher::create(id, &schema, matcher_conn, tx, sql)?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_diff() {
        let sql = "SELECT json_object(
            'targets', json_array(cs.address||':'||cs.port),
            'labels',  json_object(
              '__metrics_path__', JSON_EXTRACT(cs.meta, '$.path'),
              'app',            cs.app_name,
              'vm_account_id',  cs.organization_id,
              'instance',       cs.instance_id
            )
          )
          FROM consul_services cs
            LEFT JOIN machines m                   ON m.id = cs.instance_id
            LEFT JOIN machine_versions mv          ON m.id = mv.machine_id  AND m.machine_version_id = mv.id
            LEFT JOIN machine_version_statuses mvs ON m.id = mvs.machine_id AND m.machine_version_id = mvs.id
          WHERE cs.node = 'test-hostname'
            AND (mvs.status IS NULL OR mvs.status = 'started')
            AND cs.name == 'app-prometheus'";

        let schema_sql = "
          CREATE TABLE consul_services (
              node TEXT NOT NULL,
              id TEXT NOT NULL,
              name TEXT NOT NULL DEFAULT '',
              tags TEXT NOT NULL DEFAULT '[]',
              meta TEXT NOT NULL DEFAULT '{}',
              port INTEGER NOT NULL DEFAULT 0,
              address TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)), network_id INTEGER AS (
                  CAST(JSON_EXTRACT(meta, '$.network_id') AS INTEGER)
              ), app_name TEXT AS (JSON_EXTRACT(meta, '$.app_name')), instance_id TEXT AS (
                  COALESCE(
                      JSON_EXTRACT(meta, '$.machine_id'),
                      SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8),
                      CASE
                          WHEN INSTR(id, '_nomad-task-') = 1 THEN SUBSTR(id, 13, 8)
                          ELSE NULL
                      END
                  )
              ), organization_id INTEGER AS (
                  CAST(
                      JSON_EXTRACT(meta, '$.organization_id') AS INTEGER
                  )
              ), protocol TEXT
          AS (JSON_EXTRACT(meta, '$.protocol')),
              PRIMARY KEY (node, id)
          );
  
          CREATE TABLE machines (
              id TEXT NOT NULL PRIMARY KEY,
              node TEXT NOT NULL DEFAULT '',
              name TEXT NOT NULL DEFAULT '',
              machine_version_id TEXT NOT NULL DEFAULT '',
              app_id INTEGER NOT NULL DEFAULT 0,
              organization_id INTEGER NOT NULL DEFAULT 0,
              network_id INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL DEFAULT 0
          );
  
          CREATE TABLE machine_versions (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL DEFAULT '',
              config TEXT NOT NULL DEFAULT '{}',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );
  
          CREATE TABLE machine_version_statuses (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );
          ";

        let mut schema = parse_sql(schema_sql).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        let mut conn =
            CrConn::init(rusqlite::Connection::open(&db_path).expect("could not open conn"))
                .expect("could not init crsql");

        setup_conn(
            &mut conn,
            &[(
                tmpdir
                    .path()
                    .join("subscriptions.db")
                    .display()
                    .to_string()
                    .into(),
                "subscriptions".into(),
            )]
            .into(),
        )
        .unwrap();

        {
            let tx = conn.transaction().unwrap();
            make_schema_inner(&tx, &NormalizedSchema::default(), &mut schema).unwrap();
            tx.commit().unwrap();
        }

        // let's seed some data in there
        {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-1', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-1"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-1', 'mv-1', 'started');

                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-2', 'not-app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-2"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-2', 'mv-2', 'started');
                    "#).unwrap();
            tx.commit().unwrap();
        }

        {
            let mut conn2 = CrConn::init(
                rusqlite::Connection::open(tmpdir.path().join("test2.db"))
                    .expect("could not open conn"),
            )
            .expect("could not init crsql");

            setup_conn(
                &mut conn2,
                &[(
                    tmpdir
                        .path()
                        .join("subscriptions.db")
                        .display()
                        .to_string()
                        .into(),
                    "subscriptions".into(),
                )]
                .into(),
            )
            .unwrap();

            {
                let tx = conn2.transaction().unwrap();
                make_schema_inner(&tx, &NormalizedSchema::default(), &mut schema).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([1], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            let tx = conn2.transaction().unwrap();

            for change in changes {
                tx.prepare_cached(
                    r#"
                    INSERT INTO crsql_changes
                        ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                    VALUES
                        (?,       ?,  ?,   ?,   ?,           ?,          ?,       ?,  ?)
                "#,
                )
                .unwrap()
                .execute(params![
                    change.table.as_str(),
                    change.pk,
                    change.cid.as_str(),
                    &change.val,
                    change.col_version,
                    change.db_version,
                    &change.site_id,
                    change.cl,
                    change.seq,
                ])
                .unwrap();
            }
        }

        let id = Uuid::new_v4();

        let mut matcher_conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        setup_conn(
            &mut matcher_conn,
            &[(
                tmpdir
                    .path()
                    .join("subscriptions.db")
                    .display()
                    .to_string()
                    .into(),
                "subscriptions".into(),
            )]
            .into(),
        )
        .unwrap();

        {
            let (tx, mut rx) = mpsc::channel(1);
            let matcher = Matcher::create(id, &schema, matcher_conn, tx, sql).unwrap();

            assert!(matches!(rx.recv().await.unwrap(), QueryEvent::Columns(_)));

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            assert_eq!(rx.recv().await.unwrap(), QueryEvent::Row(1, cells));
            assert!(matches!(
                rx.recv().await.unwrap(),
                QueryEvent::EndOfQuery { .. }
            ));

            // insert the second row
            {
                let tx = conn.transaction().unwrap();
                tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-3', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-3"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-3', 'mv-3');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-3', 'mv-3');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-3', 'mv-3', 'started');
            "#).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([2], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Insert, 2, cells)
            );

            // delete the first row
            {
                let tx = conn.transaction().unwrap();
                tx.execute_batch(r#"
                        DELETE FROM consul_services where node = 'test-hostname' AND id = 'service-1';
                    "#).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([3], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    println!("change: {row:?}");
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Delete, 1, cells)
            );

            // update the second row
            {
                let tx = conn.transaction().unwrap();
                let n = tx.execute(r#"
                        UPDATE consul_services SET address = '127.0.0.2' WHERE node = 'test-hostname' AND id = 'service-3';
                    "#, ()).unwrap();
                assert_eq!(n, 1);
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([4], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    println!("change: {row:?}");
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.2:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Update, 2, cells)
            );

            // lots of operations

            let range = 4u32..1000u32;

            {
                let tx = conn.transaction().unwrap();

                for n in range.clone() {
                    let svc_id = format!("service-{n}");
                    let ip = Ipv4Addr::from(n).to_string();
                    let port = n;
                    let machine_id = format!("m-{n}");
                    let mv = format!("mv-{n}");
                    let meta =
                        serde_json::json!({"path": format!("/path-{n}"), "machine_id": machine_id});
                    tx.execute("
                                    INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', ?, 'app-prometheus', ?, ?, ?);
                                    ", params![svc_id, ip, port, meta]).unwrap();
                    tx.execute(
                        "
                        INSERT INTO machines (id, machine_version_id) VALUES (?, ?);
                        ",
                        params![machine_id, mv],
                    )
                    .unwrap();

                    tx.execute(
                        "
                        INSERT INTO machine_versions (machine_id, id) VALUES (?, ?);
                        ",
                        params![machine_id, mv],
                    )
                    .unwrap();

                    tx.execute("
                        INSERT INTO machine_version_statuses (machine_id, id, status) VALUES (?, ?, 'started');", params![machine_id, mv]).unwrap();
                }

                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([5], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(changes.as_slice()).unwrap();

            let start = Instant::now();
            for _ in range {
                assert!(rx.recv().await.is_some());
                // FIXME: test ordering... it's actually not right!
            }

            println!("took: {:?}", start.elapsed());
        }
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_some_load() {

    // }
}
