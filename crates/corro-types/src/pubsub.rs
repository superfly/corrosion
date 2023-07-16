use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Buf;
use compact_str::{CompactString, ToCompactString};
use fallible_iterator::FallibleIterator;
use indexmap::IndexMap;
use itertools::Itertools;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, JoinConstraint, Name, OneSelect, Operator, QualifiedName, ResultColumn,
        Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::block_in_place,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    api::QueryEvent,
    change::{Change, SqliteValue, SqliteValueRef},
    schema::{NormalizedSchema, NormalizedTable},
};

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

#[derive(PartialEq, Debug)]
pub enum ColumnType {
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

impl ColumnType {
    fn from_u8(u: u8) -> Option<Self> {
        Some(match u {
            1 => Self::Integer,
            2 => Self::Float,
            3 => Self::Text,
            4 => Self::Blob,
            5 => Self::Null,
            _ => return None,
        })
    }
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
        let intlen = (column_type_and_maybe_intlen >> 3 & 0xFF) as usize;

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

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Upsert,
    Delete,
}

pub enum MatcherCmd {
    ProcessChange(HashMap<CompactString, Vec<Vec<SqliteValue>>>),
    Unsubscribe,
}

#[derive(Debug, Clone)]
pub struct Matcher(pub Arc<InnerMatcher>);

#[derive(Debug, Clone)]
pub struct MatcherStmt {
    new_query: String,
    temp_query: String,
}

#[derive(Debug)]
pub struct InnerMatcher {
    pub id: Uuid,
    pub query: Stmt,
    pub statements: HashMap<String, MatcherStmt>,
    pub pks: IndexMap<String, Vec<String>>,
    pub parsed: ParsedSelect,
    pub query_table: String,
    pub qualified_table_name: String,
    pub change_tx: broadcast::Sender<QueryEvent>,
    pub cmd_tx: mpsc::Sender<MatcherCmd>,
    pub col_names: Vec<CompactString>,
    pub cancel: CancellationToken,
}

impl Matcher {
    pub fn new(
        id: Uuid,
        schema: &NormalizedSchema,
        mut conn: Connection,
        init_tx: mpsc::Sender<QueryEvent>,
        change_tx: broadcast::Sender<QueryEvent>,
        sql: &str,
        cancel: CancellationToken,
    ) -> Result<Self, MatcherError> {
        let col_names: Vec<CompactString> = {
            conn.prepare(sql)?
                .column_names()
                .into_iter()
                .map(|s| s.to_compact_string())
                .collect()
        };

        let mut parser = Parser::new(sql.as_bytes());

        let (stmt, parsed) = match parser.next()?.ok_or(MatcherError::StatementRequired)? {
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

        let mut stmt = stmt.clone();
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
                &tbl_name,
                id,
            )?;

            let mut stmt = stmt.clone();

            match &mut stmt {
                Stmt::Select(select) => match &mut select.body.select {
                    OneSelect::Select { where_clause, .. } => {
                        *where_clause = if let Some(prev) = where_clause.take() {
                            Some(Expr::Binary(Box::new(expr), Operator::And, Box::new(prev)))
                        } else {
                            Some(expr)
                        };
                    }
                    _ => {}
                },
                _ => {}
            }

            let mut new_query = Cmd::Stmt(stmt).to_string();
            new_query.pop();

            let mut tmp_cols = pks.values().cloned().flatten().collect::<Vec<String>>();
            for i in 0..(parsed.columns.len()) {
                tmp_cols.push(format!("col_{i}"));
            }

            let pk_cols = pks
                .get(tbl_name)
                .cloned()
                .ok_or(MatcherError::MissingPrimaryKeys)?
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",");

            statements.insert(
                tbl_name.clone(),
                MatcherStmt {
                    new_query,
                    temp_query: format!(
                        "SELECT {} FROM {} WHERE ({}) IN watch_{}_{}",
                        tmp_cols.join(","),
                        query_table,
                        pk_cols,
                        id.as_simple(),
                        tbl_name,
                    ),
                },
            );
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel(512);

        let matcher = Self(Arc::new(InnerMatcher {
            id,
            query: stmt,
            statements: statements,
            pks,
            parsed,
            qualified_table_name: format!("watches.{query_table}"),
            query_table,
            change_tx,
            cmd_tx,
            col_names: col_names.clone(),
            cancel: cancel.clone(),
        }));

        let mut tmp_cols = matcher
            .0
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        for i in 0..(matcher.0.parsed.columns.len()) {
            tmp_cols.push(format!("col_{i}"));
        }

        let create_temp_table = format!(
            "CREATE TABLE {} (__corro_rowid INTEGER PRIMARY KEY AUTOINCREMENT, {});
            CREATE UNIQUE INDEX watches.index_{}_pk ON {} ({});",
            matcher.0.qualified_table_name,
            tmp_cols.join(","),
            matcher.0.id.as_simple(),
            matcher.0.query_table,
            matcher
                .0
                .pks
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        );

        conn.execute_batch(&create_temp_table)?;

        tokio::spawn({
            let matcher = matcher.clone();
            async move {
                let _drop_guard = cancel.clone().drop_guard();
                if let Err(e) = init_tx.send(QueryEvent::Columns(col_names)).await {
                    error!("could not send back columns, probably means no receivers! {e}");
                    return;
                }

                let mut query_cols = vec![];
                for i in 0..(matcher.0.parsed.columns.len()) {
                    query_cols.push(format!("col_{i}"));
                }

                let res = block_in_place(|| {
                    let tx = conn.transaction()?;

                    let mut stmt_str = Cmd::Stmt(matcher.0.query.clone()).to_string();
                    stmt_str.pop();

                    let insert_into = format!(
                        "INSERT INTO {} ({}) {} RETURNING __corro_rowid,{}",
                        matcher.0.qualified_table_name,
                        tmp_cols.join(","),
                        stmt_str,
                        query_cols.join(","),
                    );

                    {
                        let mut prepped = tx.prepare(&insert_into)?;

                        let mut rows = prepped.query(())?;

                        loop {
                            match rows.next() {
                                Ok(Some(row)) => {
                                    let rowid: i64 = row.get(0)?;
                                    let cells = (1..=query_cols.len())
                                        .map(|i| row.get::<_, SqliteValue>(i))
                                        .collect::<rusqlite::Result<Vec<_>>>()?;

                                    if let Err(e) = init_tx.blocking_send(QueryEvent::Row {
                                        change_type: ChangeType::Upsert,
                                        rowid,
                                        cells,
                                    }) {
                                        error!("could not send back row: {e}");
                                        return Err(MatcherError::ChangeReceiverClosed);
                                    }
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
                    }

                    tx.commit()?;

                    Ok::<_, MatcherError>(())
                });

                if let Err(e) = res {
                    _ = init_tx.send(QueryEvent::Error(e.to_compact_string())).await;
                    return;
                }

                if let Err(e) = init_tx.send(QueryEvent::EndOfQuery).await {
                    error!("could not send back end-of-query message: {e}");
                    return;
                }

                drop(init_tx);

                loop {
                    let req = tokio::select! {
                        Some(req) = cmd_rx.recv() => req,
                        _ = cancel.cancelled() => return,
                        else => return,
                    };

                    match req {
                        MatcherCmd::ProcessChange(candidates) => {
                            if let Err(e) =
                                block_in_place(|| matcher.handle_change(&mut conn, candidates))
                            {
                                if matches!(e, MatcherError::ChangeReceiverClosed) {
                                    // break here...
                                    continue;
                                }
                                error!("could not handle change: {e}");
                            }
                        }
                        MatcherCmd::Unsubscribe => {
                            if matcher.0.change_tx.receiver_count() == 0 {
                                info!(
                                    "matcher {} has no more subscribers, we're done!",
                                    matcher.0.id
                                );
                                break;
                            }
                        }
                    }
                }

                debug!(id = %id, "matcher loop is done");

                if let Err(e) =
                    conn.execute_batch(&format!("DROP TABLE {}", matcher.0.qualified_table_name))
                {
                    warn!(
                        "could not clean up temporary table {} => {e}",
                        matcher.0.qualified_table_name
                    );
                }
            }
        });

        Ok(matcher)
    }

    pub fn cmd_tx(&self) -> &mpsc::Sender<MatcherCmd> {
        &self.0.cmd_tx
    }

    pub fn process_change<'a>(&self, changes: &[Change]) -> Result<(), MatcherError> {
        // println!("{:?}", self.0.parsed);

        let mut candidates: HashMap<CompactString, Vec<Vec<SqliteValue>>> = HashMap::new();

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

        // let stmt = if let Some(stmt) = self.0.statements.get(agg.table) {
        //     stmt
        // } else {
        //     trace!("irrelevant table!");
        //     return Ok(());
        // };

        self.0
            .cmd_tx
            .try_send(MatcherCmd::ProcessChange(candidates))
            .map_err(|_| MatcherError::ChangeQueueClosedOrFull)?;

        Ok(())
    }

    pub fn table_name(&self) -> &str {
        &self.0.qualified_table_name
    }

    pub fn handle_change(
        &self,
        conn: &mut Connection,
        candidates: HashMap<CompactString, Vec<Vec<SqliteValue>>>,
    ) -> Result<(), MatcherError> {
        let tx = conn.transaction()?;

        let tables = candidates.keys().cloned().collect::<Vec<_>>();

        for (table, pks) in candidates {
            // TODO: cache the statement string somewhere, it's always the same!
            tx.prepare_cached(&format!(
                "CREATE TEMP TABLE watch_{}_{} ({})",
                self.0.id.as_simple(),
                table,
                self.0
                    .pks
                    .get(table.as_str())
                    .ok_or_else(|| MatcherError::MissingPrimaryKeys)?
                    .iter()
                    .map(|s| s.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            ))?
            .execute(())?;

            for pks in pks {
                tx.prepare_cached(&format!(
                    "INSERT INTO watch_{}_{} VALUES ({})",
                    self.0.id.as_simple(),
                    table,
                    (0..pks.len()).map(|_i| "?").collect::<Vec<_>>().join(",")
                ))?
                .execute(params_from_iter(pks))?;
            }
        }

        for table in tables.iter() {
            let stmt = match self.0.statements.get(table.as_str()) {
                Some(stmt) => stmt,
                None => {
                    continue;
                }
            };

            let mut actual_cols = vec![];
            let mut tmp_cols = self
                .0
                .pks
                .values()
                .cloned()
                .flatten()
                .collect::<Vec<String>>();
            for i in 0..(self.0.parsed.columns.len()) {
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
                self.0.qualified_table_name,
                tmp_cols.join(","),
                stmt.new_query,
                stmt.temp_query,
                self.0
                    .pks
                    .values()
                    .cloned()
                    .flatten()
                    .collect::<Vec<String>>()
                    .join(","),
                (0..(self.0.parsed.columns.len()))
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
        )) RETURNING __corro_rowid,{}",
                // delete from
                self.0.qualified_table_name,
                self.0
                    .pks
                    .values()
                    .cloned()
                    .flatten()
                    .collect::<Vec<String>>()
                    .join(","),
                self.0
                    .pks
                    .values()
                    .cloned()
                    .flatten()
                    .collect::<Vec<String>>()
                    .join(","),
                stmt.temp_query,
                stmt.new_query,
                actual_cols.join(",")
            );

            let delete_prepped = tx.prepare_cached(&sql)?;

            for (change_type, mut prepped) in [
                (ChangeType::Upsert, insert_prepped),
                (ChangeType::Delete, delete_prepped),
            ] {
                let col_count = prepped.column_count();

                let mut rows = prepped.raw_query();

                while let Ok(Some(row)) = rows.next() {
                    let rowid: i64 = row.get(0)?;

                    match (1..col_count)
                        .map(|i| row.get::<_, SqliteValue>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                    {
                        Ok(cells) => {
                            if let Err(e) = self.0.change_tx.send(QueryEvent::Row {
                                rowid,
                                change_type,
                                cells,
                            }) {
                                error!("could not send back row to matcher sub sender: {e}");
                                return Err(MatcherError::ChangeReceiverClosed);
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
                "DROP TABLE watch_{}_{}",
                self.0.id.as_simple(),
                table
            ))?
            .execute(())?;
        }

        tx.commit()?;

        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<QueryEvent> {
        self.0.change_tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.0.change_tx.receiver_count()
    }

    pub fn cancel(&self) -> CancellationToken {
        self.0.cancel.clone()
    }
}

#[derive(Debug, Default)]
pub struct ParsedSelect {
    table_columns: IndexMap<String, HashSet<String>>,
    aliases: HashMap<String, String>,
    pub columns: Vec<ResultColumn>,
    children: Vec<Box<ParsedSelect>>,
}

fn extract_select_columns(
    select: &Select,
    schema: &NormalizedSchema,
) -> Result<ParsedSelect, MatcherError> {
    let mut parsed = ParsedSelect::default();

    match select.body.select {
        OneSelect::Select {
            ref from,
            ref columns,
            ref where_clause,
            ..
        } => {
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
                                        let entry = parsed
                                            .table_columns
                                            .entry(tbl_name.0.clone())
                                            .or_default();
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
        _ => {}
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
            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&colname.0) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: colname.0.clone(),
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
                return Err(MatcherError::TableForColumnNotFound {
                    col_name: colname.0.clone(),
                });
            }
        }
        Expr::Id(colname) => {
            if colname.0.starts_with('"') {
                return Ok(());
            }

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&colname.0) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: colname.0.clone(),
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
                .push(Box::new(extract_select_columns(select, schema)?));
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
            parsed
                .children
                .push(Box::new(extract_select_columns(rhs, schema)?));
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
                .push(Box::new(extract_select_columns(select, schema)?));
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
        QualifiedName::single(Name(format!("watch_{}_{table}", id.as_simple()))),
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
    ChangeReceiverClosed,
    #[error(transparent)]
    Unpack(#[from] UnpackError),
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::{
        change::{row_to_change, SqliteValue},
        schema::{make_schema_inner, parse_sql},
        sqlite::{init_cr_conn, setup_conn},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_matcher() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let schema_sql = "CREATE TABLE sw (pk TEXT primary key, sandwich TEXT);";
        let schema = parse_sql(schema_sql)?;

        let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

        let cancel = CancellationToken::new();
        let id = Uuid::new_v4();

        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        let mut conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        init_cr_conn(&mut conn)?;

        setup_conn(
            &mut conn,
            &[(
                tmpdir
                    .path()
                    .join("watches.db")
                    .display()
                    .to_string()
                    .into(),
                "watches".into(),
            )]
            .into(),
        )?;

        {
            let tx = conn.transaction()?;
            make_schema_inner(&tx, &NormalizedSchema::default(), &schema)?;
            tx.commit()?;
        }

        let mut matcher_conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        setup_conn(
            &mut matcher_conn,
            &[(
                tmpdir
                    .path()
                    .join("watches.db")
                    .display()
                    .to_string()
                    .into(),
                "watches".into(),
            )]
            .into(),
        )?;

        let (tx, _rx) = mpsc::channel(1);
        let (change_tx, _change_rx) = broadcast::channel(1);
        let _matcher = Matcher::new(id, &schema, matcher_conn, tx, change_tx, sql, cancel)?;

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

        let schema = parse_sql(schema_sql).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        let mut conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        init_cr_conn(&mut conn).unwrap();

        setup_conn(
            &mut conn,
            &[(
                tmpdir
                    .path()
                    .join("watches.db")
                    .display()
                    .to_string()
                    .into(),
                "watches".into(),
            )]
            .into(),
        )
        .unwrap();

        {
            let tx = conn.transaction().unwrap();
            make_schema_inner(&tx, &NormalizedSchema::default(), &schema).unwrap();
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
            let mut conn2 = rusqlite::Connection::open(tmpdir.path().join("test2.db"))
                .expect("could not open conn");

            init_cr_conn(&mut conn2).unwrap();

            setup_conn(
                &mut conn2,
                &[(
                    tmpdir
                        .path()
                        .join("watches.db")
                        .display()
                        .to_string()
                        .into(),
                    "watches".into(),
                )]
                .into(),
            )
            .unwrap();

            {
                let tx = conn2.transaction().unwrap();
                make_schema_inner(&tx, &NormalizedSchema::default(), &schema).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
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
                        ("table", pk, cid, val, col_version, db_version, seq, site_id)
                    VALUES
                        (?,       ?,  ?,   ?,   ?,           ?,          ?,    ?)
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
                    change.seq,
                    &change.site_id
                ])
                .unwrap();
            }
        }

        let cancel = CancellationToken::new();
        let id = Uuid::new_v4();

        let mut matcher_conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        setup_conn(
            &mut matcher_conn,
            &[(
                tmpdir
                    .path()
                    .join("watches.db")
                    .display()
                    .to_string()
                    .into(),
                "watches".into(),
            )]
            .into(),
        )
        .unwrap();

        {
            let (tx, mut rx) = mpsc::channel(1);
            let (change_tx, mut change_rx) = broadcast::channel(1);
            let matcher =
                Matcher::new(id, &schema, matcher_conn, tx, change_tx, sql, cancel).unwrap();

            assert!(matches!(rx.recv().await.unwrap(), QueryEvent::Columns(_)));

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Row {
                    rowid: 1,
                    change_type: ChangeType::Upsert,
                    cells
                }
            );
            assert!(matches!(rx.recv().await.unwrap(), QueryEvent::EndOfQuery));

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
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([2], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(&changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                change_rx.recv().await.unwrap(),
                QueryEvent::Row {
                    rowid: 2,
                    change_type: ChangeType::Upsert,
                    cells
                }
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
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([3], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_change(&changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            assert_eq!(
                change_rx.recv().await.unwrap(),
                QueryEvent::Row {
                    rowid: 1,
                    change_type: ChangeType::Delete,
                    cells
                }
            );
        }
    }
}
