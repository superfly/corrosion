use fallible_iterator::FallibleIterator;
use std::collections::{HashMap, HashSet};

use enquote::unquote;
use indexmap::IndexMap;

use disjoint_hash_set::DisjointHashSet;
use sqlite3_parser::{ast::*, lexer::sql::Parser};
use tracing::{info, warn};
use uuid::Uuid;

use crate::schema::{Schema, Table};

#[derive(Debug, Clone)]
pub struct MatcherStmt {
    pub(crate) new_query: String,
    pub(crate) temp_query: String,
}

impl MatcherStmt {
    pub fn new_query(&self) -> &String {
        &self.new_query
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ExprColumnRef {
    pub table: TableDef,
    // Depth in the query scope (0 for top-level, 1 for first subquery, etc..)
    // Expressions can reffer to columns from parent scopes
    pub table_scope_depth: u32,
    pub column_name: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TableDef {
    pub real_table: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct ParsedSelect {
    // All used columns from defined tables are listed here
    pub tables: IndexMap<TableDef, HashSet<String>>,
    // Aliasses are unique per scope
    pub table_by_alias: HashMap<String, TableDef>,
    // Multiple real tables can be used at the same time under different aliasses
    pub table_by_real_name: HashMap<String, Vec<TableDef>>,
    // That's a quirk with how USING constrains work
    // The columns referenced on the right side of the USING clause are not returned by * and are not resolvable without a namespace
    // This makes usually ambiguous column references valid .-.
    pub column_omissions: HashSet<ExprColumnRef>,
    // The columns returned by this query
    pub result_columns: Vec<ResultColumn>,
    // Subqueries and stuff, currently mostly ignored
    // They are included when querying for changes but not used for matching
    // For ex. SELECT * FROM ... WHERE EXISTS (SELECT * FROM other_table WHERE ...)
    // Will work as a filter but won't the subscription won't be reactive to changes in other_table
    pub children: Vec<ParsedSelect>,
    // ConstantId => ExprColumnRef
    // Those are columns which are bound to the same constant value (e.g., WHERE column = 'value')
    pub constant_columns: HashMap<String, HashSet<ExprColumnRef>>,
    // Equalities which must hold gathered from WHERE/ON constraints like col1 = col2
    pub equal_columns: DisjointHashSet<ExprColumnRef>,
}

impl Default for ParsedSelect {
    fn default() -> Self {
        Self {
            tables: IndexMap::new(),
            table_by_alias: HashMap::new(),
            table_by_real_name: HashMap::new(),
            column_omissions: HashSet::new(),
            result_columns: Vec::new(),
            children: Vec::new(),
            constant_columns: HashMap::new(),
            equal_columns: DisjointHashSet::new(),
        }
    }
}

struct ParsingScope<'a> {
    parsed: ParsedSelect,
    parent_scope: Option<&'a ParsingScope<'a>>,
    depth: u32,
}

impl<'a> ParsingScope<'a> {
    fn root_scope<'b>() -> ParsingScope<'b> {
        ParsingScope {
            parsed: Default::default(),
            parent_scope: None,
            depth: 0,
        }
    }

    fn child_scope(&'a self) -> ParsingScope<'a> {
        ParsingScope {
            parsed: Default::default(),
            parent_scope: Some(self),
            depth: self.depth + 1,
        }
    }

    fn isolated_scope(&'a self) -> ParsingScope<'a> {
        ParsingScope {
            parsed: Default::default(),
            parent_scope: None,
            depth: self.depth,
        }
    }

    fn without_parent(&self) -> ParsingScope<'a> {
        ParsingScope {
            parsed: self.parsed.clone(),
            parent_scope: None,
            depth: self.depth,
        }
    }

    // Called when a table is first encountered in FROM or JOIN clause
    fn add_table_to_scope<'b>(
        &mut self,
        schema: &'b Schema,
        table_name: &'b QualifiedName,
        alias: &'b Option<As>,
    ) -> Result<TableDef, SqlAnalysisError> {
        let real_table_name = &table_name.name;
        // Check if such table exists in the schema
        if !schema.tables.contains_key(&real_table_name.0) {
            return Err(SqlAnalysisError::TableNotFound(real_table_name.0.clone()));
        }
        // Assume that if we have no alias, the table name is the alias
        let resolved_alias = if let Some(As::As(alias) | As::Elided(alias)) = alias {
            alias.0.clone()
        } else if let Some(ref alias) = table_name.alias {
            alias.0.clone()
        } else {
            real_table_name.0.clone()
        };
        let table = TableDef {
            real_table: real_table_name.0.clone(),
            alias: resolved_alias.clone(),
        };
        // Within the same scope, check for table conflicts
        // Aliases within the same scope must be unique or bad things happen
        // Sqlite will only error when referencing the ambiguous alias
        // Let's be strict and error immediately
        if self.parsed.table_by_alias.contains_key(&resolved_alias) {
            return Err(SqlAnalysisError::AmbigousTableReference {
                real_table: real_table_name.0.clone(),
                alias: resolved_alias.clone(),
            });
        }
        self.parsed.tables.insert(table.clone(), HashSet::new());
        self.parsed
            .table_by_alias
            .insert(resolved_alias.clone(), table.clone());
        self.parsed
            .table_by_real_name
            .entry(real_table_name.0.clone())
            .or_default()
            .push(table.clone());
        Ok(table)
    }

    fn resolve_column_reference(
        &self,
        schema: &Schema,
        namespace: Option<&str>,
        column_name: &String,
    ) -> Result<ExprColumnRef, SqlAnalysisError> {
        // If it's a double-quoted column name, let's unquote it
        let check_col_name = unquote(column_name).ok().unwrap_or(column_name.clone());

        // Look for the table in the current scope
        if let Some(namespace) = namespace {
            if let Some(table_def) = self.parsed.table_by_alias.get(namespace) {
                // Ok the table is defined in this scope, it must exist in the schema
                let table = schema.tables.get(&table_def.real_table).unwrap();
                if !table.columns.contains_key(&check_col_name) {
                    return Err(SqlAnalysisError::NoSuchColumn {
                        table: table_def.alias.clone(),
                        column: check_col_name.clone(),
                    });
                }
                return Ok(ExprColumnRef {
                    table: table_def.clone(),
                    table_scope_depth: self.depth,
                    column_name: check_col_name.clone(),
                });
            }
        } else {
            // Without an explicit namespace let's try to see if an column name matches
            let mut found = None;
            for table_def in self.parsed.tables.keys() {
                if let Some(tbl) = schema.tables.get(&table_def.real_table) {
                    if tbl.columns.contains_key(&check_col_name) {
                        // Using clause columns are not returned by * and are not resolvable without a namespace
                        if self.parsed.column_omissions.contains(&ExprColumnRef {
                            table: table_def.clone(),
                            table_scope_depth: self.depth,
                            column_name: check_col_name.clone(),
                        }) {
                            continue;
                        }
                        if found.is_some() {
                            return Err(SqlAnalysisError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(table_def.clone());
                    }
                }
            }
            // If we found an unique match, let's return it
            if let Some(table_def) = found {
                return Ok(ExprColumnRef {
                    table: table_def.clone(),
                    table_scope_depth: self.depth,
                    column_name: check_col_name.clone(),
                });
            }
        }

        // If not found look for the table in parent scopes
        if let Some(parent_scope) = self.parent_scope {
            return parent_scope.resolve_column_reference(schema, namespace, column_name);
        }

        return Err(SqlAnalysisError::TableForColumnNotFound {
            col_name: column_name.to_string(),
        });
    }

    fn register_column_reference(&mut self, schema: &Schema, column: &ExprColumnRef) {
        insert_col(
            self.parsed.tables.get_mut(&column.table).unwrap(),
            schema,
            &column.table.real_table,
            &column.column_name,
        );
    }

    fn register_eq_constraint(&mut self, left: &ExprColumnRef, right: &ExprColumnRef) {
        self.parsed.equal_columns.link(left.clone(), right.clone());
    }

    fn register_constant_constaint(&mut self, constant_id: String, column: &ExprColumnRef) {
        self.parsed
            .constant_columns
            .entry(constant_id)
            .or_default()
            .insert(column.clone());
    }
}

#[derive(Debug)]
pub struct SqlAnalysis {
    pub(crate) stmt: Stmt,
    pub(crate) pks: IndexMap<String, Vec<String>>,
    pub(crate) parsed: ParsedSelect,
    pub(crate) statements: HashMap<String, MatcherStmt>,
}

pub fn analyze_sql(
    sub_id: Uuid,
    sql: &str,
    schema: &Schema,
) -> Result<SqlAnalysis, SqlAnalysisError> {
    let sql_hash = hex::encode(seahash::hash(sql.as_bytes()).to_be_bytes());
    let mut parser = Parser::new(sql.as_bytes());

    let (mut stmt, parsed) = match parser.next()?.ok_or(SqlAnalysisError::StatementRequired)? {
        Cmd::Stmt(stmt) => {
            let parsed = match stmt {
                Stmt::Select(ref select) => extract_select_columns(select, schema, None)?,
                _ => return Err(SqlAnalysisError::UnsupportedStatement),
            };

            (stmt, parsed)
        }
        _ => return Err(SqlAnalysisError::StatementRequired),
    };

    if parsed.tables.is_empty() {
        return Err(SqlAnalysisError::TableRequired);
    }

    let mut statements = HashMap::new();

    let mut pks = IndexMap::default();

    // Appends the PKs to the result columns
    match &mut stmt {
        Stmt::Select(select) => match &mut select.body.select {
            OneSelect::Select { columns, .. } => {
                // Extract pks from all referenced tables
                let mut new_cols = parsed
                    .tables
                    .iter()
                    .filter_map(|(table_def, _cols)| {
                        schema.tables.get(&table_def.real_table).map(|table| {
                            let table_alias = &table_def.alias;
                            table
                                .pk
                                .iter()
                                .map(|pk| {
                                    let pk_alias = format!("__corro_pk_{table_alias}_{pk}");
                                    let entry: &mut Vec<String> =
                                        pks.entry(table.name.clone()).or_default();
                                    entry.push(pk_alias.clone());

                                    ResultColumn::Expr(
                                        Expr::Qualified(
                                            Name(table_alias.clone()),
                                            Name(pk.clone()),
                                        ),
                                        Some(As::As(Name(pk_alias))),
                                    )
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .flatten()
                    .collect::<Vec<_>>();

                new_cols.append(&mut parsed.result_columns.clone());
                *columns = new_cols;
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    // Generate SQL for mapping the real schema into the subscription schema
    // We need a mapping both ways filtered by the affected pks
    for (idx, (tbl_def, _cols)) in parsed.tables.iter().enumerate() {
        let limit_to_pks_expr = expr_filter_query_by_table_pk(
            tbl_def,
            schema
                .tables
                .get(&tbl_def.real_table)
                .expect("this should not happen, missing table in schema"),
        )?;

        let mut stmt = stmt.clone();

        if let Stmt::Select(select) = &mut stmt {
            if let OneSelect::Select {
                where_clause, from, ..
            } = &mut select.body.select
            {
                *where_clause = if let Some(prev) = where_clause.take() {
                    Some(Expr::Binary(
                        Box::new(limit_to_pks_expr),
                        Operator::And,
                        Box::new(Expr::parenthesized(prev)),
                    ))
                } else {
                    Some(limit_to_pks_expr)
                };

                match from {
                    Some(FromClause {
                        joins: Some(joins), ..
                    }) if idx > 0 => {
                        // Replace LEFT JOIN with INNER join if the target is the joined table
                        if let Some(JoinedSelectTable {
                            operator:
                                JoinOperator::TypedJoin {
                                    join_type:
                                        join_type @ Some(JoinType::LeftOuter | JoinType::Left),
                                    ..
                                },
                            ..
                        }) = joins.get_mut(idx - 1)
                        {
                            *join_type = Some(JoinType::Inner);
                        };

                        // Remove all custom INDEXED BY clauses for the table as the most efficient
                        // way is to query it by the primary keys
                        if let Some(JoinedSelectTable {
                            table: SelectTable::Table(_, _, indexed @ Some(_)),
                            ..
                        }) = joins.get_mut(idx - 1)
                        {
                            *indexed = None
                        };
                    }
                    _ => (),
                };
            }
        }

        // Main DB -> Sub DB
        let mut new_query = Cmd::Stmt(stmt).to_string();
        new_query.pop();

        // Sub DB filtered by PKs
        let mut all_cols = pks.values().flatten().cloned().collect::<Vec<String>>();
        for i in 0..(parsed.result_columns.len()) {
            all_cols.push(format!("col_{i}"));
        }

        let tbl_name = tbl_def.real_table.clone();
        let temp_query = format!(
            "SELECT {} FROM query WHERE ({}) IN temp_{tbl_name}",
            all_cols.join(","),
            pks.get(&tbl_name)
                .cloned()
                .ok_or(SqlAnalysisError::MissingPrimaryKeys)?
                .into_iter()
                .map(|pk| format!("coalesce({pk}, \"\")"))
                .collect::<Vec<_>>()
                .join(","),
        );

        info!(%sql_hash, %sub_id, "modified query for table '{tbl_name}': {new_query}");

        statements.insert(
            tbl_name.clone(),
            MatcherStmt {
                new_query,
                temp_query,
            },
        );
    }

    Ok(SqlAnalysis {
        stmt,
        pks,
        parsed,
        statements,
    })
}

fn extract_select_columns<'a>(
    select: &Select,
    schema: &Schema,
    scope: Option<ParsingScope<'a>>,
) -> Result<ParsedSelect, SqlAnalysisError> {
    let mut scope = scope.unwrap_or(ParsingScope::root_scope());

    if select.with.is_some() {
        return Err(SqlAnalysisError::CTENotAllowed);
    }

    if select.order_by.is_some() {
        return Err(SqlAnalysisError::OrderByNotAllowed);
    }

    if select.limit.is_some() {
        return Err(SqlAnalysisError::LimitNotAllowed);
    }

    if let OneSelect::Select {
        ref from,
        ref columns,
        ref where_clause,
        ..
    } = select.body.select
    {
        match from {
            Some(from) => {
                let from_table = match &from.select {
                    Some(table) => match table.as_ref() {
                        SelectTable::Table(name, alias, _) => {
                            scope.add_table_to_scope(schema, name, alias)?;
                        }
                        // TODO: add support for:
                        // TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
                        // Select(Select, Option<As>),
                        // Sub(FromClause, Option<As>),
                        t => {
                            warn!("ignoring {t:?}");
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
                        let (tbl_name, tbl_alias) = match &join.table {
                            SelectTable::Table(name, alias, _) => {
                                // Using is tricky so defer until we've seen the constraints
                                (name, alias)
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
                                    // Simple on clause - add table to scope and extract columns
                                    scope.add_table_to_scope(schema, tbl_name, tbl_alias)?;
                                    extract_expr_columns(expr, schema, &mut scope)?;
                                }
                                // For each pair of columns identified by a USING clause, the column from the right-hand dataset is omitted from the joined dataset.
                                // This is the only difference between a USING clause and its equivalent ON constraint.
                                JoinConstraint::Using(names) => {
                                    let mut isolated_scope = scope.isolated_scope();
                                    let tbl_def = isolated_scope
                                        .add_table_to_scope(schema, tbl_name, tbl_alias)?;
                                    let mut eq_columns = Vec::new();
                                    for col_name in names.iter() {
                                        // USING is very special - the name must exist on both sides of the join directly in this scope
                                        let left = scope
                                            .without_parent()
                                            .resolve_column_reference(schema, None, &col_name.0)?;
                                        let right = isolated_scope.resolve_column_reference(
                                            schema,
                                            Some(&tbl_def.alias),
                                            &col_name.0,
                                        )?;
                                        eq_columns.push((left, right));
                                    }
                                    // Great! All columns are resolvable on both sides of the join
                                    scope.add_table_to_scope(schema, tbl_name, tbl_alias)?;
                                    for (left, right) in eq_columns {
                                        scope.register_column_reference(schema, &left);
                                        scope.register_column_reference(schema, &right);
                                        scope.register_eq_constraint(&left, &right);
                                        scope.parsed.column_omissions.insert(right.clone());
                                    }
                                }
                            }
                        } else {
                            // No constrains - just add the table to the scope
                            scope.add_table_to_scope(schema, tbl_name, tbl_alias)?;
                        }
                    }
                }
                if let Some(expr) = where_clause {
                    extract_expr_columns(expr, schema, &mut scope)?;
                }
                from_table
            }
            _ => (),
        };

        // Determine which columns will be returned by this query
        extract_columns(columns.as_slice(), schema, &mut scope)?;
    }

    Ok(scope.parsed)
}

fn insert_col(set: &mut HashSet<String>, schema: &Schema, tbl_name: &str, name: &str) {
    let table = schema.tables.get(tbl_name);
    if let Some(generated) =
        table.and_then(|tbl| tbl.columns.get(name).and_then(|col| col.generated.as_ref()))
    {
        // recursively check for generated columns
        for name in generated.from.iter() {
            insert_col(set, schema, tbl_name, name);
        }
    } else {
        set.insert(name.to_owned());
    }
}

#[derive(Debug)]
enum ExtractExprResult {
    Constant(String),
    Column(ExprColumnRef),
}
fn extract_expr_columns<'a>(
    expr: &Expr,
    schema: &Schema,
    scope: &mut ParsingScope<'a>,
) -> Result<(), SqlAnalysisError> {
    match expr {
        // simplest case
        Expr::Qualified(tblname, colname) => {
            let column = scope.resolve_column_reference(schema, Some(&tblname.0), &colname.0)?;
            scope.register_column_reference(schema, &column);
        }
        // simplest case but also mentioning the schema
        Expr::DoublyQualified(schema_name, tblname, colname) if schema_name.0 == "main" => {
            let column = scope.resolve_column_reference(schema, Some(&tblname.0), &colname.0)?;
            scope.register_column_reference(schema, &column);
        }
        Expr::DoublyQualified(schema_name, _, _) => {
            return Err(SqlAnalysisError::UnknownSchema {
                schema_name: schema_name.0.clone(),
            });
        }

        Expr::Name(colname) => {
            let column = scope.resolve_column_reference(schema, None, &colname.0)?;
            scope.register_column_reference(schema, &column)
        }

        Expr::Id(colname) => {
            let res = scope.resolve_column_reference(schema, None, &colname.0);
            if let Err(e) = res {
                // https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
                // This is a double quoted string literal, not a column reference
                if colname.0.starts_with('"') {
                    return Ok(());
                }
                return Err(e);
            }
            if let Ok(column) = res {
                scope.register_column_reference(schema, &column);
            }
        }

        Expr::Between { lhs, .. } => extract_expr_columns(lhs, schema, scope)?,
        Expr::Binary(lhs, _, rhs) => {
            extract_expr_columns(lhs, schema, scope)?;
            extract_expr_columns(rhs, schema, scope)?;
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(expr) = base {
                extract_expr_columns(expr, schema, scope)?;
            }
            for (when_expr, _then_expr) in when_then_pairs.iter() {
                // NOTE: should we also parse the then expr?
                extract_expr_columns(when_expr, schema, scope)?;
            }
            if let Some(expr) = else_expr {
                extract_expr_columns(expr, schema, scope)?;
            }
        }
        Expr::Cast { expr, .. } => extract_expr_columns(expr, schema, scope)?,
        Expr::Collate(expr, _) => extract_expr_columns(expr, schema, scope)?,
        Expr::Exists(select) => {
            scope.parsed.children.push(extract_select_columns(
                select,
                schema,
                Some(scope.child_scope()),
            )?);
        }
        Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for expr in args.iter() {
                    extract_expr_columns(expr, schema, scope)?;
                }
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, scope)?;
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    extract_expr_columns(expr, schema, scope)?;
                }
            }
        }
        Expr::InSelect { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, scope)?;
            scope.parsed.children.push(extract_select_columns(
                rhs,
                schema,
                Some(scope.child_scope()),
            )?);
        }
        expr @ Expr::InTable { .. } => {
            return Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
        }
        Expr::IsNull(expr) => {
            extract_expr_columns(expr, schema, scope)?;
        }
        Expr::Like { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, scope)?;
            extract_expr_columns(rhs, schema, scope)?;
        }

        Expr::NotNull(expr) => {
            extract_expr_columns(expr, schema, scope)?;
        }
        Expr::Parenthesized(parens) => {
            for expr in parens.iter() {
                extract_expr_columns(expr, schema, scope)?;
            }
        }
        Expr::Subquery(select) => {
            scope.parsed.children.push(extract_select_columns(
                select,
                schema,
                Some(scope.child_scope()),
            )?);
        }
        Expr::Unary(_, expr) => {
            extract_expr_columns(expr, schema, scope)?;
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
    schema: &Schema,
    scope: &mut ParsingScope<'_>,
) -> Result<(), SqlAnalysisError> {
    let mut i = 0;
    for col in columns.iter() {
        match col {
            ResultColumn::Expr(expr, _) => {
                // println!("extracting col: {expr:?} (as: {maybe_as:?})");
                extract_expr_columns(expr, schema, scope)?;
                scope.parsed.result_columns.push(ResultColumn::Expr(
                    expr.clone(),
                    Some(As::As(Name(format!("col_{i}")))),
                ));
                i += 1;
            }
            ResultColumn::Star => {
                // A star is a star - it will select everything which is in the current scope including joins
                for (table_def, referenced_columns) in &mut scope.parsed.tables {
                    let table_schema = schema.tables.get(&table_def.real_table).ok_or(
                        SqlAnalysisError::TableStarNotFound {
                            tbl_name: table_def.real_table.clone(),
                        },
                    )?;
                    // Now with the table schema resolved we need to add all columns from this table
                    for col in table_schema.columns.keys() {
                        // Unless they are omitted
                        if scope.parsed.column_omissions.contains(&ExprColumnRef {
                            table: table_def.clone(),
                            table_scope_depth: scope.depth,
                            column_name: col.clone(),
                        }) {
                            continue;
                        }
                        referenced_columns.insert(col.clone());
                        scope.parsed.result_columns.push(ResultColumn::Expr(
                            Expr::Qualified(Name(table_def.alias.clone()), Name(col.clone())),
                            Some(As::As(Name(format!("col_{i}")))),
                        ));
                        i += 1;
                    }
                }
            }
            ResultColumn::TableStar(tbl_name) => {
                let (table_def, table_schema, referenced_columns) = scope
                    .parsed
                    .table_by_alias
                    .get(tbl_name.0.as_str())
                    .and_then(|table_def| {
                        Some((
                            table_def,
                            schema.tables.get(&table_def.real_table)?,
                            scope.parsed.tables.get_mut(table_def)?,
                        ))
                    })
                    .ok_or(SqlAnalysisError::TableStarNotFound {
                        tbl_name: tbl_name.0.clone(),
                    })?;
                // Now with the table schema resolved we need to add all columns from this table
                for col in table_schema.columns.keys() {
                    referenced_columns.insert(col.clone());
                    scope.parsed.result_columns.push(ResultColumn::Expr(
                        Expr::Qualified(Name(table_def.alias.clone()), Name(col.clone())),
                        Some(As::As(Name(format!("col_{i}")))),
                    ));
                    i += 1;
                }
            }
        }
    }
    Ok(())
}

fn expr_filter_query_by_table_pk(
    table_def: &TableDef,
    table_schema: &Table,
) -> Result<Expr, SqlAnalysisError> {
    let expr = Expr::in_table(
        Expr::Parenthesized(
            table_schema
                .pk
                .iter()
                .map(|pk| Expr::Qualified(Name(table_def.alias.clone()), Name(pk.to_owned())))
                .collect(),
        ),
        false,
        QualifiedName::fullname(
            Name("__corro_sub".into()),
            Name(format!("temp_{}", table_def.real_table)),
        ),
        None,
    );

    Ok(expr)
}

// Check if an expression resolves to a constant single value
// This is used to look for patterns like col_name = constant
// As in such cases we can assume col_name is unique within the query result set
// and thus we can treat the column as uniquely identifiable for the purpose of subscription matching
// This is especially important if col_name is part of a table PK for example
// SELECT * FROM foo JOIN bar ON foo.random_key = bar.random_key WHERE bar.id = ?
// Normally we would use (foo.id, bar.id) to identify the result row uniquely but due to the WHERE clause,
// we can safely use (foo.id) as bar.id => bar.random_key so bar.random_key is constant so the result depends only on foo.id
// NULLS are not considered constant single values
//
// Returns Some if that's the case. None otherwise.
// The returned string identifies the constant value, if the string is empty
// then assume it's a new identity not seen before
fn is_constant_single_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(lit) => match lit {
            Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_) => Some("".into()),
            Literal::Keyword(keyword) => {
                let l = keyword.to_string().to_lowercase();
                if l == "false" || l == "true" {
                    Some("".into())
                } else {
                    None
                }
            }
            // thing = NULL doesn't make sense
            Literal::Null => None,
            Literal::CurrentDate => None,
            Literal::CurrentTime => None,
            Literal::CurrentTimestamp => None,
        },
        Expr::Variable(variable_name) => Some(variable_name.clone()),
        Expr::Cast { expr, .. } => is_constant_single_value(expr),
        Expr::Collate(expr, ..) => is_constant_single_value(expr),
        Expr::Parenthesized(exprs) => {
            // For parenthesized expressions, check if they contain only a single constant
            // For ex SELECT (2);
            if exprs.len() == 1 {
                is_constant_single_value(&exprs[0])
            } else {
                None
            }
        }
        // (SELECT constant)
        Expr::Subquery(sub) => match sub.as_ref() {
            Select {
                with: None,
                body:
                    SelectBody {
                        select:
                            OneSelect::Select {
                                distinctness: None,
                                ref columns,
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: None,
                            },
                        compounds: None,
                    },
                order_by: None,
                limit: None,
            } => match columns.as_slice() {
                [ResultColumn::Expr(expr, _)] => is_constant_single_value(expr),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqlAnalysisError {
    #[error(transparent)]
    Lexer(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("one statement is required for matching")]
    StatementRequired,
    #[error("unsupported statement")]
    UnsupportedStatement,
    #[error("at least 1 table is required in FROM / JOIN clause")]
    TableRequired,
    #[error("table not found in schema: {0}")]
    TableNotFound(String),
    #[error("expression is not supported: {expr:?}")]
    UnsupportedExpr { expr: Expr },
    #[error("could not find table for {tbl_name}.* in corrosion's schema")]
    TableStarNotFound { tbl_name: String },
    #[error("{real_table} AS {alias} is ambiguous in scope, please disambiguate")]
    AmbigousTableReference { real_table: String, alias: String },
    #[error("ORDER BY is not allowed in matcher expressions")]
    OrderByNotAllowed,
    #[error("LIMIT is not allowed in matcher expressions")]
    LimitNotAllowed,
    #[error("CTEs are not allowed in matcher expressions")]
    CTENotAllowed,
    #[error("<tbl>.{col_name} qualification required for ambiguous column name")]
    QualificationRequired { col_name: String },
    #[error("could not find table for column {col_name}")]
    TableForColumnNotFound { col_name: String },
    #[error("missing primary keys, this shouldn't happen")]
    MissingPrimaryKeys,
    #[error("no such column: {table}.{column}")]
    NoSuchColumn { table: String, column: String },
    #[error("unknown schema: {schema_name}")]
    UnknownSchema { schema_name: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_sql;

    #[test]
    fn test_self_join() {
        let schema = parse_sql(
            "
        CREATE TABLE tree (id INTEGER PRIMARY KEY, parent_id INTEGER);
        ",
        )
        .unwrap();

        assert!(matches!(
            analyze_sql(Uuid::new_v4(), "SELECT * FROM tree JOIN tree", &schema).unwrap_err(),
            SqlAnalysisError::AmbigousTableReference {
                real_table: ref rt,
                alias: ref a
            } if rt == "tree" && a == "tree",
        ));
        assert!(matches!(
            analyze_sql(Uuid::new_v4(), "SELECT * FROM tree, tree", &schema).unwrap_err(),
            SqlAnalysisError::AmbigousTableReference {
                real_table: ref rt,
                alias: ref a
            } if rt == "tree" && a == "tree",
        ));

        // TODO:
        // This will use rec from the nested scope:
        // SELECT a.*, rec.* FROM rec AS a JOIN rec ON a.id = rec.parent WHERE EXISTS (SELECT 1 FROM rec WHERE rec.id = 2);
        // This will use rec from parent scope:
        // SELECT a.*, rec.* FROM rec AS a JOIN rec ON a.id = rec.parent WHERE EXISTS (SELECT 1 WHERE rec.id = 2);
        // This should fail due to ambiguity as id might reffer both to a and rec
        // SELECT a.*, rec.* FROM rec AS a JOIN rec ON a.id = rec.parent WHERE EXISTS (SELECT 1 WHERE id = 2);

        let analysis = analyze_sql(
            Uuid::new_v4(),
            "SELECT * FROM tree as a JOIN tree as b ON b.parent_id = a.id",
            &schema,
        )
        .unwrap();
        println!("Stmt: {:#?}", Cmd::Stmt(analysis.stmt).to_string());
        println!("Statements: {:#?}", analysis.statements);
        println!("Pks: {:#?}", analysis.pks);
        println!("Parsed: {:#?}", analysis.parsed);
    }

    #[test]
    fn test_left_join() {
        let schema = parse_sql(
            "
        CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE bar (id INTEGER PRIMARY KEY, foo_id INTEGER);
        CREATE TABLE baz (id INTEGER PRIMARY KEY, bar_id INTEGER);
        ",
        )
        .unwrap();
        let analysis = analyze_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.foo_id WHERE EXISTS (SELECT 1 FROM baz WHERE bar_id = bar.id) AND foo.name = 'test'",
            &schema,
        )
        .unwrap();
        println!("Stmt: {:#?}", Cmd::Stmt(analysis.stmt).to_string());
        println!("Statements: {:#?}", analysis.statements);
        println!("Pks: {:#?}", analysis.pks);
        println!("Parsed: {:#?}", analysis.parsed);
    }
}
