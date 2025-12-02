use fallible_iterator::FallibleIterator;
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
};

use enquote::unquote;
use indexmap::IndexMap;

use disjoint_hash_set::DisjointHashSet;
use sqlite3_parser::{ast::*, lexer::sql::Parser};
use tracing::{info, warn};
use uuid::Uuid;

use crate::schema::{Schema, Table};

// The schema name used to attach the main database to the subscription db in RO mode
pub const MAIN_DB_IN_SUB_DB_SCHEMA_NAME: &str = "corro_main";

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
pub struct ReturnExpr {
    pub sources: HashSet<ExprColumnRef>,
    pub kind: ExprKind,
    pub expr: Expr,
    // We always assign an alias to returned expressions
    pub alias: Name,
}

impl ReturnExpr {
    // Such return expressions are safe to rename
    // For ex we might promote __corro_col_0_0 to __corro_pk_{table}_{column}
    // Or __corro_col_0_1 to __corro_gk_0
    pub fn is_internal_alias(&self) -> bool {
        self.alias.0.starts_with("__corro_")
    }

    pub fn to_result_column(&self) -> ResultColumn {
        ResultColumn::Expr(self.expr.clone(), Some(As::As(self.alias.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct PkEntry {
    // The expression which will return this PK entry
    pub expr: ReturnExpr,
    // Is this entry fully functionally dependent on other PK entries?
    // If yes, then it can be omitted from minimal_pk
    // For ex in: SELECT * FROM tree AS a JOIN tree AS b ON a.id = b.parent_id
    // the func dependencies are:
    // a.id -> a.parent_id
    // b.id -> b.parent_id
    // b.parent_id -> a.id
    // Therefore for PK entries (a.id, b.id) the entry a.id is fully functionally dependent on b.id
    pub fully_dependent: bool,
    // Whether this PK entry can be used for incremental maintenance
    // This is mainly here due to the cursed case of SELECT DISTINCT ... GROUP BY ...
    // Where the PK contains all columns from the result set but we can only filter on the group keys
    pub usable_for_maintenance: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedSelect {
    // All used columns from defined tables are listed here
    pub tables: IndexMap<TableDef, HashSet<String>>,
    // Aliasses are unique per scope
    pub table_by_alias: HashMap<String, TableDef>,
    // Multiple real tables can be used at the same time under different aliasses
    pub table_by_real_name: HashMap<String, Vec<TableDef>>,
    // Column references from parent scopes
    pub parent_column_references: Vec<ExprColumnRef>,
    // That's a quirk with how USING constrains work
    // The columns referenced on the right side of the USING clause are not returned by * and are not resolvable without a namespace
    // This makes usually ambiguous column references valid .-.
    pub column_omissions: HashSet<ExprColumnRef>,
    // SELECT DISTINCT...
    pub is_distinct: bool,
    pub group_by: Option<Vec<ReturnExpr>>,
    // The columns returned by this query
    pub result_columns: Vec<ReturnExpr>,
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
            parent_column_references: Vec::new(),
            column_omissions: HashSet::new(),
            is_distinct: false,
            group_by: None,
            result_columns: Vec::new(),
            children: Vec::new(),
            constant_columns: HashMap::new(),
            equal_columns: DisjointHashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExprKind {
    Constant(String),
    Column(ExprColumnRef),
    Complex,
}

#[derive(Debug)]
struct ParsingScope<'a> {
    schema: &'a Schema,
    parsed: ParsedSelect,
    parent_scope: Option<&'a ParsingScope<'a>>,
    depth: u32,
    // When going down the AST and encountering an OR or NOT we can't gurantee that an = operator down the line will hold globally
    // For ex. (foo.id = 2) OR (bar.id = 2)
    // For ex. NOT (foo.id = 2)
    suppress_eq_constraints: bool,
    // This is used to track which columns are used in expressions
    expr_sources: Option<HashSet<ExprColumnRef>>,
}

#[derive(Debug)]
struct SuppressedEqConstraintsGuard<'a, 'b> {
    scope: &'a mut ParsingScope<'b>,
    prev: bool,
}

impl<'a, 'b> Deref for SuppressedEqConstraintsGuard<'a, 'b> {
    type Target = ParsingScope<'b>;

    fn deref(&self) -> &Self::Target {
        self.scope
    }
}

impl<'a, 'b> DerefMut for SuppressedEqConstraintsGuard<'a, 'b> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scope
    }
}

impl<'a, 'b> Drop for SuppressedEqConstraintsGuard<'a, 'b> {
    fn drop(&mut self) {
        self.scope.suppress_eq_constraints = self.prev;
    }
}

impl<'a> ParsingScope<'a> {
    fn root_scope(schema: &'a Schema) -> ParsingScope<'a> {
        ParsingScope {
            schema,
            parsed: Default::default(),
            parent_scope: None,
            depth: 0,
            // In the top scope = operators are guranteed to hold globally
            suppress_eq_constraints: false,
            expr_sources: None,
        }
    }

    fn child_scope(&'a self) -> ParsingScope<'a> {
        ParsingScope {
            schema: self.schema,
            parsed: Default::default(),
            parent_scope: Some(self),
            depth: self.depth + 1,
            // Note: We don't want to suppress eq constraints in subqueries
            // While such queries will generate constrains in the child scope
            // ... WHERE NOT EXISTS (SELECT 1 FROM foo WHERE foo.id = bar.id)
            // ... FROM baz WHERE EXISTS (SELECT 1 FROM foo WHERE foo.id = baz.foo_id AND foo.random = 2) OR EXISTS (SELECT 1 FROM bar WHERE bar.id = baz.bar_id)
            // We need to determine in the parent scope if the child constraints on parent references hold globally
            // For ex constraints in WHERE ... AND EXISTS subqueries or JOIN (subquery ...) can get propagated IF they only reffer to parent scope columns
            suppress_eq_constraints: false,
            expr_sources: None,
        }
    }

    // An empty scope with the same schema and depth
    // Column references from this scope can be used in the source scope if the table is present in the source
    fn isolated_scope(&'a self) -> ParsingScope<'a> {
        ParsingScope {
            schema: self.schema,
            parsed: Default::default(),
            parent_scope: None,
            depth: self.depth,
            suppress_eq_constraints: false,
            expr_sources: None,
        }
    }

    fn without_parent(&self) -> ParsingScope<'a> {
        ParsingScope {
            schema: self.schema,
            parsed: self.parsed.clone(),
            parent_scope: None,
            depth: self.depth,
            suppress_eq_constraints: self.suppress_eq_constraints,
            expr_sources: None,
        }
    }

    fn maybe_suppress_eq_constraints<'b>(
        &'b mut self,
        suppress: bool,
    ) -> SuppressedEqConstraintsGuard<'b, 'a> {
        // Once we suppress eq constraints, the only way to unsuppress them is to drop the guard
        let prev = self.suppress_eq_constraints;
        self.suppress_eq_constraints = prev || suppress;
        SuppressedEqConstraintsGuard { scope: self, prev }
    }

    // Called when a table is first encountered in FROM or JOIN clause
    fn add_table_to_scope<'b>(
        &mut self,
        table_name: &'b QualifiedName,
        alias: &'b Option<As>,
    ) -> Result<TableDef, SqlAnalysisError> {
        let real_table_name = &table_name.name;
        // Check if such table exists in the schema
        if !self.schema.tables.contains_key(&real_table_name.0) {
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

    // TODO: Sqlite allows to reference aliasses from the select
    // For ex this works:
    // SELECT id AS foo FROM bar GROUP BY foo
    // SELECT id AS foo FROM bar WHERE EXISTS (SELECT 1 WHERE foo = 2)
    // SELECT id AS foo FROM bar WHERE foo = 2
    // It's tricky as it has weird scoping rules. For ex. this fails:
    // SELECT id AS foo FROM bar WHERE EXISTS (SELECT 1 GROUP BY foo)
    fn resolve_column_reference(
        &self,
        namespace: Option<&str>,
        column_name: &String,
    ) -> Result<ExprColumnRef, SqlAnalysisError> {
        // If it's a double-quoted column name, let's unquote it
        // This also applies to qualified names. foo."id" is the same as foo.id
        let check_col_name = unquote(column_name).ok().unwrap_or(column_name.clone());

        // Look for the table in the current scope
        if let Some(namespace) = namespace {
            if let Some(table_def) = self.parsed.table_by_alias.get(namespace) {
                // Ok the table is defined in this scope, it must exist in the schema
                let table = self.schema.tables.get(&table_def.real_table).unwrap();
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
                if let Some(tbl) = self.schema.tables.get(&table_def.real_table) {
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
            return parent_scope.resolve_column_reference(namespace, column_name);
        }

        return Err(SqlAnalysisError::TableForColumnNotFound {
            col_name: column_name.to_string(),
        });
    }

    fn register_column_reference(&mut self, column: &ExprColumnRef) {
        if column.table_scope_depth == self.depth {
            insert_col(
                self.parsed.tables.get_mut(&column.table).unwrap(),
                self.schema,
                &column.table.real_table,
                &column.column_name,
            );
            if let Some(expr_sources) = &mut self.expr_sources {
                expr_sources.insert(column.clone());
            }
        } else if column.table_scope_depth < self.depth {
            // If we're in a subquery an expression can reference columns from the parent scope
            self.parsed.parent_column_references.push(column.clone());
            if let Some(expr_sources) = &mut self.expr_sources {
                expr_sources.insert(column.clone());
            }
        } else {
            panic!("column table scope depth is greater than current scope depth");
        }
    }

    fn register_eq_constraint(&mut self, left: &ExprColumnRef, right: &ExprColumnRef) {
        if self.suppress_eq_constraints {
            return;
        }
        self.parsed.equal_columns.link(left.clone(), right.clone());
    }

    fn register_constant_constraint(&mut self, constant_id: String, column: &ExprColumnRef) {
        if self.suppress_eq_constraints {
            return;
        }
        // Unnamed constants mean "this value is unique"
        let cid = if constant_id.is_empty() {
            format!("constant_{}", rand::random::<u64>())
        } else {
            constant_id
        };
        self.parsed
            .constant_columns
            .entry(cid)
            .or_default()
            .insert(column.clone());
    }

    fn visit_select(mut self, select: &Select) -> Result<ParsedSelect, SqlAnalysisError> {
        if select.with.is_some() {
            return Err(SqlAnalysisError::CTENotAllowed);
        }

        if select.order_by.is_some() {
            return Err(SqlAnalysisError::OrderByNotAllowed);
        }

        if select.limit.is_some() {
            return Err(SqlAnalysisError::LimitNotAllowed);
        }

        if select.body.compounds.is_some() {
            return Err(SqlAnalysisError::CompoundNotAllowed);
        }

        if let OneSelect::Select {
            ref from,
            ref columns,
            ref where_clause,
            ref distinctness,
            ref group_by,
            ..
        } = select.body.select
        {
            match from {
                Some(from) => {
                    let from_table = match &from.select {
                        Some(table) => match table.as_ref() {
                            SelectTable::Table(name, alias, _) => {
                                self.add_table_to_scope(name, alias)?;
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
                                        self.add_table_to_scope(tbl_name, tbl_alias)?;
                                        self.visit_expr(expr)?;
                                    }
                                    // For each pair of columns identified by a USING clause, the column from the right-hand dataset is omitted from the joined dataset.
                                    // This is the only difference between a USING clause and its equivalent ON constraint.
                                    JoinConstraint::Using(names) => {
                                        let mut isolated_scope = self.isolated_scope();
                                        let tbl_def = isolated_scope
                                            .add_table_to_scope(tbl_name, tbl_alias)?;
                                        let mut eq_columns = Vec::new();
                                        for col_name in names.iter() {
                                            // USING is very special - the name must exist on both sides of the join directly in this scope
                                            let left = self
                                                .without_parent()
                                                .resolve_column_reference(None, &col_name.0)?;
                                            let right = isolated_scope.resolve_column_reference(
                                                Some(&tbl_def.alias),
                                                &col_name.0,
                                            )?;
                                            eq_columns.push((left, right));
                                        }
                                        // Great! All columns are resolvable on both sides of the join
                                        self.add_table_to_scope(tbl_name, tbl_alias)?;
                                        for (left, right) in eq_columns {
                                            self.register_column_reference(&left);
                                            self.register_column_reference(&right);
                                            self.register_eq_constraint(&left, &right);
                                            self.parsed.column_omissions.insert(right.clone());
                                        }
                                    }
                                }
                            } else {
                                // No constrains - just add the table to the scope
                                self.add_table_to_scope(tbl_name, tbl_alias)?;
                            }
                        }
                    }
                    if let Some(expr) = where_clause {
                        self.visit_expr(expr)?;
                    }
                    from_table
                }
                _ => (),
            };

            // Determine which columns will be returned by this query
            self.visit_return_columns(columns.as_slice())?;

            // For SELECT DISTINCT col1, col2, ... colN
            // the PK will be (col1, col2, ... colN)
            self.parsed.is_distinct = if let Some(Distinctness::Distinct) = distinctness {
                true
            } else {
                false
            };

            if let Some(group_by) = group_by {
                let mut group_by_res = Vec::with_capacity(group_by.exprs.len());
                let mut i = 0;
                for expr in &group_by.exprs {
                    // TODO: This can refer to aliases from select columns
                    //       Also a number literal refers to the Nth column
                    //       in the result set. For now we error in such cases.
                    let (kind, sources) = self
                        .maybe_suppress_eq_constraints(true)
                        .visit_expr_with_sources(expr)?;
                    // For now error on constant expressions and literals in GROUP BY
                    if sources.len() == 0 {
                        return Err(SqlAnalysisError::UnsupportedGroupByExpression {
                            expr: expr.clone(),
                        });
                    }
                    group_by_res.push(ReturnExpr {
                        sources,
                        kind,
                        expr: expr.clone(),
                        // gk -> group key
                        alias: Name(format!("__corro_gk_{i}")),
                    });
                    i += 1;
                }
                self.parsed.group_by = Some(group_by_res);

                // TODO: Are having constrains useful for us?
                // For ex. SELECT group_key, count(*) FROM ... GROUP BY group_key HAVING foo.id = bar.foo_id
                // How does sqlite evaluate foo.id=bar.foo_id?
                // Does it take a random row from each group and evaluate the constraint?
                // Does it evaluate the constraint for each row in the group?
                // Does it allow only aggregate functions here?
                if let Some(having) = &group_by.having {
                    self.maybe_suppress_eq_constraints(true)
                        .visit_expr(having)?;
                }
            }
        }

        Ok(self.parsed)
    }

    fn visit_subquery(&mut self, subquery: &Select) -> Result<&ParsedSelect, SqlAnalysisError> {
        let child = self.child_scope().visit_select(subquery)?;
        // If the subquery reffers to columns from the parent scope, we need to register them
        for reference in &child.parent_column_references {
            self.register_column_reference(reference);
        }
        self.parsed.children.push(child);
        Ok(self.parsed.children.last().unwrap())
    }

    fn visit_expr_with_sources(
        &mut self,
        expr: &Expr,
    ) -> Result<(ExprKind, HashSet<ExprColumnRef>), SqlAnalysisError> {
        if self.expr_sources.is_some() {
            unreachable!("Did visit_expr_with_sources got called from inside visit_expr?")
        }
        self.expr_sources = Some(HashSet::new());
        let kind = self.visit_expr(expr)?;
        let sources = self.expr_sources.take().unwrap();
        Ok((kind, sources))
    }

    fn visit_expr(&mut self, expr: &Expr) -> Result<ExprKind, SqlAnalysisError> {
        match expr {
            // simplest case
            Expr::Qualified(tblname, colname) => {
                let column = self.resolve_column_reference(Some(&tblname.0), &colname.0)?;
                self.register_column_reference(&column);
                Ok(ExprKind::Column(column))
            }
            // simplest case but also mentioning the schema
            Expr::DoublyQualified(schema_name, tblname, colname) if schema_name.0 == "main" => {
                let column = self.resolve_column_reference(Some(&tblname.0), &colname.0)?;
                self.register_column_reference(&column);
                Ok(ExprKind::Column(column))
            }
            Expr::DoublyQualified(schema_name, _, _) => Err(SqlAnalysisError::UnknownSchema {
                schema_name: schema_name.0.clone(),
            }),

            Expr::Name(colname) => {
                let column = self.resolve_column_reference(None, &colname.0)?;
                self.register_column_reference(&column);
                Ok(ExprKind::Column(column))
            }

            Expr::Id(colname) => {
                match self.resolve_column_reference(None, &colname.0) {
                    Err(e) => {
                        // https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
                        // This is a double quoted string literal, not a column reference
                        if colname.0.starts_with('"') {
                            Ok(ExprKind::Constant("".to_string()))
                        } else {
                            Err(e)
                        }
                    }
                    Ok(column) => {
                        self.register_column_reference(&column);
                        Ok(ExprKind::Column(column))
                    }
                }
            }

            Expr::Between {
                lhs, start, end, ..
            } => {
                let mut scope = self.maybe_suppress_eq_constraints(true);
                scope.visit_expr(lhs)?;
                scope.visit_expr(start)?;
                scope.visit_expr(end)?;
                Ok(ExprKind::Complex)
            }
            Expr::Binary(lhs, op, rhs) => {
                let suppress_eq_constraints = match op {
                    // This is always fine: WHERE ... AND ... AND ... AND
                    Operator::And => false,
                    // Be very conservative here
                    // U know false = (foo.id = bar.id) should ignore the inner eq constraint
                    _ => true,
                };
                let (left, right) = {
                    let mut scope = self.maybe_suppress_eq_constraints(suppress_eq_constraints);
                    (scope.visit_expr(lhs)?, scope.visit_expr(rhs)?)
                };
                match (op, left, right) {
                    // Any binary operator on constants is a constant
                    (_, ExprKind::Constant(_), ExprKind::Constant(_)) => {
                        Ok(ExprKind::Constant("".to_string()))
                    }
                    // Column eq column
                    (Operator::Equals, ExprKind::Column(left), ExprKind::Column(right)) => {
                        self.register_eq_constraint(&left, &right);
                        Ok(ExprKind::Complex)
                    }
                    // Column eq constant
                    (Operator::Equals, ExprKind::Column(column), ExprKind::Constant(cid)) => {
                        self.register_constant_constraint(cid, &column);
                        Ok(ExprKind::Complex)
                    }
                    // Constant eq column
                    (Operator::Equals, ExprKind::Constant(cid), ExprKind::Column(column)) => {
                        self.register_constant_constraint(cid, &column);
                        Ok(ExprKind::Complex)
                    }

                    _ => Ok(ExprKind::Complex),
                }
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                let mut scope = self.maybe_suppress_eq_constraints(true);
                if let Some(expr) = base {
                    scope.visit_expr(expr)?;
                }
                for (when_expr, then_expr) in when_then_pairs.iter() {
                    scope.visit_expr(when_expr)?;
                    scope.visit_expr(then_expr)?;
                }
                if let Some(expr) = else_expr {
                    scope.visit_expr(expr)?;
                }
                Ok(ExprKind::Complex)
            }
            Expr::Cast { expr, .. } => {
                match self.maybe_suppress_eq_constraints(true).visit_expr(expr)? {
                    c @ ExprKind::Constant(_) => Ok(c),
                    // Assume casting a column is too complex for eq constrains
                    _ => Ok(ExprKind::Complex),
                }
            }
            Expr::Collate(expr, _) => {
                match self.maybe_suppress_eq_constraints(true).visit_expr(expr)? {
                    c @ ExprKind::Constant(_) => Ok(c),
                    // Assume collating a column is too complex for eq constrains
                    _ => Ok(ExprKind::Complex),
                }
            }
            Expr::Exists(select) => {
                self.visit_subquery(select)?;
                // TODO: propagate eq constraints
                Ok(ExprKind::Complex)
            }
            // TODO: for aggregate functions you can specify what get's plugged into the function
            // for ex. count(distinct X)
            //         count(X) FILTER (WHERE Y)
            //         count(X ORDER BY Z)
            //         count(X) OVER (PARTITION BY Y ORDER BY Z)
            //         count(X) OVER (PARTITION BY Y ORDER BY Z ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            // For now we don't error on these but whether they will result in correct behavior will depend on the
            // actual query and whether it has a group by clause
            expr @ Expr::FunctionCall { name, args, .. } => {
                let name = name.0.to_lowercase();
                if name == "unlikely" || name == "likely" {
                    if let Some(args) = args {
                        if args.len() == 1 {
                            return self.visit_expr(&args[0]);
                        }
                    }
                    Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
                } else if name == "likelihood" {
                    if let Some(args) = args {
                        if args.len() == 2 {
                            let prob = self
                                .maybe_suppress_eq_constraints(true)
                                .visit_expr(&args[1])?;
                            if let ExprKind::Constant(_) = prob {
                                return self.visit_expr(&args[0]);
                            }
                        }
                    }
                    Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
                } else {
                    let mut scope = self.maybe_suppress_eq_constraints(true);
                    if let Some(args) = args {
                        for expr in args.iter() {
                            scope.visit_expr(expr)?;
                        }
                    }
                    Ok(ExprKind::Complex)
                }
            }
            Expr::InList { lhs, rhs, .. } => {
                let mut scope = self.maybe_suppress_eq_constraints(true);
                scope.visit_expr(lhs)?;
                if let Some(rhs) = rhs {
                    for expr in rhs.iter() {
                        scope.visit_expr(expr)?;
                    }
                }
                Ok(ExprKind::Complex)
            }
            Expr::InSelect { lhs, rhs, .. } => {
                let mut scope = self.maybe_suppress_eq_constraints(true);
                scope.visit_expr(lhs)?;
                scope.visit_subquery(rhs)?;
                Ok(ExprKind::Complex)
            }
            expr @ Expr::InTable { .. } => {
                return Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
            }
            Expr::IsNull(expr) => {
                self.maybe_suppress_eq_constraints(true).visit_expr(expr)?;
                // PK's can never be null, therefore nullability does not give us any information
                Ok(ExprKind::Complex)
            }
            Expr::Like { lhs, rhs, .. } => {
                let mut scope = self.maybe_suppress_eq_constraints(true);
                scope.visit_expr(lhs)?;
                scope.visit_expr(rhs)?;
                Ok(ExprKind::Complex)
            }
            Expr::NotNull(expr) => {
                self.maybe_suppress_eq_constraints(true).visit_expr(expr)?;
                // PK's can never be null, therefore nullability does not give us any information
                Ok(ExprKind::Complex)
            }
            Expr::Parenthesized(parens) => {
                // TODO: Return a list of ExprKind here so stuff like this can work:
                // SELECT * FROM foo WHERE (foo.id, foo.parent) = (1, 3);
                // SELECT * FROM foo JOIN bar WHERE (foo.id, foo.parent) = (bar.id, bar.parent);

                // Some edge cases to think about:
                // false = (foo.id = bar.id);
                // (foo.id = bar.id)
                // true = (foo.id = bar.id)
                // (foo.id = bar.id) > 0
                // (foo.id = bar.id) = false
                // (true, true) = (foo.id = bar.id, baz.id = bar.id)
                // For now assume if there are more than 1 element in the paranthesis then ignore eq constraints
                let mut scope = self.maybe_suppress_eq_constraints(parens.len() > 1);
                for expr in parens.iter() {
                    let r = scope.visit_expr(expr)?;
                    if parens.len() == 1 {
                        // Parenthesised column or constant is ok
                        return Ok(r);
                    }
                }
                Ok(ExprKind::Complex)
            }
            Expr::Subquery(select) => {
                let child = self.visit_subquery(select)?;
                // If it returns a single column and does not reference any tables, it might be a constant
                if child.tables.len() == 0 && child.result_columns.len() == 1 {
                    let ret = child.result_columns.first().unwrap();
                    // The funny thing is that such subquery can select a parent column :D
                    // For example: SELECT * FROM foo WHERE 2 = (SELECT id)
                    // Is equivalent to: SELECT * FROM foo WHERE 2 = foo.id
                    return Ok(ret.kind.clone());
                }
                Ok(ExprKind::Complex)
            }
            Expr::Unary(op, expr) => {
                let suppress = match op {
                    // In ON/where clauses sqlite treats stuff as true if it evaluates to smth != 0
                    // This means -1 is true
                    // SELECT * FROM foo WHERE +(foo.id = 2) -- OK
                    // SELECT * FROM foo WHERE -(foo.id = 2) -- OK, because all true values are still true, all false values are still false
                    // SELECT * FROM foo WHERE ~(foo.id = 2) -- NOPE
                    // SELECT * FROM foo WHERE NOT (foo.id = 2) -- NOPE
                    UnaryOperator::Positive | UnaryOperator::Negative => false,
                    UnaryOperator::BitwiseNot | UnaryOperator::Not => true,
                };
                match (
                    op,
                    self.maybe_suppress_eq_constraints(suppress)
                        .visit_expr(expr)?,
                ) {
                    // The unary operator + is a no-op. It can be applied to strings, numbers, blobs or NULL and it always returns a result with the same value as the operand
                    (UnaryOperator::Positive, e) => Ok(e),
                    // ~ and - on a constant is still a constant but a different one
                    (
                        UnaryOperator::Negative | UnaryOperator::BitwiseNot,
                        ExprKind::Constant(_),
                    ) => Ok(ExprKind::Constant("".into())),
                    _ => Ok(ExprKind::Complex),
                }
            }

            Expr::Literal(lit) => match lit {
                Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_) => {
                    Ok(ExprKind::Constant("".into()))
                }
                Literal::Keyword(keyword) => {
                    let l = keyword.to_string().to_lowercase();
                    if l == "false" || l == "true" {
                        Ok(ExprKind::Constant("".into()))
                    } else {
                        Ok(ExprKind::Complex)
                    }
                }
                // thing = NULL doesn't make sense as it's always false
                Literal::Null => Ok(ExprKind::Complex),
                Literal::CurrentDate => Ok(ExprKind::Complex),
                Literal::CurrentTime => Ok(ExprKind::Complex),
                Literal::CurrentTimestamp => Ok(ExprKind::Complex),
            },
            Expr::Variable(variable_name) => Ok(ExprKind::Constant(variable_name.clone())),

            // Raise is only valid inside triggers
            expr @ Expr::Raise { .. } => {
                return Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
            }

            // TODO: count(*) etc...
            //Expr::FunctionCallStar { name, filter_over } => todo!(),
            _ => Ok(ExprKind::Complex),
        }
    }

    fn visit_return_columns(&mut self, columns: &[ResultColumn]) -> Result<(), SqlAnalysisError> {
        // SELECT foo.id = bar.foo_id FROM foo, bar; -- doesn't constrain anything
        let prev_suppress_eq_constraints = self.suppress_eq_constraints;
        self.suppress_eq_constraints = true;
        // Returned columns without an explicit alias are given the name __col_{depth}_{col_idx}
        let depth = self.depth;
        let mut col_idx = 0;
        for col in columns.iter() {
            match col {
                ResultColumn::Expr(expr, alias) => {
                    // Respect the original alias if it exists
                    // Cause sqlite allows select aliases to be referenced in various places
                    // And renaming them might break the query
                    let alias = if let Some(As::As(alias)) = alias {
                        alias.clone()
                    } else if let Some(As::Elided(alias)) = alias {
                        alias.clone()
                    } else {
                        Name(format!("__corro_col_{depth}_{col_idx}"))
                    };
                    // println!("extracting col: {expr:?} (as: {maybe_as:?})");
                    let (kind, sources) = self.visit_expr_with_sources(expr)?;
                    self.parsed.result_columns.push(ReturnExpr {
                        sources,
                        kind,
                        expr: expr.clone(),
                        alias,
                    });
                    col_idx += 1;
                }
                ResultColumn::Star => {
                    // A star is a star - it will select everything which is in the current scope including joins
                    for (table_def, referenced_columns) in &mut self.parsed.tables {
                        let table_schema = self.schema.tables.get(&table_def.real_table).ok_or(
                            SqlAnalysisError::TableStarNotFound {
                                tbl_name: table_def.real_table.clone(),
                            },
                        )?;
                        // Now with the table schema resolved we need to add all columns from this table
                        for col in table_schema.columns.keys() {
                            let resolved_col = ExprColumnRef {
                                table: table_def.clone(),
                                table_scope_depth: self.depth,
                                column_name: col.clone(),
                            };
                            // Unless they are omitted
                            if self.parsed.column_omissions.contains(&resolved_col) {
                                continue;
                            }
                            referenced_columns.insert(col.clone());
                            self.parsed.result_columns.push(ReturnExpr {
                                sources: HashSet::from([resolved_col.clone()]),
                                kind: ExprKind::Column(resolved_col),
                                expr: Expr::Qualified(
                                    Name(table_def.alias.clone()),
                                    Name(col.clone()),
                                ),
                                alias: Name(format!("__corro_col_{depth}_{col_idx}")),
                            });
                            col_idx += 1;
                        }
                    }
                }
                ResultColumn::TableStar(tbl_name) => {
                    let (table_def, table_schema, referenced_columns) = self
                        .parsed
                        .table_by_alias
                        .get(tbl_name.0.as_str())
                        .and_then(|table_def| {
                            Some((
                                table_def,
                                self.schema.tables.get(&table_def.real_table)?,
                                self.parsed.tables.get_mut(table_def)?,
                            ))
                        })
                        .ok_or(SqlAnalysisError::TableStarNotFound {
                            tbl_name: tbl_name.0.clone(),
                        })?;
                    // Now with the table schema resolved we need to add all columns from this table
                    for col in table_schema.columns.keys() {
                        let resolved_col = ExprColumnRef {
                            table: table_def.clone(),
                            table_scope_depth: self.depth,
                            column_name: col.clone(),
                        };
                        referenced_columns.insert(col.clone());
                        self.parsed.result_columns.push(ReturnExpr {
                            sources: HashSet::from([resolved_col.clone()]),
                            kind: ExprKind::Column(resolved_col),
                            expr: Expr::Qualified(Name(table_def.alias.clone()), Name(col.clone())),
                            alias: Name(format!("__corro_col_{depth}_{col_idx}")),
                        });
                        col_idx += 1;
                    }
                }
            }
        }
        self.suppress_eq_constraints = prev_suppress_eq_constraints;
        Ok(())
    }
}

// Statements used for incomming table changes
// Before any reconciliation is done were inserting changed pks into such table
#[derive(Debug, Clone)]
pub struct RealTableMatcherStmt {
    // What are the names of the primary keys of the table
    pub table_pk: Vec<String>,
    // On which columns we are matching
    pub subscribed_cols: Vec<String>,
    // How is the table named in the main database
    pub main_table_name: String,
    // How is the table named in the subscription database
    pub sub_table_name: String,
    // Statement used to create the table in the subscription database
    pub create_table_stmt: String,
    // Statement used to cleanup the table in the subscription database
    pub clean_table_stmt: String,
    // Statement used to insert the primary keys of the table in the subscription database
    pub insert_pks_stmt: String,
}

// For each referenced table we get a 2 way mapping from the sub query to the real table
#[derive(Debug, Clone)]
pub struct QueryTableMatcherStmt {
    pub table_def: TableDef,
    // In order for a subscription to be incrementally maintainable
    // each referenced table in the main schema must have a mapping from
    // it's source table PK to a subset of the row_pk we use to match
    // Otherwise subscriptions are not faster than running a full query from time to time
    pub query_pk: Vec<String>,
    // pub pk_remapper: Option<PkMapper> // TODO :D
    pub new_query: String,
    pub temp_query: String,
}

impl QueryTableMatcherStmt {
    pub fn new_query(&self) -> &String {
        &self.new_query
    }
}

#[derive(Debug, Clone)]
pub struct PreparedSubscriptionSql {
    // The query which will get executed to get the initial subscription rows
    pub(crate) initial_query: Stmt,
    // The parsed query
    pub(crate) parsed: ParsedSelect,
    // The PK of a result row
    pub(crate) pk: Vec<PkEntry>,
    pub(crate) recomended_indexes: Vec<Vec<PkEntry>>,
    // The result set of the query
    pub(crate) result_set: Vec<ResultColumn>,
    // The result set of the query with the PKs added
    // Keep in mind that len(pk) + len(result_set) != len(result_set_with_pk)
    // As the result_set might have already requested the PKs we needed to add
    // instead of adding them again we use existing columns to reduce space usage
    // If the reused column was aliased, the original alias will be respected
    pub(crate) result_set_with_pk: Vec<ResultColumn>,
    // real_name -> real_table
    pub(crate) real_tables: IndexMap<String, RealTableMatcherStmt>,
    // alias -> query_table
    pub(crate) query_tables: IndexMap<String, QueryTableMatcherStmt>,
}

pub fn prepare_subscription_sql(
    sub_id: Uuid,
    sql: &str,
    schema: &Schema,
) -> Result<PreparedSubscriptionSql, SqlAnalysisError> {
    let sql_hash = hex::encode(seahash::hash(sql.as_bytes()).to_be_bytes());
    let mut parser = Parser::new(sql.as_bytes());

    let (mut stmt, parsed) = match parser.next()?.ok_or(SqlAnalysisError::StatementRequired)? {
        Cmd::Stmt(stmt) => {
            let parsed = match stmt {
                Stmt::Select(ref select) => {
                    ParsingScope::root_scope(schema).visit_select(select)?
                }
                _ => return Err(SqlAnalysisError::UnsupportedStatement),
            };

            (stmt, parsed)
        }
        _ => return Err(SqlAnalysisError::StatementRequired),
    };

    if parsed.tables.is_empty() {
        return Err(SqlAnalysisError::TableRequired);
    }

    // Now it's time to establish the PK
    let pk = derive_pk(&parsed, schema)?;
    // Tweak the statement a bit so it returns results along with the PK
    let (pk, result_set, result_set_with_pk) = establish_result_set(&parsed, pk);
    match &mut stmt {
        Stmt::Select(select) => match &mut select.body.select {
            OneSelect::Select { columns, .. } => {
                *columns = result_set_with_pk
                    .iter()
                    .map(|x| x.to_result_column())
                    .collect();
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    // Now after we've parsed the query we can generate the main table queries
    let mut real_tables = IndexMap::default();
    let mut query_tables = IndexMap::default();
    for (table_def, cols) in parsed.tables {
        let real_table = real_tables.entry(table_def.real_table).or_insert_with(|| {
            let schema_table = schema.tables.get(&table_def.real_table).unwrap();
            let sub_table_name = format!("main_pks_{}", table_def.real_table);
            let pk_sql = schema_table
                .pk
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            // TODO: keep track of nullable constraints
            // coalesce(value0, ""), coalesce(value1, ""), ..., coalesce(valueN, "")
            let values_sql = schema_table
                .pk
                .iter()
                .enumerate()
                .map(|(i, _)| format!("coalesce(value{}, \"\")", i))
                .collect::<Vec<_>>()
                .join(", ");
            // ?, ?, ..., ?
            let unnest_args_sql = schema_table
                .pk
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            RealTableMatcherStmt {
                table_pk: schema_table.pk.iter().cloned().collect(),
                subscribed_cols: Vec::new(),
                main_table_name: table_def.real_table.clone(),
                sub_table_name: sub_table_name.clone(),
                create_table_stmt: format!("CREATE TEMP TABLE {} ({})", sub_table_name, pk_sql),
                // https://sqlite.org/lang_delete.html
                // When the WHERE clause and RETURNING clause are both omitted from a DELETE statement and the table being deleted has no triggers, SQLite uses an optimization to erase the entire table content without having to visit each row of the table individually.
                // This "truncate" optimization makes the delete run much faster
                clean_table_stmt: format!("DELETE FROM {}", sub_table_name),
                insert_pks_stmt: format!(
                    "INSERT INTO {} ({}) SELECT {} FROM unnest({})",
                    sub_table_name, pk_sql, values_sql, unnest_args_sql
                ),
            }
        });
        real_table.subscribed_cols.extend(cols.iter().cloned());
        query_tables.insert(
            table_def.alias.clone(),
            QueryTableMatcherStmt {
                table_def: table_def.clone(),
                // For now assume the PK of the query table is the same as the PK of the real table
                // For group by queries this is not true
                query_pk: real_table.table_pk.clone(),
                new_query: String::new(),
                temp_query: String::new(),
            },
        );
    }

    let mut statements = HashMap::new();
    let mut pks = IndexMap::default();

    // Appends the PKs to the result columns

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

    Ok(PreparedSubscriptionSql {
        initial_query: stmt,
        row_pk: pks,
        minimal_pk: pks,
        parsed,
        query_tables,
        real_tables,
    })
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

// Establish the primary key of a result row of the parsed query
fn derive_pk(parsed: &ParsedSelect, schema: &Schema) -> Result<Vec<PkEntry>, SqlAnalysisError> {
    // This is not exactly a check that this is top level but good enough
    assert!(parsed.parent_column_references.is_empty());
    let pk = if parsed.is_distinct {
        if let Some(group_by) = &parsed.group_by {
            // The query executes in this order: filters -> group -> distinct
            // To incrementally maintain SELECT DISTINCT ... GROUP BY ... we would need to recalculate affected groups
            // Imagine a query like this SELECT DISTINCT count(*) AS c FROM foo GROUP BY foo.region
            // We need to recalculate by foo.region but the PK is c - this would require us to rerun the entire query
            // instead of just incrementally maintaining the groups
            // So the only feasible way to support this would be to only accept such queries if the group PK is contained in the result columns
            // But in such cases by definition this FD holds group_pk => result_columns so the distinctness is an no-op :P
            // The only case besides this is when part of the group key is in the result column
            // SELECT DISTINCT foo.continent, count(*) AS c FROM foo GROUP BY foo.region, foo.continent
            // In such cases we can update incrementally the query using foo.continent like this:
            // SELECT DISTINCT foo.continent, count(*) AS c FROM foo WHERE foo.continent in (...) GROUP BY foo.region, foo.continent
            let group_by_ast = group_by
                .iter()
                .map(|x| (x.expr.to_string(), x))
                .collect::<HashMap<String, &ReturnExpr>>();
            let group_by_col_ref = group_by
                .iter()
                .filter_map(|x| {
                    if let ExprKind::Column(col_ref) = &x.kind {
                        Some((col_ref, x))
                    } else {
                        None
                    }
                })
                .collect::<HashMap<&ExprColumnRef, &ReturnExpr>>();
            let r = parsed
                .result_columns
                .iter()
                .map(|x| {
                    // Look if this expr is in the group by list by comparing the AST's and checking if they both
                    // resolve to the same column reference
                    let in_group_by = group_by_ast.get(&x.expr.to_string()).or_else(|| {
                        if let ExprKind::Column(col_ref) = &x.kind {
                            group_by_col_ref.get(col_ref)
                        } else {
                            None
                        }
                    });
                    let renamable = x.is_internal_alias();
                    PkEntry {
                        expr: ReturnExpr {
                            sources: x.sources.clone(),
                            kind: x.kind.clone(),
                            expr: x.expr.clone(),
                            // If the returned expr can get renamed just use the group by alias as
                            // __corro_gk_... is more descriptive than __corro_col_...
                            // if an explicit name was placed then just use that explicit name
                            alias: if renamable && in_group_by.is_some() {
                                in_group_by.unwrap().alias.clone()
                            } else {
                                x.alias.clone()
                            },
                        },
                        fully_dependent: false,
                        // SELECT DISTINCT ... GROUP BY ... - only group by expressions are usable for maintenance
                        usable_for_maintenance: in_group_by.is_some(),
                    }
                })
                .collect::<Vec<PkEntry>>();

            // TODO: Select aliases and number literal in GROUP BY
            if !r.iter().any(|x| x.usable_for_maintenance) {
                return Err(SqlAnalysisError::QueryNotIncrementallyMaintainable {
                    reason: "SELECT DISTINCT ... GROUP BY ... requires at least one element from the GROUP BY clause to be in the result columns".to_string(),
                });
            }
            r
        } else {
            parsed
                .result_columns
                .iter()
                .map(|x| PkEntry {
                    expr: x.clone(),
                    fully_dependent: false,
                    // SELECT DISTINCT ... - all PKs are usable for maintenance
                    usable_for_maintenance: true,
                })
                .collect::<Vec<PkEntry>>()
        }
    } else if let Some(group_by) = &parsed.group_by {
        // Just use the group by expressions as PKs
        group_by
            .iter()
            .map(|x| PkEntry {
                expr: x.clone(),
                fully_dependent: false,
                // SELECT ... GROUP BY ... - all group keys are usable for maintenance
                usable_for_maintenance: true,
            })
            .collect::<Vec<PkEntry>>()
    } else {
        // If we don't have distinct or group by just use the union of referenced table's PKS as PKs
        parsed
            .tables
            .iter()
            .flat_map(|(table_def, _cols)| {
                let table_schema = schema.tables.get(&table_def.real_table).unwrap();
                table_schema.pk.iter().map(|pk| {
                    let resolved_column = ExprColumnRef {
                        table: table_def.clone(),
                        column_name: pk.clone(),
                        // FIXME: This will break if we materialize subqueries
                        table_scope_depth: 0,
                    };
                    let table_alias = &table_def.alias;
                    let return_expr = ReturnExpr {
                        sources: HashSet::from([resolved_column.clone()]),
                        expr: Expr::Qualified(Name(table_alias.clone()), Name(pk.clone())),
                        kind: ExprKind::Column(resolved_column),
                        alias: Name(format!("__corro_pk_{table_alias}_{pk}")),
                    };
                    PkEntry {
                        expr: return_expr,
                        fully_dependent: false,
                        // SELECT ... - all PKs are usable for maintenance
                        usable_for_maintenance: true,
                    }
                })
            })
            .collect::<Vec<PkEntry>>()
    };
    // TODO: Use functional dependencies and eq constraints to calculate minimal PKs
    Ok(pk)
}

// Helper struct to get the most specific alias for a return expression
// And to determine if an PK was already in the result set
// Result Expressions are compared by their AST and Column Reference
// user_alias > __corro_pk/__corro_gk > __corro_col
struct ReturnExprAliasHelper<'a> {
    by_ast: HashMap<String, &'a ReturnExpr>,
    by_col_ref: HashMap<&'a ExprColumnRef, &'a ReturnExpr>,
}

impl<'a> ReturnExprAliasHelper<'a> {
    fn new<I, T>(return_exprs: I) -> Self
    where
        I: IntoIterator<Item = T> + Copy,
        T: Borrow<&'a ReturnExpr>,
    {
        let by_ast = return_exprs
            .into_iter()
            .map(|x| {
                let r: &'a ReturnExpr = *x.borrow();
                (r.expr.to_string(), r)
            })
            .collect::<HashMap<String, &'a ReturnExpr>>();
        let by_col_ref = return_exprs
            .into_iter()
            .filter_map(|v| {
                let r: &'a ReturnExpr = *v.borrow();
                if let ExprKind::Column(col_ref) = &r.kind {
                    Some((col_ref, r))
                } else {
                    None
                }
            })
            .collect::<HashMap<&ExprColumnRef, &ReturnExpr>>();
        Self { by_ast, by_col_ref }
    }

    fn find(&self, expr: &ReturnExpr) -> Option<&&ReturnExpr> {
        // Direct AST match
        self.by_ast.get(&expr.expr.to_string()).or_else(|| {
            // AST differs but resolves to the same column reference
            if let ExprKind::Column(col_ref) = &expr.kind {
                self.by_col_ref.get(col_ref)
            } else {
                None
            }
        })
    }
}

// Given a list of PK's includes them in the result set
// Prepends the PKs which were not already in the result set
// parsed.result_columns.len + pk.len() != new_result_columns.len()
// pk.len() == new_pk.len()
// The returned PKs are the same as the input PKs but might be differently named
fn establish_result_set(
    parsed: &ParsedSelect,
    pk: Vec<PkEntry>,
) -> (Vec<PkEntry>, Vec<ReturnExpr>, Vec<ReturnExpr>) {
    let group_exprs = ReturnExprAliasHelper::new(parsed.group_by.as_ref().unwrap_or(&vec![]));
    let return_exprs = ReturnExprAliasHelper::new(&parsed.result_columns);
    let pk_exprs = ReturnExprAliasHelper::new(&pk.iter().map(|x| &x.expr).collect::<Vec<&ReturnExpr>>());
    let new_pks = Vec::with_capacity(pk.len());
    let new_result_set_with_pk = Vec::with_capacity(parsed.result_columns.len() + pk.len());

    (new_pks, new_result_set, new_result_set_with_pk)
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
    #[error("UNION/INTERSECT/EXCEPT of multiple SELECTs are not allowed in matcher expressions")]
    CompoundNotAllowed,
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
    #[error("unsupported group by expression: {expr:?}")]
    UnsupportedGroupByExpression { expr: Expr },
    #[error("query is not incrementally maintainable: {reason}")]
    QueryNotIncrementallyMaintainable { reason: String },
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
            prepare_subscription_sql(Uuid::new_v4(), "SELECT * FROM tree JOIN tree", &schema).unwrap_err(),
            SqlAnalysisError::AmbigousTableReference {
                real_table: ref rt,
                alias: ref a
            } if rt == "tree" && a == "tree",
        ));
        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT * FROM tree, tree", &schema).unwrap_err(),
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

        let analysis = prepare_subscription_sql(
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
        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.foo_id WHERE EXISTS (SELECT 1 FROM baz WHERE bar_id = bar.id) AND UNLIKELY(foo.name = 'test')",
            &schema,
        )
        .unwrap();
        println!("Stmt: {:#?}", Cmd::Stmt(analysis.stmt).to_string());
        println!("Statements: {:#?}", analysis.statements);
        println!("Pks: {:#?}", analysis.pks);
        println!("Parsed: {:#?}", analysis.parsed);
    }
}
