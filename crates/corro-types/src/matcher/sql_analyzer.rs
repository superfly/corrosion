use bit_set::BitSet;
use fallible_iterator::FallibleIterator;
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    ops::{Deref, DerefMut},
};

use enquote::unquote;
use indexmap::{IndexMap, IndexSet};

use disjoint_hash_set::DisjointHashSet;
use sqlite3_parser::{ast::*, lexer::sql::Parser};
use tracing::{info, warn};
use uuid::Uuid;

use crate::schema::Schema;

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

    pub fn as_column(&self) -> Option<&ExprColumnRef> {
        match self.kind {
            ExprKind::Column(ref col) => Some(col),
            _ => None,
        }
    }

    pub fn as_complex_sources(&self) -> Option<&HashSet<ExprColumnRef>> {
        match self.kind {
            ExprKind::Complex => Some(&self.sources),
            _ => None,
        }
    }
}

impl AsRef<ReturnExpr> for ReturnExpr {
    fn as_ref(&self) -> &ReturnExpr {
        self
    }
}

// Container for collecting various constraints on expressions
#[derive(Debug, Clone, Default)]
pub struct ExprConstrains {
    // ConstantId. ExprColumnRef
    pub constant_constrains: Vec<(String, ExprColumnRef)>,
    // Equalities which must hold gathered from WHERE/ON constraints like col1 = col2
    pub eq_constrains: Vec<(ExprColumnRef, ExprColumnRef)>,
}

#[derive(Debug, Clone)]
pub struct TableDetails {
    // What table is this about
    pub table: TableDef,
    // What columns does this table reference
    pub columns: HashSet<String>,
    // If the join condition holds then which constraints hold
    pub constraints: ExprConstrains,
    // If the join condition fails does this influence the nullability of other tables?
    // Joins can make tables nullable only when the join condition fails
    // if !(self.lhs_nullable || self.rhs_nullable) then it's an inner join
    pub lhs_nullable: bool,
    pub rhs_nullable: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedSelect {
    // All used columns from defined tables are listed here
    pub tables: IndexMap<TableDef, TableDetails>,
    // Aliasses are unique per scope
    pub table_by_alias: HashMap<String, TableDef>,
    // Multiple real tables can be used at the same time under different aliasses
    pub table_by_real_name: HashMap<String, Vec<TableDef>>,
    // Column references from parent scopes
    pub parent_column_references: Vec<ExprColumnRef>,
    // Later we will optimize the PK using bitsets, to do that we need to have a mapping from
    // Columns/attributes we care about to bitset positions, to simplify we're just storing
    // all table columns in an IndexSet - the index of a column being the index in bitsets
    pub columns_in_current_scope: IndexSet<ExprColumnRef>,
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
    // Where constrains always apply regardless of join nullability
    // Where constrains actually can infer a non nullable constraint
    pub where_constrains: Option<ExprConstrains>,
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
    // This is used to track which constraints is the expr generating
    expr_constrains: Option<ExprConstrains>,
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
            expr_constrains: None,
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
            expr_constrains: None,
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
            expr_constrains: None,
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
            expr_constrains: None,
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
        lhs_nullable: bool,
        rhs_nullable: bool,
    ) -> Result<TableDef, SqlAnalysisError> {
        let real_table_name = &table_name.name;
        // Check if such table exists in the schema
        let table_schema = self.schema.tables.get(&real_table_name.0);
        if table_schema.is_none() {
            return Err(SqlAnalysisError::TableNotFound(real_table_name.0.clone()));
        }
        let table_schema = table_schema.unwrap();
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
        self.parsed.tables.insert(
            table.clone(),
            TableDetails {
                table: table.clone(),
                columns: HashSet::new(),
                constraints: Default::default(),
                lhs_nullable,
                rhs_nullable,
            },
        );
        self.parsed
            .table_by_alias
            .insert(resolved_alias.clone(), table.clone());
        self.parsed
            .table_by_real_name
            .entry(real_table_name.0.clone())
            .or_default()
            .push(table.clone());

        for (col_name, _col_details) in &table_schema.columns {
            self.parsed.columns_in_current_scope.insert(ExprColumnRef {
                table: table.clone(),
                table_scope_depth: self.depth,
                column_name: col_name.clone(),
            });
        }

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
            for col_ref in &self.parsed.columns_in_current_scope {
                if col_ref.column_name == check_col_name && col_ref.table_scope_depth == self.depth
                {
                    // Using clause columns are not returned by * and are not resolvable without a namespace
                    if self.parsed.column_omissions.contains(col_ref) {
                        continue;
                    }
                    if found.is_some() {
                        return Err(SqlAnalysisError::QualificationRequired {
                            col_name: check_col_name,
                        });
                    }
                    found = Some(col_ref.clone());
                }
            }
            // If we found an unique match, let's return it
            if let Some(col_ref) = found {
                return Ok(col_ref);
            }
            // TODO: If not found then check return expressions
            //       Is this the right place or is visit_expr the right place?
        }

        // If not found look for the table in parent scopes
        if let Some(parent_scope) = self.parent_scope {
            return parent_scope.resolve_column_reference(namespace, column_name);
        }

        Err(SqlAnalysisError::TableForColumnNotFound {
            col_name: column_name.to_string(),
        })
    }

    fn register_column_reference(&mut self, column: &ExprColumnRef) {
        if column.table_scope_depth == self.depth {
            insert_col(
                &mut self.parsed.tables.get_mut(&column.table).unwrap().columns,
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
            // So bitsets work for parent references as well
            self.parsed.columns_in_current_scope.insert(column.clone());
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
        if let Some(constrains) = &mut self.expr_constrains {
            constrains.eq_constrains.push((left.clone(), right.clone()));
        }
    }

    fn register_constant_constraint(&mut self, constant_id: String, column: &ExprColumnRef) {
        if self.suppress_eq_constraints {
            return;
        }
        if let Some(constrains) = &mut self.expr_constrains {
            // Unnamed constants mean "this value is unique"
            let cid = if constant_id.is_empty() {
                format!("constant_{}", rand::random::<u64>())
            } else {
                constant_id
            };

            constrains.constant_constrains.push((cid, column.clone()));
        }
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
            if let Some(from) = from {
                match &from.select {
                    Some(table) => match table.as_ref() {
                        SelectTable::Table(name, alias, _) => {
                            // The from table is not nullable unless joins intervene
                            // No constraints are registered for this table
                            self.add_table_to_scope(name, alias, false, false)?;
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
                        //let has_condition = join.constraint.is_some();
                        let (is_natural, lhs_nullable, rhs_nullable) = match join.operator {
                            // Comma joins are just inner joins
                            JoinOperator::Comma => (false, false, false),
                            JoinOperator::TypedJoin { natural, join_type } => {
                                let (lhs_nullable, rhs_nullable) = match join_type {
                                    None => (false, false), // Natural joins are just inner joins with implicit USING
                                    // Cross joins are just inner joins with a query planner hint to not reorder the join order
                                    Some(JoinType::Cross | JoinType::Inner) => (false, false),
                                    Some(JoinType::Left | JoinType::LeftOuter) => (false, true),
                                    Some(JoinType::Right | JoinType::RightOuter) => (true, false),
                                    Some(JoinType::Full | JoinType::FullOuter) => (true, true),
                                };
                                // -- a is null and b is null
                                // SELECT * FROM (SELECT 1 AS id,2,3) AS a
                                // JOIN (SELECT 1 AS id,2,3) AS b ON a.id!=b.id
                                // RIGHT JOIN (SELECT 4,5,6);
                                // Will return rows besides there being no failed condition on the right join
                                // Adding ON 1; still emits rows cause the lhs is still present (but null)
                                // Therefore don't change join types prematurely
                                (
                                    natural,
                                    lhs_nullable, /*&& (has_condition || natural)*/
                                    rhs_nullable, /*&& (has_condition || natural)*/
                                )
                            }
                        };
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
                            // Natural joins can't have an USING or ON clause
                            if is_natural {
                                return Err(SqlAnalysisError::NaturalJoinWithUsingOnClause);
                            }
                            match constraint {
                                JoinConstraint::On(expr) => {
                                    // Simple on clause - add table to scope and extract columns
                                    let tbl_def = self.add_table_to_scope(
                                        tbl_name,
                                        tbl_alias,
                                        lhs_nullable,
                                        rhs_nullable,
                                    )?;
                                    let (_kind, constraints) =
                                        self.visit_expr_with_constrains(expr)?;
                                    self.parsed.tables.get_mut(&tbl_def).unwrap().constraints =
                                        constraints;
                                }
                                // For each pair of columns identified by a USING clause, the column from the right-hand dataset is omitted from the joined dataset.
                                // This is the only difference between a USING clause and its equivalent ON constraint.
                                JoinConstraint::Using(names) => {
                                    let mut isolated_scope = self.isolated_scope();
                                    let tbl_def = isolated_scope
                                        .add_table_to_scope(tbl_name, tbl_alias, false, false)?;
                                    let mut constraints = ExprConstrains::default();
                                    for col_name in names.iter() {
                                        // USING is very special - the name must exist on both sides of the join directly in this scope
                                        let left = self
                                            .without_parent()
                                            .resolve_column_reference(None, &col_name.0)?;
                                        let right = isolated_scope.resolve_column_reference(
                                            Some(&tbl_def.alias),
                                            &col_name.0,
                                        )?;
                                        constraints.eq_constrains.push((left, right));
                                    }
                                    // Great! All columns are resolvable on both sides of the join
                                    let tbl_def = self.add_table_to_scope(
                                        tbl_name,
                                        tbl_alias,
                                        lhs_nullable,
                                        rhs_nullable,
                                    )?;
                                    for (left, right) in &constraints.eq_constrains {
                                        self.register_column_reference(left);
                                        self.register_column_reference(right);
                                        self.parsed.column_omissions.insert(right.clone());
                                    }
                                    self.parsed.tables.get_mut(&tbl_def).unwrap().constraints =
                                        constraints;
                                }
                            }
                        } else {
                            if is_natural {
                                todo!("scan for common columns between lhs and rhs")
                            }
                            // No constrains - just add the table to the scope
                            self.add_table_to_scope(
                                tbl_name,
                                tbl_alias,
                                lhs_nullable,
                                rhs_nullable,
                            )?;
                        }
                    }
                }

                if let Some(expr) = where_clause {
                    let (_kind, constrains) = self.visit_expr_with_constrains(expr)?;
                    self.parsed.where_constrains = Some(constrains);
                }
            }

            // Determine which columns will be returned by this query
            self.visit_return_columns(columns.as_slice())?;

            // For SELECT DISTINCT col1, col2, ... colN
            // the PK will be (col1, col2, ... colN)
            self.parsed.is_distinct = matches!(distinctness, Some(Distinctness::Distinct));

            if let Some(group_by) = group_by {
                let mut group_by_res = Vec::with_capacity(group_by.exprs.len());
                for (i, expr) in group_by.exprs.iter().enumerate() {
                    // TODO: This can refer to aliases from select columns
                    //       Also a number literal refers to the Nth column
                    //       in the result set. For now we error in such cases.
                    let (kind, sources) = self
                        .maybe_suppress_eq_constraints(true)
                        .visit_expr_with_sources(expr)?;
                    // For now error on constant expressions and literals in GROUP BY
                    if sources.is_empty() {
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

    fn visit_expr_with_constrains(
        &mut self,
        expr: &Expr,
    ) -> Result<(ExprKind, ExprConstrains), SqlAnalysisError> {
        if self.expr_constrains.is_some() {
            unreachable!("Did visit_expr_with_constrains got called from inside visit_expr?")
        }
        self.expr_constrains = Some(Default::default());
        let kind = self.visit_expr(expr)?;
        let expr_constrains = self.expr_constrains.take().unwrap();
        Ok((kind, expr_constrains))
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
                Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
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
                if child.tables.is_empty() && child.result_columns.len() == 1 {
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
                Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
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
        let mut used_aliasses = HashSet::new();
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
                    // SQlite is very liberal and allows stuff like this:
                    // SELECT 1 as foo, 2 as foo WHERE foo = 1;
                    // Sqlite will bind the foo in the where clause to the first select column
                    // Let's be a bit strict and error on non unique aliases within the same scope
                    // This way we may use this alias safely for reffering to exactly this returned column
                    if used_aliasses.contains(&alias.0) {
                        return Err(SqlAnalysisError::AliasAmbigousInReturnList {
                            alias: alias.0.clone(),
                        });
                    }
                    used_aliasses.insert(alias.0.clone());
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
                    for col_ref in &self.parsed.columns_in_current_scope {
                        // Unless they are explicitly omitted
                        if self.parsed.column_omissions.contains(col_ref) {
                            continue;
                        }
                        self.parsed
                            .tables
                            .get_mut(&col_ref.table)
                            .unwrap()
                            .columns
                            .insert(col_ref.column_name.clone());
                        self.parsed.result_columns.push(ReturnExpr {
                            sources: HashSet::from([col_ref.clone()]),
                            kind: ExprKind::Column(col_ref.clone()),
                            expr: Expr::Qualified(
                                Name(col_ref.table.alias.clone()),
                                Name(col_ref.column_name.clone()),
                            ),
                            alias: Name(format!("__corro_col_{depth}_{col_idx}")),
                        });
                        col_idx += 1;
                    }
                }
                ResultColumn::TableStar(tbl_name) => {
                    let (table_def, table_schema, table_info) = self
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
                        table_info.columns.insert(col.clone());
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
    pub subscribed_cols: HashSet<String>,
    // How is the table named in the main database
    pub main_table_name: String,
    // How is the table named in the subscription database
    pub sub_table_name: String,
    // Statement used to create the table in the subscription database
    pub create_table_stmt: String,
    // Statement used to cleanup the table in the subscription database
    pub clean_table_stmt: String,
    // Statement used to insert the primary keys of the table in the subscription database
    pub unnest_insert_pks_stmt: String,
}

// For each referenced table we get a 2 way mapping from the sub query to the real table
#[derive(Debug, Clone)]
pub struct QueryTableMatcherStmt {
    pub table_def: TableDef,
    // How real PKs map to query PKs
    pub real_pks_to_row_pk: Vec<(String, Vec<ReturnExpr>)>,
    // This will be a table where query PKs are calculated
    // So when a row get's deleted we know what row Pks got affected
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
pub struct TableNullPredicate {
    #[allow(dead_code)]
    pub table: TableDef,
    pub pk_aliases: Vec<String>,
    pub is_null: bool,
}

#[derive(Debug, Clone)]
pub struct IndexFilter {
    pub predicates: Vec<TableNullPredicate>,
}

impl IndexFilter {
    pub fn to_sql(&self) -> String {
        self.predicates
            .iter()
            .map(|p| {
                let all_null = p
                    .pk_aliases
                    .iter()
                    .map(|a| format!("{a} IS NULL"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                if p.is_null {
                    format!("({all_null})")
                } else {
                    format!("NOT ({all_null})")
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub index_name: String,
    pub column_aliases: Vec<String>,
    pub coalesce: bool,
    pub filter: Option<IndexFilter>,
}

impl IndexDef {
    pub fn index_columns_sql(&self) -> String {
        if self.coalesce {
            self.column_aliases
                .iter()
                .map(|a| format!("coalesce({a}, \"\")"))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            self.column_aliases.join(",")
        }
    }

    pub fn filter_expr_sql(&self) -> Option<String> {
        self.filter.as_ref().map(|f| f.to_sql())
    }

    pub fn perhaps_where_clause(&self) -> String {
        self.filter_expr_sql()
            .map(|f| format!("WHERE {f}"))
            .unwrap_or_default()
    }

    pub fn on_conflict_clause(&self) -> String {
        format!(
            "ON CONFLICT({conflict_clause}) {where_clause}",
            conflict_clause = self.index_columns_sql(),
            where_clause = self.perhaps_where_clause()
        )
    }
}

#[derive(Debug, Clone)]
pub struct PreparedSubscriptionSql {
    // The query which will get executed to get the initial subscription rows
    pub(crate) initial_query: Stmt,
    // The parsed query
    pub(crate) parsed: ParsedSelect,
    // The PK of a result row
    #[allow(dead_code)]
    pub(crate) pk: Vec<ReturnExpr>,
    // The required non unique indexes for the query
    // index_name => (index_columns)
    pub(crate) required_query_indexes: Vec<IndexDef>,
    pub(crate) identity_uniq_indexes: Vec<IndexDef>,
    // The result set of the query
    pub(crate) result_set: Vec<ReturnExpr>,
    // The result set of the query with the PKs added
    // Keep in mind that len(pk) + len(result_set) != len(result_set_with_pk)
    // As the result_set might have already requested the PKs we needed to add
    // instead of adding them again we use existing columns to reduce space usage
    // If the reused column was aliased, the original alias will be respected
    pub(crate) result_set_with_pk: Vec<ReturnExpr>,
    // real_name -> real_table
    pub(crate) real_tables: IndexMap<String, RealTableMatcherStmt>,
    // alias -> query_table
    pub(crate) query_tables: IndexMap<String, QueryTableMatcherStmt>,
}

impl PreparedSubscriptionSql {
    pub fn return_column_names_with_pks(&self) -> String {
        self.result_set_with_pk
            .iter()
            .map(|x| x.alias.0.clone())
            .collect::<Vec<_>>()
            .join(",")
    }
    pub fn return_column_names_with_pks_and_type(&self) -> String {
        self.result_set_with_pk
            .iter()
            .map(|x| format!("{} ANY", x.alias.0.clone()))
            .collect::<Vec<_>>()
            .join(",")
    }
    pub fn return_column_names(&self) -> String {
        self.result_set
            .iter()
            .map(|x| x.alias.0.clone())
            .collect::<Vec<_>>()
            .join(",")
    }
    pub fn return_column_names_with_type(&self) -> String {
        self.result_set
            .iter()
            .map(|x| format!("{} ANY", x.alias.0.clone()))
            .collect::<Vec<_>>()
            .join(",")
    }
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

    // Tweak the statement a bit so it returns results along with the PKs
    // This may reuse existing result_set entries for PK's and adjust aliasses
    let (join_pk, result_set, result_set_with_pk) = {
        // Now it's time to establish the PK
        let join_pk = derive_join_pk(&parsed, schema);
        let _group_pk = derive_group_pk(&parsed)?;
        establish_result_set(&parsed, join_pk)
    };
    // Sanity check the pks
    if join_pk.is_empty() {
        return Err(SqlAnalysisError::QueryNotIncrementallyMaintainable {
            reason: "No row pk".to_string(),
        });
    }
    if join_pk
        .iter()
        .any(|x| matches!(&x.kind, ExprKind::Constant(_)))
    {
        return Err(SqlAnalysisError::QueryNotIncrementallyMaintainable {
            reason: "PK can't be a constant".to_string(),
        });
    }
    // TODO: Add group mapping tables to support this
    if join_pk.iter().any(|x| matches!(&x.kind, ExprKind::Complex)) {
        return Err(SqlAnalysisError::QueryNotIncrementallyMaintainable {
            reason: "Group by is not supported yet".to_string(),
        });
    }
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

    // Now after we've parsed the query we can generate queries
    let mut real_tables = IndexMap::default();
    let mut query_tables = IndexMap::default();
    let mut required_query_indexes = Vec::new();
    // For every aliassed source table
    for (idx, (table_def, table_info)) in parsed.tables.iter().enumerate() {
        // Get the real table
        let real_table = real_tables
            .entry(table_def.real_table.clone())
            .or_insert_with(|| {
                let schema_table = schema.tables.get(&table_def.real_table).unwrap();
                let sub_table_name = format!("main_pks_{}", table_def.real_table);
                let pk_sql = schema_table
                    .pk
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                // We need to convert NULLs into sentinel values because
                // all the diffing which we're doing is based on the IN operator
                // And `SELECT 1 WHERE (NULL, 1) IN ((NULL, 1));` will return 0 rows
                // coalesce(value0, ""), coalesce(value1, ""), ..., coalesce(valueN, "")
                let coalesced_values_sql: String = schema_table
                    .pk
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("coalesce(value{i}, \"\")"))
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
                    subscribed_cols: HashSet::new(),
                    main_table_name: table_def.real_table.clone(),
                    sub_table_name: sub_table_name.clone(),
                    create_table_stmt: format!(
                        "CREATE TEMP TABLE IF NOT EXISTS {sub_table_name} ({pk_sql})",
                    ),
                    // https://sqlite.org/lang_delete.html
                    // When the WHERE clause and RETURNING clause are both omitted from a DELETE statement and the table being deleted has no triggers, SQLite uses an optimization to erase the entire table content without having to visit each row of the table individually.
                    // This "truncate" optimization makes the delete run much faster
                    clean_table_stmt: format!("DELETE FROM {sub_table_name}"),
                    unnest_insert_pks_stmt: format!(
                        "INSERT INTO {sub_table_name} ({pk_sql}) SELECT {coalesced_values_sql} FROM unnest({unnest_args_sql})",
                    ),
                }
            });
        // Subscribe to all referenced columns in the data source
        real_table
            .subscribed_cols
            .extend(table_info.columns.iter().cloned());

        // Now in order for this to be incrementally maintainable
        // we need to have a mapping from the real_table_pks to our row pks
        // TODO: Introduce a mapping table for GroupBy/Distinct/Exists support
        //       For now let's only handle the case where there's an 1to1 mapping available
        // Essentially find all row pks which depend on this real_table's PKs
        let real_to_query_pk = real_table
            .table_pk
            .iter()
            .map(|pk_col| {
                let affected_row_pks = join_pk
                    .iter()
                    .filter(|x| {
                        // TODO: For group by support check the sources of the expr, not the kind
                        if let ExprKind::Column(col_ref) = &x.kind {
                            col_ref.table_scope_depth == 0
                                && col_ref.column_name == *pk_col
                                && col_ref.table.real_table == real_table.main_table_name
                                && col_ref.table.alias == table_def.alias
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();
                (pk_col.clone(), affected_row_pks)
            })
            .collect::<Vec<(String, Vec<ReturnExpr>)>>();

        // TODO: Add mapping tables for group_by/distinct support
        // This way we know how PKs map to groups :D
        // create table as {table_name} AS SELECT {real_pks}, {row_pks} FROM {stmt}
        // For now only support the identity mapping
        for (_real_pk, query_pks) in &real_to_query_pk {
            if query_pks.len() != 1 {
                return Err(SqlAnalysisError::QueryNotIncrementallyMaintainable {
                    reason: "Unable to map real PK to query PK".to_string(),
                });
            }
        }

        let limit_to_pks_expr = expr_filter_by_query_pk(real_table, &real_to_query_pk)?;

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
        // Remove trailing ;
        let mut new_query = Cmd::Stmt(stmt).to_string();
        new_query.pop();

        // Sub DB filtered by PKs
        let all_cols = result_set_with_pk
            .iter()
            .map(|x| x.alias.0.clone())
            .collect::<Vec<_>>()
            .join(",");

        let column_aliases = real_to_query_pk
            .iter()
            .map(|(_, row_pks)| row_pks.first().map(|pk| pk.alias.0.clone()).unwrap())
            .collect::<Vec<_>>();

        let index_name = format!("index_{}_{}_subpk", table_def.alias, table_def.real_table);
        let index = IndexDef {
            index_name: index_name.clone(),
            column_aliases,
            coalesce: true,
            filter: None,
        };
        let index_columns_sql = index.index_columns_sql();
        required_query_indexes.push(index);
        let temp_query: String = format!(
            "SELECT {} FROM query INDEXED BY {} WHERE ({}) IN temp.{}",
            all_cols, index_name, index_columns_sql, real_table.sub_table_name
        );

        let real_table_name = &table_def.real_table;
        let table_alias = &table_def.alias;
        info!(%sql_hash, %sub_id, "modified query for table '{real_table_name}' AS '{table_alias}': {new_query}");

        query_tables.insert(
            table_def.alias.clone(),
            QueryTableMatcherStmt {
                table_def: table_def.clone(),
                real_pks_to_row_pk: real_to_query_pk,
                new_query,
                temp_query,
            },
        );
    }

    // Let's establish what assumptions of tables being NULL/NOT_NULL are worth analyzing
    let nullability_scenarios = establish_table_nullability(&parsed);

    // Now for each scenario calculate the minimal PK under the nullability assumptions
    // Different assumptions can yield the same minimal PK
    let mut minimal_pks: HashMap<MinimalPK, Vec<&NullabilityScenario>> = HashMap::new();
    for scenario in &nullability_scenarios {
        info!(%sql_hash, %sub_id, "Will analyze the scenario: {}", scenario.short_description());
        let minimal_pk = calculate_minimal_pk(&join_pk, scenario, &parsed, schema);
        minimal_pks.entry(minimal_pk).or_default().push(scenario);
    }

    // Now generate the identity unique indexes
    let mut identity_uniq_indexes = Vec::with_capacity(minimal_pks.len());
    for (MinimalPK(minimal_pk), possible_scenarios) in minimal_pks {
        // Combine the scenarios into a single scenario
        let scenario = {
            let mut it = possible_scenarios.iter();
            let first = (*it.next().unwrap()).clone();
            // Combine the scenarios into a single scenario
            it.fold(first, |mut acc, item| {
                acc.or_mut(item);
                acc
            })
        };
        let mut h = DefaultHasher::new();
        minimal_pk.hash(&mut h);
        let pk_id = h.finish();

        // Calculate attr_closure for this pk
        let (eq_constrains, constant_constrains) =
            scenario.for_pk_minimization(&parsed.where_constrains);
        let eq_constrains = eq_constrains
            .iter()
            .map(|eq| bitset_from_iter(&parsed, eq.iter()))
            .collect::<Vec<_>>();
        let constant_constrains = constant_constrains
            .iter()
            .map(|constant| bitset_from_iter(&parsed, constant.iter()))
            .collect::<Vec<_>>();
        let fds = determine_fds(&parsed, &scenario, schema);
        let attr_closure =
            calculate_attr_closure(&minimal_pk, &eq_constrains, &constant_constrains, &fds);

        // Only use the PKs that are present in the minimal PK
        let minimal_pk = join_pk
            .iter()
            .filter(|x| {
                x.as_column()
                    .map(|c| {
                        minimal_pk
                            .contains(parsed.columns_in_current_scope.get_index_of(c).unwrap())
                    })
                    // Expressions can be eliminated if the expression is deterministic and
                    // The columns in the PK are enough to determine the sources for the expression
                    // TODO: parse out info about expression determinism
                    .unwrap_or_else(|| {
                        x.as_complex_sources()
                            .map(|sources| {
                                // Keep expr if it has no sources(rand() for ex) or if at least one source is not in the attr closure
                                sources.is_empty()
                                    || sources.iter().any(|source| {
                                        !attr_closure.0.contains(
                                            parsed
                                                .columns_in_current_scope
                                                .get_index_of(source)
                                                .unwrap(),
                                        )
                                    })
                            })
                            // Keep constants for now
                            // TODO: Eliminate all constants besides 1 if that's
                            //       the only PK. for ex. SELECT DISTINCT 1 FROM ...
                            .unwrap_or(true)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();

        // Use all non fully dependent columns as the identity index
        let column_aliases = minimal_pk
            .iter()
            .map(|pk| pk.alias.0.clone())
            .collect::<Vec<_>>();

        let filter_predicates = scenario
            .table_info
            .iter()
            .filter_map(|(table_def, state)| {
                state.table_is_null.map(|is_null| {
                    let pk_aliases = join_pk
                        .iter()
                        .filter(|x| {
                            x.as_column()
                                .map(|expr| &expr.table == table_def)
                                .unwrap_or(false)
                        })
                        .map(|pk| pk.alias.0.clone())
                        .collect::<Vec<_>>();
                    TableNullPredicate {
                        table: table_def.clone(),
                        pk_aliases,
                        is_null,
                    }
                })
            })
            .collect::<Vec<_>>();

        identity_uniq_indexes.push(IndexDef {
            index_name: format!("identity_uniq_index_{pk_id}"),
            column_aliases,
            coalesce: true,
            filter: if filter_predicates.is_empty() {
                None
            } else {
                Some(IndexFilter {
                    predicates: filter_predicates,
                })
            },
        });
    }

    Ok(PreparedSubscriptionSql {
        initial_query: stmt,
        pk: join_pk,
        result_set,
        result_set_with_pk,
        identity_uniq_indexes,
        required_query_indexes,
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

fn expr_filter_by_query_pk(
    real_table: &RealTableMatcherStmt,
    real_to_query_pks: &[(String, Vec<ReturnExpr>)],
) -> Result<Expr, SqlAnalysisError> {
    let expr = Expr::in_table(
        Expr::Parenthesized(
            real_to_query_pks
                .iter()
                .map(|(_, row_pks)| Expr::FunctionCall {
                    name: Id("coalesce".to_string()),
                    distinctness: None,
                    args: Some(vec![
                        row_pks.first().unwrap().expr.clone(),
                        Expr::Literal(Literal::String("\"\"".to_string())),
                    ]),
                    order_by: None,
                    filter_over: None,
                })
                .collect(),
        ),
        false,
        QualifiedName::fullname(Name("temp".into()), Name(real_table.sub_table_name.clone())),
        None,
    );

    Ok(expr)
}

// Establish the primary key of a result row of the parsed query
fn derive_join_pk(parsed: &ParsedSelect, schema: &Schema) -> Vec<ReturnExpr> {
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
                ReturnExpr {
                    sources: HashSet::from([resolved_column.clone()]),
                    expr: Expr::Qualified(Name(table_alias.clone()), Name(pk.clone())),
                    kind: ExprKind::Column(resolved_column),
                    alias: Name(format!("__corro_pk_{table_alias}_{pk}")),
                }
            })
        })
        .collect::<Vec<ReturnExpr>>()
}

fn derive_group_pk(parsed: &ParsedSelect) -> Result<Vec<ReturnExpr>, SqlAnalysisError> {
    // This is not exactly a check that this is top level but good enough
    assert!(parsed.parent_column_references.is_empty());
    let pk = if parsed.is_distinct {
        /*if let Some(group_by) = &parsed.group_by {
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
            let group_exprs = ReturnExprAliasHelper::new(group_by);
            let r = parsed
                .result_columns
                .iter()
                .map(|x| {
                    // We could run this check later when establishing aliases
                    let in_group_by = group_exprs.find(x);
                    PkEntry {
                        expr: x.clone(),
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
        } else */
        {
            parsed.result_columns.clone()
        }
    } else if let Some(group_by) = &parsed.group_by {
        // Just use the group by expressions as PKs
        group_by.clone()
    } else {
        Vec::new()
    };
    Ok(pk)
}

// Helper struct to get the most specific alias for a return expression
// And to determine if an PK was already in the result set
// Result Expressions are compared by their AST and Column Reference
struct ReturnExprAliasHelper<'a, T> {
    by_ast: HashMap<String, &'a T>,
    by_col_ref: HashMap<&'a ExprColumnRef, &'a T>,
}

impl<'a, T> ReturnExprAliasHelper<'a, T> {
    fn new<I, K>(return_exprs: I) -> Self
    where
        I: IntoIterator<Item = K> + Copy,
        K: Borrow<&'a T>,
        T: AsRef<ReturnExpr>,
    {
        let by_ast = return_exprs
            .into_iter()
            .map(|x| {
                let data = *x.borrow();
                let r: &ReturnExpr = data.as_ref();
                (r.expr.to_string(), data)
            })
            .collect::<HashMap<String, &'a T>>();
        let by_col_ref = return_exprs
            .into_iter()
            .filter_map(|v| {
                let data = *v.borrow();
                let r: &ReturnExpr = data.as_ref();
                if let ExprKind::Column(col_ref) = &r.kind {
                    Some((col_ref, data))
                } else {
                    None
                }
            })
            .collect::<HashMap<&ExprColumnRef, &T>>();
        Self { by_ast, by_col_ref }
    }

    fn find(&self, expr: &ReturnExpr) -> Option<&&T> {
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
// Will use the most specific alias for expressions
// user_alias > __corro_gk > __corro_pk > __corro_col
fn establish_result_set(
    parsed: &ParsedSelect,
    pks: Vec<ReturnExpr>,
) -> (Vec<ReturnExpr>, Vec<ReturnExpr>, Vec<ReturnExpr>) {
    let empty_vec = vec![];
    let group_exprs =
        ReturnExprAliasHelper::new(parsed.group_by.as_ref().unwrap_or(&empty_vec).as_slice());
    let return_exprs = ReturnExprAliasHelper::new(parsed.result_columns.as_slice());
    let pk_exprs = ReturnExprAliasHelper::new(pks.as_slice());
    let mut new_pks = Vec::with_capacity(pks.len());
    let mut new_result_set = Vec::with_capacity(parsed.result_columns.len());
    let mut new_result_set_with_pk = Vec::with_capacity(parsed.result_columns.len() + pks.len());

    // SELECT foo.id, foo.id, ... FROM foo;
    // We check for alias uniqueness earlier so we can be sure that all input aliasses are unique
    // When reusing an result column as a PK we only need to use 1 result column for that
    // So in the above query the final aliasses should look like this:
    // SELECT foo.id AS __corro_pk_foo_id, foo.id AS __corro_col_1, ... FROM foo;
    // Not like this:
    // SELECT foo.id AS __corro_pk_foo_id, foo.id AS __corro_pk_foo_id, ... FROM foo;
    let mut used_aliases: HashSet<String> = HashSet::new();
    let mut reused_pks: HashSet<String> = HashSet::new();

    for pk in &pks {
        // Only prepend PKs which are not already in the result set
        if return_exprs.find(pk).is_none() {
            // Try using the most specific alias for it
            let mut new_pk = pk.clone();
            if pk.is_internal_alias() {
                if let Some(group_expr) = group_exprs.find(pk) {
                    // Use the group by expr alias if it's more specific and wasn't used before
                    if !used_aliases.contains(&group_expr.alias.0) {
                        new_pk.alias = group_expr.alias.clone()
                    }
                }
            }
            assert!(used_aliases.insert(new_pk.alias.0.clone())); // Important sanity check
            new_result_set_with_pk.push(new_pk.clone());
            new_pks.push(new_pk);
        }
    }

    for res in &parsed.result_columns {
        let pk_expr = pk_exprs.find(res);
        let mut new_res = res.clone();

        if res.is_internal_alias() {
            // Try using the most specific alias for it
            if let Some(group_expr) = group_exprs.find(res) {
                // Use the group by expr alias if it wasn't used before
                if !used_aliases.contains(&group_expr.alias.0) {
                    new_res.alias = group_expr.alias.clone()
                }
            } else if let Some(pk_expr) = pk_expr {
                // Or use the pk alias if it wasn't used before
                if !used_aliases.contains(&pk_expr.alias.0) {
                    new_res.alias = pk_expr.alias.clone();
                }
            }
        }

        assert!(used_aliases.insert(new_res.alias.0.clone())); // Important sanity check
        new_result_set.push(new_res.clone());
        new_result_set_with_pk.push(new_res.clone());
        if let Some(old_pk) = pk_expr {
            // Ok this column might get reused as a PK
            if reused_pks.insert(old_pk.alias.0.clone()) {
                new_pks.push(new_res);
            }
        }
    }

    (new_pks, new_result_set, new_result_set_with_pk)
}

// Helper struct for considering multiple nullability scenarios
#[derive(Debug, Clone)]
struct TableNullability<'a> {
    pub table_details: &'a TableDetails,
    // None -> Uncertain
    // Some(true) -> MustBeNull
    // Some(false) -> MustNotBeNull
    pub table_is_null: Option<bool>,
    // None -> Uncertain
    // Some(true) -> Yes
    // Some(false) -> Nope
    pub constraints_hold: Option<bool>,
}

#[derive(Debug, Clone)]
struct NullabilityScenario<'a> {
    pub table_info: IndexMap<TableDef, TableNullability<'a>>,
}

impl<'a> NullabilityScenario<'a> {
    fn new(parsed: &'a ParsedSelect) -> Self {
        NullabilityScenario {
            table_info: parsed
                .tables
                .iter()
                .map(|(table_def, details)| {
                    (
                        table_def.clone(),
                        TableNullability {
                            table_details: details,
                            table_is_null: None,
                            constraints_hold: None,
                        },
                    )
                })
                .collect(),
        }
    }

    fn state_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for (table_def, table_nullability) in &self.table_info {
            let cvt = |x: Option<bool>| -> u8 {
                match x {
                    None => 0,
                    Some(true) => 1,
                    Some(false) => 2,
                }
            };
            hasher.write(table_def.alias.as_bytes());
            hasher.write(table_def.real_table.as_bytes());
            hasher.write_u8(cvt(table_nullability.table_is_null));
            hasher.write_u8(cvt(table_nullability.constraints_hold));
        }
        hasher.finish()
    }

    fn state_id_without_two(&self, a_idx: usize, b_idx: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        for (idx, (table_def, table_nullability)) in self.table_info.iter().enumerate() {
            if idx == a_idx || idx == b_idx {
                continue;
            }
            let cvt = |x: Option<bool>| -> u8 {
                match x {
                    None => 0,
                    Some(true) => 1,
                    Some(false) => 2,
                }
            };
            hasher.write(table_def.alias.as_bytes());
            hasher.write(table_def.real_table.as_bytes());
            hasher.write_u8(cvt(table_nullability.table_is_null));
            hasher.write_u8(cvt(table_nullability.constraints_hold));
        }
        hasher.finish()
    }

    fn short_description(&self) -> String {
        self.table_info
            .iter()
            .map(|(table_def, state)| {
                format!(
                    "{}(tab={} is_null={} constrains_hold={} join_type={})",
                    table_def.alias,
                    table_def.real_table,
                    state
                        .table_is_null
                        .map(|x| x.to_string())
                        .unwrap_or("not_known".to_string()),
                    state
                        .constraints_hold
                        .map(|x| x.to_string())
                        .unwrap_or("not_known".to_string()),
                    match (
                        state.table_details.lhs_nullable,
                        state.table_details.rhs_nullable
                    ) {
                        (false, false) => "INNER",
                        (false, true) => "LEFT",
                        (true, false) => "RIGHT",
                        (true, true) => "FULL",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    // Changes self into a scenario where both self and rhs can happen
    fn or_mut(&mut self, rhs: &Self) {
        assert!(rhs.table_info.len() == self.table_info.len());
        for ((lhs_table_def, lhs_state), (rhs_table_def, rhs_state)) in
            self.table_info.iter_mut().zip(&rhs.table_info)
        {
            assert!(lhs_table_def == rhs_table_def);
            // Both lhs and rhs need to fully agree - otherwise everything can happen
            if lhs_state.constraints_hold != rhs_state.constraints_hold {
                lhs_state.constraints_hold = None;
            }
            if lhs_state.table_is_null != rhs_state.table_is_null {
                lhs_state.table_is_null = None;
            }
        }
    }

    fn is_certain_state(&self) -> bool {
        // Everything uncertain quality is certain
        self.table_info.iter().all(|(_table_def, state)| {
            state.table_is_null.is_some() && state.constraints_hold.is_some()
        })
    }

    fn is_table_definitelly_null(&self, table_def: &TableDef) -> bool {
        self.table_info
            .get(table_def)
            .map(|x| x.table_is_null == Some(true))
            .unwrap_or_default()
    }

    fn is_table_definitelly_not_null(&self, table_def: &TableDef) -> bool {
        self.table_info
            .get(table_def)
            .map(|x| x.table_is_null == Some(false))
            .unwrap_or_default()
    }

    // Converts this scenario into constraints for pk minimization
    fn for_pk_minimization(
        &self,
        where_constrains: &Option<ExprConstrains>,
    ) -> (Vec<HashSet<ExprColumnRef>>, Vec<HashSet<ExprColumnRef>>) {
        let mut eq_constrains = DisjointHashSet::new();
        let mut constant_constrains = HashMap::<&String, HashSet<ExprColumnRef>>::new();
        // Consider where constrains
        if let Some(where_constrains) = where_constrains {
            for (cid, expr) in &where_constrains.constant_constrains {
                constant_constrains
                    .entry(cid)
                    .or_default()
                    .insert(expr.clone());
            }
            for (a, b) in &where_constrains.eq_constrains {
                eq_constrains.link(a.clone(), b.clone());
            }
        }
        // See which join constrains hold
        for (_table_def, nullability) in &self.table_info {
            // Only consider constrains which definitelly hold
            if nullability.constraints_hold != Some(true) {
                continue;
            }
            for (constant_id, expr) in &nullability.table_details.constraints.constant_constrains {
                constant_constrains
                    .entry(constant_id)
                    .or_default()
                    .insert(expr.clone());
            }
            for (a, b) in &nullability.table_details.constraints.eq_constrains {
                eq_constrains.link(a.clone(), b.clone());
            }
        }
        // If 2 different columns are equal to the same constant then
        // they are also equal to each other
        // If the constant is an NULL then that isn't true but then the whole constrain
        // would evaluate to NULL eitherway so we can assume all constants are not null
        let constant_constrains: Vec<_> = constant_constrains.into_values().collect();
        for same_constant in &constant_constrains {
            let mut iter = same_constant.iter();
            let a = iter.next().unwrap();
            for b in iter {
                eq_constrains.link(a.clone(), b.clone());
            }
        }
        (eq_constrains.sets().collect(), constant_constrains)
    }

    // Returns true if we got an contradiction
    fn assume_table_null(&mut self, null_table_def: TableDef) -> bool {
        let (idx, _table_def, entry) = self.table_info.get_full_mut(&null_table_def).unwrap();
        if entry.table_is_null.is_some() {
            // If it is MustNotBeNull then we got an contradiction
            return !entry.table_is_null.unwrap();
        }
        // Uncertain -> MustBeNull
        entry.table_is_null = Some(true);
        // If this table is nullable it's RHS nullability needs to be true
        // Or some table to the right has LHS nullability true
        // SELECT * FROM a LEFT JOIN b ON ... RIGHT JOIN c ON ...
        // b is null implies left join failed or the right join failed
        // If no such join exists, for ex by only having inner joins then the resulting row will just get filtered out
        // So that table can't be null and we get an contradiction
        let mut possibly_failed_conditions: Vec<TableDef> = Vec::new();
        // This join might have failed
        if entry.table_details.rhs_nullable {
            possibly_failed_conditions.push(null_table_def.clone());
        }
        // RIGHT/FULL joins to the right might have failed
        for to_the_right_idx in idx + 1..self.table_info.len() {
            let (inspected_table_def, info) = self.table_info.get_index(to_the_right_idx).unwrap();
            // It's lhs is nullable and we know it's constraints might not hold
            if info.table_details.lhs_nullable && info.constraints_hold != Some(true) {
                possibly_failed_conditions.push(inspected_table_def.clone());
            }
        }
        if possibly_failed_conditions.is_empty() {
            // We assumed the table is null but there is no way that can be true
            return true;
        }
        // There is only 1 constrain which might have failed!
        // Without just filtering out the whole row
        // Mark it as so
        // When more than 2 constrains might have failed we don't assume anything more
        // We can search deeper into the possibility tree but what we do now is good enough
        if possibly_failed_conditions.len() == 1
            && self
                .assume_join_condition_failed(possibly_failed_conditions.first().unwrap().clone())
        {
            return true;
        }

        // Look at all constrains on all tables, if they reffer to this table the condition will automatically fail
        // as the join clause will auto evaluate to NULL
        // Don't need to look at the WHERE constrains as all tables in it's constrains are marked as not null already
        let mut definitely_failed_conditions: Vec<TableDef> = Vec::new();
        for (inspected_table_def, info) in &self.table_info {
            // NULL = anything_including_null  will result in NULL
            // So the condition will automatically fail
            for (a, b) in &info.table_details.constraints.eq_constrains {
                for expr in [a, b] {
                    if expr.table_scope_depth == 0 && expr.table == null_table_def {
                        definitely_failed_conditions.push(inspected_table_def.clone());
                    }
                }
            }
        }
        for inspected_table_def in definitely_failed_conditions {
            if self.assume_join_condition_failed(inspected_table_def.clone()) {
                return true;
            }
        }

        // No contradictions when assuming that this table is null
        false
    }

    // Returns true if we got an contradiction
    fn assume_join_condition_failed(&mut self, table_def: TableDef) -> bool {
        let (idx, _table_def, entry) = self.table_info.get_full_mut(&table_def).unwrap();
        if entry.constraints_hold.is_some() {
            // If the constraints hold then we got an contradiction
            return entry.constraints_hold.unwrap();
        }
        // Uncertain -> False
        entry.constraints_hold = Some(false);
        let lhs_nullable = entry.table_details.lhs_nullable;
        let rhs_nullable = entry.table_details.rhs_nullable;

        // We need to be careful here, It's always safe to assume that something is null
        // Cause no matter what happens later the table won't get ressurected
        // It's not safe here to assume a table is NOT NULL as something later
        // can force it to become null.
        let mut lhs_definitelly_null = Vec::new();
        let mut lhs_definitelly_not_null = Vec::new();
        let mut lhs_unresolved = Vec::new();
        for to_the_left_idx in 0..idx {
            let (inspected_table_def, info) = self.table_info.get_index(to_the_left_idx).unwrap();
            if info.table_is_null == Some(true) {
                lhs_definitelly_null.push(inspected_table_def.clone());
            } else if info.table_is_null == Some(false) {
                lhs_definitelly_not_null.push(inspected_table_def.clone());
            } else {
                lhs_unresolved.push(inspected_table_def.clone());
            }
        }

        // Failed conditions on full joins mean
        // LHS == NULL OR RHS == NULL
        if lhs_nullable && rhs_nullable {
            // TODO: Check if smth to the right can make both LHS and RHS NULL at the same time
            // If not then we can make more assumptions like
            // LHS == NULL => RHS != NULL
            // RHS == NULL => LHS != NULL
            return false;
        }

        // Failed conditions on left joins mean:
        // RHS == NULL
        if rhs_nullable {
            // TODO: Check if smth to the right can make both LHS and RHS NULL at the same time
            // If not then we can make more assumptions like
            // Exists t IN LHS WHERE t IS NOT NULL
            return self.assume_table_null(table_def.clone());
        }

        // Failed conditions on right joins mean:
        // LHS == NULL
        // If !(LHS == NULL && RHS == NULL) then RHS != NULL
        // Failed conditions on inner joins mean:
        // LHS == NULL AND RHS == NULL

        // Make lhs null
        {
            if !lhs_definitelly_not_null.is_empty() {
                // Something must be not null to the left - contradiction
                return true;
            }
            for inspected_table_def in lhs_unresolved {
                // Assume all tables to the left are null
                if self.assume_table_null(inspected_table_def.clone()) {
                    return true;
                }
            }
        }

        // Make rhs null when we have an inner join
        if !lhs_nullable && self.assume_table_null(table_def.clone()) {
            return true;
        }
        false
    }

    // Returns true if we got an contradiction
    fn assume_table_not_null(&mut self, table_def: TableDef) -> bool {
        let (idx, _table_def, entry) = self.table_info.get_full_mut(&table_def).unwrap();
        if entry.table_is_null.is_some() {
            // If it is MustBeNull then we got an contradiction
            return entry.table_is_null.unwrap();
        }
        // Uncertain -> MustNotBeNull
        entry.table_is_null = Some(false);

        // For this table we have those cases:
        // 1. INNER JOIN x ON ...
        //     If the join clause fails it's not possible for a join to the right to bring this table back!
        //     therefore the join clause must succeed
        // 2. LEFT JOIN x ON ...
        //     The join condition MUST hold, otherwise this table would be null regardless of the joins to the right
        // 3. RIGHT JOIN x. ON ...
        //     The table is present regardless of the join clause holding
        // 4. FULL JOIN x ON ...
        //     The table is present regardless of the join clause holding
        if !entry.table_details.lhs_nullable
            && self.assume_join_condition_succeeds(table_def.clone())
        {
            return true;
        }

        // We now know that nothing to the right of this table can possibly make all tables to it's left NULL
        // Therefore we can make stronger assumptions about which tables are not null

        // For all joins to the right:
        // 1. INNER JOIN x ON ...
        //     join clause must succeed AND x is not null
        // 2. LEFT JOIN x ON ...
        //     The table is present regardless of the join clause holding
        // 3. RIGHT JOIN x. ON ...
        //     join clause must succeed AND x is not null
        // 4. FULL JOIN x ON ...
        //     The table is present regardless of the join clause holding
        for to_the_right_idx in idx + 1..self.table_info.len() {
            let (inspected_table_def, info) = self.table_info.get_index(to_the_right_idx).unwrap();
            if !info.table_details.rhs_nullable {
                if self.assume_join_condition_succeeds(inspected_table_def.clone()) {
                    return true;
                }
                // No need to recurse here, just mark as not null
                let (_table_def, info) = self.table_info.get_index_mut(to_the_right_idx).unwrap();
                if info.table_is_null.is_some() {
                    // If it is MustBeNull then we got an contradiction
                    return info.table_is_null.unwrap();
                }
                // Uncertain -> MustNotBeNull
                info.table_is_null = Some(false);
            }
        }

        false
    }

    fn assume_join_condition_succeeds(&mut self, table_def: TableDef) -> bool {
        let (_idx, _table_def, entry) = self.table_info.get_full_mut(&table_def).unwrap();
        if entry.constraints_hold.is_some() {
            // If the constraints don't hold then we got an contradiction
            return !entry.constraints_hold.unwrap();
        }
        // Uncertain -> True
        entry.constraints_hold = Some(true);
        let _lhs_nullable = entry.table_details.lhs_nullable;
        let _rhs_nullable = entry.table_details.rhs_nullable;

        // Great! we assumed this join clause will not fail
        // That's only possible if all sources of this ON clause are not null
        // othewise the whole condition will evaluate to NULL so we get a contradiction
        // NULL = anything_including_null  will result in NULL
        // So the condition will automatically fail
        let mut definitely_not_null_tables = Vec::new();
        for (a, b) in &entry.table_details.constraints.eq_constrains {
            for expr in [a, b] {
                if expr.table_scope_depth == 0 {
                    definitely_not_null_tables.push(expr.table.clone());
                }
            }
        }
        for (_, expr) in &entry.table_details.constraints.constant_constrains {
            if expr.table_scope_depth == 0 {
                definitely_not_null_tables.push(expr.table.clone());
            }
        }

        for table in definitely_not_null_tables {
            if self.assume_table_not_null(table) {
                return true;
            }
        }

        false
    }
}

// For PK minimization we need to know which constrains need to hold
// The problem being we can't just combine all ON ... constrains together and call it a day
// Cause OUTER JOIN asdf ON <c> emits rows when (c OR !c) - the difference being when c holds
// then both sides of the join are emitted, when !c holds then one/or both sides can be null
// This makes constrains from outer joins useless without additional logic
// The trick is to have multiple unique partial indexes on the table conditional on some tables being null or not
// which combined cover the whole table PK.
// Naively for a statement with M outer joins one would need 2**M indexes - one for each possible outcome of ON clauses
// Hoever most queries will require a few indexes at most cause when one table is NULL it usually forces other tables to be NULL as well
// In the worst case it's still 2**M so instead of considering each possible outcome of ON clauses we look at each table separately
// Then assume it's null and look for contradictions in that assumption, then assume it's not null and analyse again
// This yields at most 2*M indexes even in the case where 2**M indexes are required
// Please note that in practise in most queries there are at most a few outer joins
fn establish_table_nullability(parsed: &ParsedSelect) -> Vec<NullabilityScenario> {
    let mut current_nullability = NullabilityScenario::new(parsed);
    // All referenced tables in the where clause contraints
    // are required to not be null or the where clause will automatically evaluate to NULL
    // and the row would get filtered out
    if let Some(where_constrains) = &parsed.where_constrains {
        // We treat query parameters as constants but fortunately NULL=NULL is still null
        for (_, expr) in &where_constrains.constant_constrains {
            if expr.table_scope_depth == 0
                && current_nullability.assume_table_not_null(expr.table.clone())
            {
                panic!("Logic error - possible bug in where constant constrain logic")
            }
        }
        for (a, b) in &where_constrains.eq_constrains {
            for expr in [a, b] {
                if expr.table_scope_depth == 0
                    && current_nullability.assume_table_not_null(expr.table.clone())
                {
                    panic!("Logic error - possible bug in where eq constrain logic")
                }
            }
        }
    }

    let mut possibilities: HashMap<u64, NullabilityScenario> =
        HashMap::<u64, NullabilityScenario>::new();
    let mut defer_save = true;
    for (table_def, _) in &parsed.tables {
        // The nullability of this table has already been established
        if current_nullability
            .table_info
            .get(table_def)
            .unwrap()
            .table_is_null
            .is_some()
        {
            continue;
        }

        // See what happens if u try to make this table null
        let mut null_case = current_nullability.clone();
        let null_is_not_possible = null_case.assume_table_null(table_def.clone());
        // See what happens if u try to make this table not null
        let mut not_null_case = current_nullability.clone();
        let not_null_is_not_possible = not_null_case.assume_table_not_null(table_def.clone());
        // None of the branches are possible?
        if null_is_not_possible && not_null_is_not_possible {
            // Nothing stops someone from giving us an query which will always return 0 rows
            // Right now we're not parsing neq, IS NULL, NULL constrains so this is a logic error
            // If support for them is added this panic will need to be changed as for ex we can get:
            // SELECT * FROM foo WHERE foo.id IS NULL AND foo.id IS NOT NULL
            panic!("Logic error - possible bug in table nullability logic")
        }
        if !null_is_not_possible && !not_null_is_not_possible {
            // Both branches are possible
            // Check is one of them is a certain state(no branching will happen on it)
            // This can happen pretty often as usually one specific table being null can force all other
            // undetermined tables to be NULL aswell (for ex. chained left joins)
            if null_case.is_certain_state() {
                possibilities.insert(null_case.state_id(), null_case);
                defer_save = true;
                current_nullability = not_null_case;
            } else if not_null_case.is_certain_state() {
                possibilities.insert(not_null_case.state_id(), not_null_case);
                defer_save = true;
                current_nullability = null_case;
            } else {
                // Save both branches
                defer_save = false;
                possibilities.insert(null_case.state_id(), null_case);
                possibilities.insert(not_null_case.state_id(), not_null_case);
            }
        } else if !null_is_not_possible {
            // Only the null case is possible
            defer_save = true;
            current_nullability = null_case;
        } else if !not_null_is_not_possible {
            // Only the not null case is possible
            defer_save = true;
            current_nullability = not_null_case;
        }
    }

    // When there was only 1 branch we continued making assumptions on it
    // So now we need to save it's state if we deffered it
    if defer_save {
        possibilities.insert(current_nullability.state_id(), current_nullability);
    }

    // Now we have a good set of possibilities but we can do a bit better
    let mut possibilities = possibilities.into_values().collect::<Vec<_>>();
    while let Some(refined) = refine_nullability(parsed, &possibilities) {
        possibilities = refined;
    }
    possibilities
}

fn refine_nullability<'a>(
    parsed: &ParsedSelect,
    possibilities: &Vec<NullabilityScenario<'a>>,
) -> Option<Vec<NullabilityScenario<'a>>> {
    // Now let's refine the scenarios.
    // The idea here is find 4 states like this:
    // common_prefix | NULL     | common_middle | idk      | common_suffix
    // common_prefix | NOT NULL | common_middle | idk      | common_suffix
    // common_prefix | idk      | common_middle | NULL     | common_suffix
    // common_prefix | idk      | common_middle | NOT NULL | common_suffix
    // Such patterns are always worth expanding 1 layer deeper
    // as they will yield 4 scenarios but with less uncertainty
    let mut null_count = vec![0; parsed.tables.len()];
    let mut not_null_count = vec![0; parsed.tables.len()];
    let mut idk_count = vec![0; parsed.tables.len()];
    for scenario in possibilities {
        for (i, state) in scenario.table_info.values().enumerate() {
            match state.table_is_null {
                Some(true) => null_count[i] += 1,
                Some(false) => not_null_count[i] += 1,
                None => idk_count[i] += 1,
            }
        }
    }
    // Refine those tables where we have at least 1 null, 1 not null and 2 idk
    let mut refinement_candidates = Vec::new();
    for i in 0..null_count.len() {
        if null_count[i] > 0 && not_null_count[i] > 0 && idk_count[i] > 1 {
            refinement_candidates.push(i);
        }
    }

    if refinement_candidates.len() < 2 {
        return None;
    }

    for a_idx in 0..refinement_candidates.len() {
        let a = refinement_candidates[a_idx];
        for &b in refinement_candidates.iter().skip(a_idx + 1) {
            // Group by state but without the 2 tables we're refining
            let mut groups = HashMap::<u64, Vec<&NullabilityScenario<'a>>>::new();
            for scenario in possibilities {
                groups
                    .entry(scenario.state_id_without_two(a, b))
                    .or_default()
                    .push(scenario);
            }

            let mut new_states: HashMap<u64, NullabilityScenario<'_>> = HashMap::new();
            let mut to_copy: Vec<&Vec<&NullabilityScenario<'_>>> = Vec::new();
            for group in groups.values() {
                if let Some(new_group) = expand_2_tables(group, a, b) {
                    if new_group.len() > group.len() {
                        // Expandable but not worth it
                        to_copy.push(group);
                        continue;
                    }
                    // Worth it after expansion
                    for new_state in new_group {
                        new_states.insert(new_state.state_id(), new_state);
                    }
                } else {
                    // Not expandable
                    to_copy.push(group);
                }
            }

            // Ok some groups were refined
            if !new_states.is_empty() {
                for group in to_copy {
                    for state in group {
                        new_states.insert(state.state_id(), (*state).clone());
                    }
                }
                return Some(new_states.into_values().collect());
            }
        }
    }

    None
}

fn expand_2_tables<'a>(
    states: &[&NullabilityScenario<'a>],
    a_id: usize,
    b_id: usize,
) -> Option<Vec<NullabilityScenario<'a>>> {
    let mut new_states: Option<HashMap<u64, NullabilityScenario<'_>>> = None;

    for idx in 0..states.len() {
        let state = states[idx];
        // Iterate until we find a state where we can expand
        if state.table_info[a_id].table_is_null.is_some()
            && state.table_info[b_id].table_is_null.is_some()
        {
            continue;
        }
        if new_states.is_none() {
            let mut hashmap = HashMap::new();
            // Copy prev states
            for state in states.iter().take(idx) {
                hashmap.insert(state.state_id(), (*state).clone());
            }
            new_states = Some(hashmap);
        }
        if let Some(ref mut hashmap) = new_states {
            let a_table = &state.table_info[a_id].table_details.table;
            let b_table = &state.table_info[b_id].table_details.table;
            for (a_state, b_state) in [(true, true), (true, false), (false, true), (false, false)] {
                let mut new_state = state.clone();
                let mut contradiction = false;
                if a_state {
                    contradiction |= new_state.assume_table_null(a_table.clone())
                } else {
                    contradiction |= new_state.assume_table_not_null(a_table.clone())
                }
                if contradiction {
                    continue;
                }
                if b_state {
                    contradiction |= new_state.assume_table_null(b_table.clone())
                } else {
                    contradiction |= new_state.assume_table_not_null(b_table.clone())
                }
                if contradiction {
                    continue;
                }
                hashmap.insert(new_state.state_id(), new_state);
            }
        }
    }

    if let Some(new_states) = new_states {
        return Some(new_states.into_values().collect());
    }
    None
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct MinimalPK(BitSet<u32>);

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct AttrClosure(BitSet<u32>);

// Left implies right
#[derive(Debug)]
pub struct FunctionalDependency {
    pub left: BitSet<u32>,
    pub right: BitSet<u32>,
}

fn calculate_minimal_pk(
    pks: &[ReturnExpr],
    scenario: &NullabilityScenario,
    parsed: &ParsedSelect,
    schema: &Schema,
) -> MinimalPK {
    let (eq_constrains, constant_constrains) =
        scenario.for_pk_minimization(&parsed.where_constrains);
    let eq_constrains = eq_constrains
        .iter()
        .map(|eq| bitset_from_iter(parsed, eq.iter()))
        .collect::<Vec<_>>();
    let constant_constrains = constant_constrains
        .iter()
        .map(|constant| bitset_from_iter(parsed, constant.iter()))
        .collect::<Vec<_>>();
    let fds = determine_fds(parsed, scenario, schema);

    let starting_pk = bitset_from_iter(
        parsed,
        pks.iter().filter_map(|pk| {
            let col = pk.as_column()?;
            if scenario.is_table_definitelly_null(&col.table) {
                None
            } else {
                Some(col)
            }
        }),
    );

    let mut minimal_pk = starting_pk.clone();

    let mut changed = true;
    while changed && minimal_pk.len() > 1 {
        changed = false;

        // Find an attribute which which after removal will still determine the starting_pk fully
        for attr in minimal_pk.iter() {
            let mut new_minimal_pk = minimal_pk.clone();
            new_minimal_pk.remove(attr);
            let new_attr_closure =
                calculate_attr_closure(&new_minimal_pk, &eq_constrains, &constant_constrains, &fds);
            if starting_pk.is_subset(&new_attr_closure.0) {
                minimal_pk.remove(attr);
                changed = true;
                break;
            }
        }
    }

    MinimalPK(minimal_pk)
}

fn calculate_attr_closure(
    pk: &BitSet<u32>,
    eq_constrains: &[BitSet<u32>],
    constant_constrains: &[BitSet<u32>],
    fds: &Vec<FunctionalDependency>,
) -> AttrClosure {
    let mut attr_closure = pk.clone();
    // All constants are instantly known
    constant_constrains
        .iter()
        .for_each(|group| attr_closure.union_with(group));

    // Iterate until we reach a fixed point
    loop {
        let mut new_attr_closure = attr_closure.clone();

        // Consider column equality
        for eq_group in eq_constrains {
            // If we know at least one column in this group we can derive all columns in this group
            if !new_attr_closure.is_disjoint(eq_group) {
                new_attr_closure.union_with(eq_group);
            }
        }

        // Consider functional dependencies
        for fd in fds {
            if new_attr_closure.is_superset(&fd.left) {
                new_attr_closure.union_with(&fd.right);
            }
        }

        if new_attr_closure != attr_closure {
            attr_closure = new_attr_closure;
        } else {
            break;
        }
    }

    AttrClosure(attr_closure)
}

fn determine_fds(
    parsed: &ParsedSelect,
    scenario: &NullabilityScenario,
    schema: &Schema,
) -> Vec<FunctionalDependency> {
    parsed
        .tables
        .iter()
        .filter_map(|(table_def, _table_details)| {
            if !scenario.is_table_definitelly_not_null(table_def) {
                return None;
            }
            let table_schema = schema.tables.get(table_def.real_table.as_str())?;

            let left = bitset_from_iter(
                parsed,
                table_schema.pk.iter().map(|col_name| -> ExprColumnRef {
                    ExprColumnRef {
                        table: table_def.clone(),
                        table_scope_depth: 0,
                        column_name: col_name.clone(),
                    }
                }),
            );
            let right = bitset_from_iter(
                parsed,
                table_schema
                    .columns
                    .iter()
                    .map(|(col_name, _)| -> ExprColumnRef {
                        ExprColumnRef {
                            table: table_def.clone(),
                            table_scope_depth: 0,
                            column_name: col_name.clone(),
                        }
                    }),
            );

            Some(FunctionalDependency { left, right })
        })
        .collect()
}

fn bitset_from_iter(
    parsed: &ParsedSelect,
    iter: impl IntoIterator<Item = impl Borrow<ExprColumnRef>>,
) -> BitSet<u32> {
    let mut bitset = BitSet::with_capacity(parsed.columns_in_current_scope.len());
    for col_ref in iter {
        let col_ref = col_ref.borrow();
        bitset.insert(
            parsed
                .columns_in_current_scope
                .get_index_of(col_ref)
                .unwrap(),
        );
    }
    bitset
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
    #[error(
        "SELECT ... AS {alias} - The alias is ambigous in the return list, please disambiguate"
    )]
    AliasAmbigousInReturnList { alias: String },
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
    #[error("natural joins can't have an explicit ON or USING clause")]
    NaturalJoinWithUsingOnClause,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_sql;

    fn find_table<'a>(
        analysis: &'a PreparedSubscriptionSql,
        real_table: &str,
    ) -> &'a RealTableMatcherStmt {
        analysis
            .real_tables
            .get(real_table)
            .unwrap_or_else(|| panic!("missing real_table {real_table}"))
    }

    fn has_result_column(parsed: &ParsedSelect, table_alias: &str, col: &str) -> bool {
        parsed.result_columns.iter().any(|r| {
            if let ExprKind::Column(c) = &r.kind {
                c.table.alias == table_alias && c.column_name == col
            } else {
                false
            }
        })
    }

    fn has_omission(parsed: &ParsedSelect, table_alias: &str, col: &str) -> bool {
        parsed
            .column_omissions
            .iter()
            .any(|c| c.table.alias == table_alias && c.column_name == col)
    }

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

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM tree as a JOIN tree as b ON b.parent_id = a.id",
            &schema,
        )
        .unwrap();
        assert!(analysis.query_tables.contains_key("a"));
        assert!(analysis.query_tables.contains_key("b"));

        assert_eq!(analysis.identity_uniq_indexes.len(), 1);
        assert_eq!(
            analysis.identity_uniq_indexes[0].column_aliases,
            vec!["__corro_pk_b_id".to_string()]
        );
    }

    #[test]
    fn test_left_self_join_identity_indexes() {
        let schema = parse_sql(
            "
        CREATE TABLE tree (id INTEGER PRIMARY KEY, parent_id INTEGER);
        ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM tree as a LEFT JOIN tree as b ON b.parent_id = a.id",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.identity_uniq_indexes.len(), 2);

        let b_idx = analysis
            .identity_uniq_indexes
            .iter()
            .find(|d| d.column_aliases == vec!["__corro_pk_b_id".to_string()])
            .expect("missing identity index keyed by b.id");
        let b_filter = b_idx.filter.as_ref().expect("b.id index should be partial");
        assert!(b_filter.predicates.iter().any(|p| {
            p.table.alias == "b"
                && p.pk_aliases == vec!["__corro_pk_b_id".to_string()]
                && !p.is_null
        }));

        let a_idx = analysis
            .identity_uniq_indexes
            .iter()
            .find(|d| d.column_aliases == vec!["__corro_pk_a_id".to_string()])
            .expect("missing identity index keyed by a.id");
        let a_filter = a_idx.filter.as_ref().expect("a.id index should be partial");
        assert!(a_filter.predicates.iter().any(|p| {
            p.table.alias == "b" && p.pk_aliases == vec!["__corro_pk_b_id".to_string()] && p.is_null
        }));
    }

    #[test]
    fn test_three_left_joins_refine_to_single_identity_index() {
        let schema = parse_sql(
            "
            CREATE TABLE consul_services (id INTEGER PRIMARY KEY, instance_id INTEGER);
            CREATE TABLE machines (id INTEGER PRIMARY KEY, machine_version_id INTEGER);
            CREATE TABLE machine_versions (
                machine_id INTEGER,
                id INTEGER,
                PRIMARY KEY(machine_id, id)
            );
            CREATE TABLE machine_version_statuses (
                machine_id INTEGER,
                id INTEGER,
                PRIMARY KEY(machine_id, id)
            );
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT *
             FROM consul_services cs
               LEFT JOIN machines m ON m.id = cs.instance_id
               LEFT JOIN machine_versions mv
                 ON m.id = mv.machine_id AND m.machine_version_id = mv.id
               LEFT JOIN machine_version_statuses mvs
                 ON m.id = mvs.machine_id AND m.machine_version_id = mvs.id",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.identity_uniq_indexes.len(), 1);
        assert_eq!(
            analysis.identity_uniq_indexes[0].column_aliases,
            vec!["__corro_pk_cs_id".to_string()]
        );

        if let Some(filter) = &analysis.identity_uniq_indexes[0].filter {
            assert!(!filter
                .predicates
                .iter()
                .any(|p| p.table.alias == "mv" || p.table.alias == "mvs"));
        }
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

        // Correlated subquery parent refs should be propagated (bar.id referenced from child)
        assert_eq!(analysis.parsed.children.len(), 1);
        assert!(analysis.parsed.children[0]
            .parent_column_references
            .iter()
            .any(|c| c.table.alias == "bar" && c.column_name == "id"));

        // Used columns should include foo.name due to unlikely()
        assert!(find_table(&analysis, "foo")
            .subscribed_cols
            .contains("name"));

        assert!(analysis
            .parsed
            .where_constrains
            .as_ref()
            .expect("expected where constraints")
            .constant_constrains
            .iter()
            .any(|(_cid, expr)| expr.table.alias == "foo" && expr.column_name == "name"));

        // LEFT JOIN should yield multiple identity indexes (bar can be null or not null)
        assert!(analysis.identity_uniq_indexes.len() >= 2);
    }

    #[test]
    fn test_duplicate_select_alias_rejected() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT 1 AS foo, 2 AS foo FROM foo", &schema)
                .unwrap_err(),
            SqlAnalysisError::AliasAmbigousInReturnList { alias } if alias == "foo"
        ));
    }

    #[test]
    fn test_unqualified_ambiguous_column_requires_qualification() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            CREATE TABLE bar (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT id FROM foo JOIN bar ON 1", &schema)
                .unwrap_err(),
            SqlAnalysisError::QualificationRequired { col_name } if col_name == "id"
        ));
    }

    #[test]
    fn test_using_clause_omits_rhs_column_from_star_and_unqualified_resolution() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, x INTEGER);
            CREATE TABLE bar (id INTEGER PRIMARY KEY, x INTEGER);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo JOIN bar USING(x)",
            &schema,
        )
        .unwrap();

        assert!(has_result_column(&analysis.parsed, "foo", "x"));
        assert!(!has_result_column(&analysis.parsed, "bar", "x"));
        assert!(has_omission(&analysis.parsed, "bar", "x"));

        let analysis2 = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT x FROM foo JOIN bar USING(x)",
            &schema,
        )
        .unwrap();

        // It should resolve unqualified due to omission semantics
        assert_eq!(analysis2.result_set.len(), 1);
        assert!(find_table(&analysis2, "foo").subscribed_cols.contains("x"));
        assert!(find_table(&analysis2, "bar").subscribed_cols.contains("x"));
    }

    #[test]
    fn test_table_star_unknown_alias_errors() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT missing.* FROM foo", &schema).unwrap_err(),
            SqlAnalysisError::TableStarNotFound { tbl_name } if tbl_name == "missing"
        ));
    }

    #[test]
    fn test_schema_qualification_main_ok_other_rejected() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        let analysis =
            prepare_subscription_sql(Uuid::new_v4(), "SELECT main.foo.id FROM foo", &schema)
                .unwrap();
        assert!(find_table(&analysis, "foo").subscribed_cols.contains("id"));

        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT other.foo.id FROM foo", &schema).unwrap_err(),
            SqlAnalysisError::UnknownSchema { schema_name } if schema_name == "other"
        ));
    }

    #[test]
    fn test_double_quoted_unresolved_identifier_is_constant_not_column() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT foo.\"id\" AS id1, \"id\" AS id2 FROM foo WHERE \"not_a_column\" = 1",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.result_set.len(), 2);
        for r in &analysis.result_set {
            match &r.kind {
                ExprKind::Column(c) => {
                    assert_eq!(c.table.alias, "foo");
                    assert_eq!(c.column_name, "id");
                }
                other => panic!("expected a column ref, got: {other:?}"),
            }
        }

        assert!(!find_table(&analysis, "foo")
            .subscribed_cols
            .contains("not_a_column"));
    }

    #[test]
    fn test_group_by_rejected() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        assert!(matches!(
            prepare_subscription_sql(Uuid::new_v4(), "SELECT id FROM foo GROUP BY 1", &schema)
                .unwrap_err(),
            SqlAnalysisError::UnsupportedGroupByExpression { .. }
        ));
    }

    #[test]
    fn test_tuple_parenthesized_expr_tracks_columns() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, parent_id INTEGER);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo WHERE (foo.id, foo.parent_id) = (1, 3)",
            &schema,
        )
        .unwrap();

        assert!(find_table(&analysis, "foo").subscribed_cols.contains("id"));
        assert!(find_table(&analysis, "foo")
            .subscribed_cols
            .contains("parent_id"));
        assert!(analysis
            .parsed
            .where_constrains
            .as_ref()
            .map(|c| c.eq_constrains.is_empty())
            .unwrap_or(true));
    }

    #[test]
    fn test_or_suppresses_eq_constraints_affecting_identity_pk_minimization() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, bar_id INTEGER);
            CREATE TABLE bar (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        let and_case = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo JOIN bar ON 1 WHERE foo.bar_id = bar.id",
            &schema,
        )
        .unwrap();

        let or_case = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo JOIN bar ON 1 WHERE (foo.bar_id = bar.id) OR (foo.bar_id = 123)",
            &schema,
        )
        .unwrap();

        assert!(and_case.identity_uniq_indexes.len() == 1);
        assert!(and_case.identity_uniq_indexes[0].column_aliases.len() == 1);
        assert!(or_case.identity_uniq_indexes.len() == 1);
        assert!(or_case.identity_uniq_indexes[0].column_aliases.len() == 2);
    }

    #[test]
    fn test_correlated_exists_propagates_parent_columns() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE bar (id INTEGER PRIMARY KEY, foo_id INTEGER);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT foo.id FROM foo WHERE EXISTS (SELECT 1 FROM bar WHERE bar.foo_id = foo.name)",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.parsed.children.len(), 1);
        assert!(find_table(&analysis, "foo").subscribed_cols.contains("id"));
        assert!(find_table(&analysis, "foo")
            .subscribed_cols
            .contains("name"));
    }

    #[test]
    fn test_non_correlated_exists_does_not_subscribe_to_child_table() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE bar (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT foo.name FROM foo WHERE EXISTS (SELECT 1 FROM bar WHERE bar.id = 1)",
            &schema,
        )
        .unwrap();

        assert!(analysis.real_tables.contains_key("foo"));
        assert!(!analysis.real_tables.contains_key("bar"));
        assert_eq!(analysis.parsed.children.len(), 1);
        assert!(analysis.parsed.children[0]
            .tables
            .iter()
            .any(|(t, _)| t.real_table == "bar"));
    }

    #[test]
    fn test_scalar_subquery_can_reference_parent_column() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo WHERE 2 = (SELECT id)",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.parsed.children.len(), 1);
        assert!(find_table(&analysis, "foo").subscribed_cols.contains("id"));
        assert!(analysis.parsed.children[0]
            .parent_column_references
            .iter()
            .any(|c| c.table.real_table == "foo" && c.column_name == "id"));
    }

    #[test]
    fn test_left_join_indexes_and_join_rewrite_and_where_forces_not_null() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE bar (id INTEGER PRIMARY KEY, foo_id INTEGER);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.foo_id",
            &schema,
        )
        .unwrap();

        assert_eq!(analysis.required_query_indexes.len(), 2);
        let req_names = analysis
            .required_query_indexes
            .iter()
            .map(|d| d.index_name.clone())
            .collect::<Vec<_>>();
        assert!(req_names.iter().any(|n| n == "index_foo_foo_subpk"));
        assert!(req_names.iter().any(|n| n == "index_bar_bar_subpk"));

        assert!(analysis.identity_uniq_indexes.len() >= 2);
        let bar_predicates = analysis
            .identity_uniq_indexes
            .iter()
            .filter_map(|d| d.filter.as_ref())
            .flat_map(|f| f.predicates.iter())
            .filter(|p| p.table.alias == "bar")
            .collect::<Vec<_>>();
        assert!(bar_predicates.iter().any(|p| p.is_null));
        assert!(bar_predicates.iter().any(|p| !p.is_null));

        let bar_q = analysis.query_tables.get("bar").unwrap().new_query();
        assert!(bar_q.contains(" JOIN "));
        assert!(!bar_q.contains("LEFT"));

        let forced = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.foo_id WHERE bar.id = 1",
            &schema,
        )
        .unwrap();
        assert_eq!(forced.identity_uniq_indexes.len(), 1);
    }

    #[test]
    fn test_indexed_by_is_removed_from_new_query() {
        let schema = parse_sql(
            "
            CREATE TABLE foo (id INTEGER PRIMARY KEY);
            CREATE TABLE bar (id INTEGER PRIMARY KEY, foo_id INTEGER);
            CREATE INDEX idx_bar ON bar(foo_id);
            ",
        )
        .unwrap();

        let analysis = prepare_subscription_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo JOIN bar INDEXED BY idx_bar ON foo.id = bar.foo_id",
            &schema,
        )
        .unwrap();

        let bar_q = analysis.query_tables.get("bar").unwrap().new_query();
        assert!(!bar_q.contains("INDEXED BY idx_bar"));
    }

    #[test]
    fn test_generated_column_expands_to_source_columns() {
        let schema = parse_sql(
            "
            CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                a INTEGER,
                b INTEGER,
                g INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL
            );
            ",
        )
        .unwrap();

        let analysis =
            prepare_subscription_sql(Uuid::new_v4(), "SELECT g FROM t", &schema).unwrap();
        let subs = &find_table(&analysis, "t").subscribed_cols;
        assert!(subs.contains("a"));
        assert!(subs.contains("b"));
        assert!(!subs.contains("g"));
    }
}
