use fallible_iterator::FallibleIterator;
use std::collections::{HashMap, HashSet};

use enquote::unquote;
use indexmap::IndexMap;
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

#[derive(Debug, Default, Clone)]
pub struct ParsedSelect {
    pub table_columns: IndexMap<String, HashSet<String>>,
    pub aliases: HashMap<String, String>,
    pub columns: Vec<ResultColumn>,
    pub children: Vec<ParsedSelect>,
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
                Stmt::Select(ref select) => extract_select_columns(select, schema)?,
                _ => return Err(SqlAnalysisError::UnsupportedStatement),
            };

            (stmt, parsed)
        }
        _ => return Err(SqlAnalysisError::StatementRequired),
    };

    if parsed.table_columns.is_empty() {
        return Err(SqlAnalysisError::TableRequired);
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
                                .find_map(|(alias, actual)| (actual == tbl_name).then_some(alias))
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
                                        Expr::Qualified(Name(tbl_name.clone()), Name(pk.clone())),
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

    for (idx, (tbl_name, _cols)) in parsed.table_columns.iter().enumerate() {
        let expr = table_to_expr(
            &parsed.aliases,
            schema
                .tables
                .get(tbl_name)
                .expect("this should not happen, missing table in schema"),
            tbl_name,
        )?;

        let mut stmt = stmt.clone();

        if let Stmt::Select(select) = &mut stmt {
            if let OneSelect::Select {
                where_clause, from, ..
            } = &mut select.body.select
            {
                *where_clause = if let Some(prev) = where_clause.take() {
                    Some(Expr::Binary(
                        Box::new(expr),
                        Operator::And,
                        Box::new(Expr::parenthesized(prev)),
                    ))
                } else {
                    Some(expr)
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

        let mut new_query = Cmd::Stmt(stmt).to_string();
        new_query.pop();

        let mut all_cols = pks.values().flatten().cloned().collect::<Vec<String>>();
        for i in 0..(parsed.columns.len()) {
            all_cols.push(format!("col_{i}"));
        }

        let temp_query = format!(
            "SELECT {} FROM query WHERE ({}) IN temp_{tbl_name}",
            all_cols.join(","),
            pks.get(tbl_name)
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

fn extract_select_columns(
    select: &Select,
    schema: &Schema,
) -> Result<ParsedSelect, SqlAnalysisError> {
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
                                return Err(SqlAnalysisError::TableNotFound(name.name.0.clone()));
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
                                        insert_col(entry, schema, &tbl_name.0, &name.0);
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

fn extract_expr_columns(
    expr: &Expr,
    schema: &Schema,
    parsed: &mut ParsedSelect,
) -> Result<(), SqlAnalysisError> {
    match expr {
        // simplest case
        Expr::Qualified(tblname, colname) => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            insert_col(
                parsed
                    .table_columns
                    .entry(resolved_name.clone())
                    .or_default(),
                schema,
                resolved_name,
                &colname.0,
            );
        }
        // simplest case but also mentioning the schema
        Expr::DoublyQualified(schema_name, tblname, colname) if schema_name.0 == "main" => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            insert_col(
                parsed
                    .table_columns
                    .entry(resolved_name.clone())
                    .or_default(),
                schema,
                resolved_name,
                &colname.0,
            );
        }

        Expr::Name(colname) => {
            let check_col_name = unquote(&colname.0).ok().unwrap_or(colname.0.clone());

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&check_col_name) {
                        if found.is_some() {
                            return Err(SqlAnalysisError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                insert_col(
                    parsed.table_columns.entry(found.to_owned()).or_default(),
                    schema,
                    found,
                    &check_col_name,
                );
            } else {
                return Err(SqlAnalysisError::TableForColumnNotFound {
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
                            return Err(SqlAnalysisError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                insert_col(
                    parsed.table_columns.entry(found.to_owned()).or_default(),
                    schema,
                    found,
                    &colname.0,
                );
            } else {
                if colname.0.starts_with('"') {
                    return Ok(());
                }
                return Err(SqlAnalysisError::TableForColumnNotFound {
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
            return Err(SqlAnalysisError::UnsupportedExpr { expr: expr.clone() })
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
    schema: &Schema,
    parsed: &mut ParsedSelect,
) -> Result<(), SqlAnalysisError> {
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
                        return Err(SqlAnalysisError::TableStarNotFound {
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
                    return Err(SqlAnalysisError::TableStarNotFound {
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
    tbl: &Table,
    table: &str,
) -> Result<Expr, SqlAnalysisError> {
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
        QualifiedName::fullname(Name("__corro_sub".into()), Name(format!("temp_{table}"))),
        None,
    );

    Ok(expr)
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
    #[error("<tbl>.{col_name} qualification required for ambiguous column name")]
    QualificationRequired { col_name: String },
    #[error("could not find table for column {col_name}")]
    TableForColumnNotFound { col_name: String },
    #[error("missing primary keys, this shouldn't happen")]
    MissingPrimaryKeys,
}

#[cfg(test)]
mod tests {
    use crate::schema::parse_sql;

    use super::*;

    #[test]
    fn test_left_join() {
        let schema = parse_sql("
        CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE bar (id INTEGER PRIMARY KEY, foo_id INTEGER);
        CREATE TABLE baz (id INTEGER PRIMARY KEY, bar_id INTEGER);
        ").unwrap();
        let analysis = analyze_sql(
            Uuid::new_v4(),
            "SELECT * FROM foo LEFT JOIN bar ON foo.id = bar.foo_id WHERE EXISTS (SELECT 1 FROM baz WHERE bar_id = bar.id) AND foo.name = 'test'",
            &schema,
        )
        .unwrap();
        println!("Analysis: {:#?}", analysis);
    }
}
