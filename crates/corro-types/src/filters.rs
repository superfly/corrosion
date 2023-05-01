use std::{cmp::Ordering, collections::HashMap, ops::Deref};

use enquote::unquote;
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use rusqlite::types::Type;
use serde::{Deserialize, Serialize};
use sqlite3_parser::{
    ast::{Cmd, Expr, Literal, OneSelect, Operator, Stmt},
    lexer::sql::Parser,
};
use tracing::{error, trace};

use crate::change::{Change, SqliteValue, SqliteValueRef};

const CORRO_EVENT: &str = "evt_type";
const CORRO_TABLE: &str = "tbl_name";

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sqlite_type: Type,
    pub primary_key: bool,
    pub nullable: bool,
}

// schema contains a mapping of
pub type Schema = HashMap<String, Vec<Column>>;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("unknown event {0}")]
    UnknownEvent(String),
    #[error(transparent)]
    Parse(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("unsupported command")]
    UnsupportedCmd(Cmd),
    #[error("unsupported statement")]
    UnsupportedStmt(Stmt),
    #[error("unsupported select")]
    UnsupportedSelect(OneSelect),
    #[error("unsupported expr: {0}")]
    UnsupportedExpr(Expr),
    #[error("unsupported left-hand-side expr: {0}")]
    UnsupportedLhsExpr(Expr),
    #[error("unsupported binary operator: {0:?}")]
    UnsupportedOperator(Operator),
    #[error("unsupported right-hand-side expr: {0}")]
    UnsupportedRhsExpr(Expr),
    #[error("invalid column {1} for table {0}")]
    InvalidColumn(String, String),
    #[error("unsupported right-hand-side literal: {0:?}")]
    UnsupportedLiteral(Literal),
    #[error("literal was expected")]
    ExpectedLiteral,
    #[error("left-hand-side identifier / name expected")]
    ExpectedLhsIdentifier,
    #[error("qualified name in 'table.column' format is required, got: '{0}'")]
    QualifiedNameRequired(String),
    #[error("invalid blob literal: {0}")]
    InvalidBlobLiteral(#[from] hex::FromHexError),
    #[error("numeric literal was neither a i64 or f64")]
    NumericNeitherIntegerNorReal,
    #[error("wrong literal type for column type")]
    WrongLiteralType,
    #[error("column is not nullable")]
    ColumnIsNotNullable,
    #[error("unsupported binary operator {0:?}")]
    UnsupportedBinaryOp(Operator),
    #[error("unsupported null operator {0:?}")]
    UnsupportedNullOp(Operator),
}

#[derive(Debug)]
pub enum SupportedExpr {
    TableName {
        name: String,
        not_equal: bool,
    },
    EventType {
        evt: String,
        not_equal: bool,
    },

    LiteralInteger {
        table: String,
        col: String,
        op: BinaryOp,
        lhs: i64,
    },
    LiteralReal {
        table: String,
        col: String,
        op: BinaryOp,
        lhs: f64,
    },
    LiteralText {
        table: String,
        col: String,
        op: BinaryOp,
        lhs: String,
    },
    LiteralBlob {
        table: String,
        col: String,
        op: BinaryOp,
        lhs: Vec<u8>,
    },
    LiteralNull {
        table: String,
        col: String,
        op: NullOp,
    },

    // BinaryLiteral(FilterLhs, BinaryOp, FilterRhs),
    BinaryAnd(Box<SupportedExpr>, Box<SupportedExpr>),
    BinaryOr(Box<SupportedExpr>, Box<SupportedExpr>),
    Parenthesized(Vec<SupportedExpr>),
    InList {
        list: Vec<SupportedExpr>,
        not: bool,
    },

    IsNull(FilterLhs),
    NotNull(FilterLhs),
}

#[derive(Debug)]
pub enum NullOp {
    Is,
    IsNot,
}

#[derive(Debug)]
pub enum SupportedRhsLiteral {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Copy, Clone)]
pub enum BinaryOp {
    Equals,
    Greater,
    GreaterEquals,
    Is,
    IsNot,
    Less,
    LessEquals,
    NotEquals,
}

#[derive(Debug, Clone)]
pub enum FilterLhs {
    Column { table: String, name: String },
    TableName,
    EventType,
}

#[derive(Debug)]
pub enum FilterRhs {
    Literal(SupportedRhsLiteral),
    TableName(String),
    EventType(ChangeEvent),
}

pub fn parse_expr(input: &str) -> Result<SupportedExpr, ParseError> {
    // kind of a hack...
    let input = format!("SELECT dummy FROM dummy WHERE {input}");

    let mut parser = Parser::new(input.as_bytes());

    // only parse the first statement
    let cmd = parser.next()?.unwrap();

    let expr = match cmd {
        Cmd::Stmt(stmt) => match stmt {
            Stmt::Select(select) => match select.body.select {
                OneSelect::Select {
                    where_clause: Some(where_clause),
                    ..
                } => where_clause.try_into()?,
                select => return Err(ParseError::UnsupportedSelect(select)),
            },
            stmt => return Err(ParseError::UnsupportedStmt(stmt)),
        },
        cmd => return Err(ParseError::UnsupportedCmd(cmd)),
    };

    Ok(expr)
}

impl TryFrom<Expr> for FilterLhs {
    type Error = ParseError;

    fn try_from(lhs: Expr) -> Result<Self, Self::Error> {
        match lhs {
            Expr::Id(id) => match id.0.as_str() {
                CORRO_TABLE => Ok(FilterLhs::TableName),
                CORRO_EVENT => Ok(FilterLhs::EventType),
                _ => Err(ParseError::QualifiedNameRequired(id.0.clone())),
            },
            Expr::Name(name) => match name.0.as_str() {
                CORRO_TABLE => Ok(FilterLhs::TableName),
                CORRO_EVENT => Ok(FilterLhs::EventType),
                _ => Err(ParseError::QualifiedNameRequired(name.0.clone())),
            },
            Expr::Qualified(tbl_name, name) => Ok(FilterLhs::Column {
                table: tbl_name.0.clone(),
                name: name.0.clone(),
            }),
            _ => Err(ParseError::ExpectedLhsIdentifier),
        }
    }
}

impl TryFrom<Literal> for SupportedRhsLiteral {
    type Error = ParseError;

    fn try_from(lit: Literal) -> Result<Self, Self::Error> {
        match lit {
            Literal::Null => Ok(SupportedRhsLiteral::Null),
            Literal::Blob(b) => Ok(SupportedRhsLiteral::Blob(hex::decode(b)?)),
            Literal::String(s) => Ok(SupportedRhsLiteral::Text(unquote(s.as_str()).unwrap_or(s))),
            Literal::Numeric(n) => match n.parse::<i64>() {
                Ok(i) => Ok(SupportedRhsLiteral::Integer(i)),
                Err(_) => match n.parse::<f64>() {
                    Ok(f) => Ok(SupportedRhsLiteral::Real(f)),
                    Err(_) => Err(ParseError::NumericNeitherIntegerNorReal),
                },
            },
            _ => Err(ParseError::UnsupportedLiteral(lit)),
        }
    }
}

impl TryFrom<(Expr, FilterLhs, Operator)> for SupportedExpr {
    type Error = ParseError;

    fn try_from((rhs, lhs, op): (Expr, FilterLhs, Operator)) -> Result<Self, Self::Error> {
        match rhs {
            Expr::Literal(lit) => {
                let rhs: SupportedRhsLiteral = lit.try_into()?;
                match (lhs, op, rhs) {
                    (FilterLhs::TableName, op, SupportedRhsLiteral::Text(name)) => {
                        let not_equal = match op {
                            Operator::Equals | Operator::Is => false,
                            Operator::NotEquals | Operator::IsNot => true,
                            _ => return Err(ParseError::UnsupportedBinaryOp(op)),
                        };

                        Ok(SupportedExpr::TableName { name, not_equal })
                    }

                    (FilterLhs::TableName, _, _) => Err(ParseError::WrongLiteralType),

                    (FilterLhs::EventType, op, SupportedRhsLiteral::Text(s)) => {
                        let not_equal = match op {
                            Operator::Equals | Operator::Is => false,
                            Operator::NotEquals | Operator::IsNot => true,
                            _ => return Err(ParseError::UnsupportedBinaryOp(op)),
                        };
                        Ok(SupportedExpr::EventType { evt: s, not_equal })
                    }

                    (FilterLhs::EventType, _, _) => Err(ParseError::WrongLiteralType),

                    (FilterLhs::Column { table, name }, op, SupportedRhsLiteral::Null) => {
                        let op = op.try_into()?;
                        Ok(SupportedExpr::LiteralNull {
                            table,
                            col: name,
                            op,
                        })
                    }

                    (FilterLhs::Column { table, name }, op, rhs) => {
                        let op = op.try_into()?;
                        Ok(match rhs {
                            SupportedRhsLiteral::Integer(value) => SupportedExpr::LiteralInteger {
                                table,
                                col: name,
                                op,
                                lhs: value,
                            },
                            SupportedRhsLiteral::Real(value) => SupportedExpr::LiteralReal {
                                table,
                                col: name,
                                op,
                                lhs: value,
                            },
                            SupportedRhsLiteral::Text(value) => SupportedExpr::LiteralText {
                                table,
                                col: name,
                                op,
                                lhs: value,
                            },
                            SupportedRhsLiteral::Blob(value) => SupportedExpr::LiteralBlob {
                                table,
                                col: name,
                                op,
                                lhs: value,
                            },
                            SupportedRhsLiteral::Null => unreachable!(),
                        })
                    }
                }
            }
            _ => Err(ParseError::ExpectedLiteral),
        }
    }
}

impl TryFrom<Operator> for BinaryOp {
    type Error = ParseError;

    fn try_from(value: Operator) -> Result<Self, Self::Error> {
        Ok(match value {
            Operator::Equals => BinaryOp::Equals,
            Operator::Greater => BinaryOp::Greater,
            Operator::GreaterEquals => BinaryOp::GreaterEquals,
            Operator::Is => BinaryOp::Is,
            Operator::IsNot => BinaryOp::IsNot,
            Operator::Less => BinaryOp::Less,
            Operator::LessEquals => BinaryOp::LessEquals,
            Operator::NotEquals => BinaryOp::NotEquals,
            op => return Err(ParseError::UnsupportedBinaryOp(op)),
        })
    }
}

impl TryFrom<Operator> for NullOp {
    type Error = ParseError;

    fn try_from(value: Operator) -> Result<Self, Self::Error> {
        Ok(match value {
            Operator::Is => NullOp::Is,
            Operator::IsNot => NullOp::IsNot,
            op => return Err(ParseError::UnsupportedNullOp(op)),
        })
    }
}

impl TryFrom<Expr> for SupportedExpr {
    type Error = ParseError;

    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        match expr {
            Expr::Binary(lhs, op, rhs) => match op {
                op @ Operator::And | op @ Operator::Or => {
                    let lhs: SupportedExpr = (*lhs).try_into()?;
                    let rhs: SupportedExpr = (*rhs).try_into()?;
                    match op {
                        Operator::And => Ok(SupportedExpr::BinaryAnd(Box::new(lhs), Box::new(rhs))),
                        Operator::Or => Ok(SupportedExpr::BinaryOr(Box::new(lhs), Box::new(rhs))),
                        _ => unreachable!(),
                    }
                }
                op => {
                    let lhs: FilterLhs = (*lhs).try_into()?;
                    (*rhs, lhs, op).try_into()
                }
            },
            Expr::Parenthesized(exprs) => Ok(SupportedExpr::Parenthesized(
                exprs
                    .into_iter()
                    .map(|expr| (expr).try_into())
                    .collect::<Result<Vec<SupportedExpr>, Self::Error>>()?,
            )),
            Expr::InList {
                lhs,
                rhs: Some(rhs),
                not,
            } => {
                let lhs: FilterLhs = (*lhs).try_into()?;

                Ok(SupportedExpr::InList {
                    list: rhs
                        .into_iter()
                        .map(|rhs| (rhs, lhs.clone(), Operator::Equals).try_into())
                        .collect::<Result<Vec<_>, Self::Error>>()?,
                    not,
                })
            }
            Expr::IsNull(lhs) => Ok(SupportedExpr::IsNull((*lhs).try_into()?)),
            Expr::NotNull(lhs) => Ok(SupportedExpr::NotNull((*lhs).try_into()?)),
            _ => return Err(ParseError::UnsupportedExpr(expr.clone())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeEvent {
    Insert,
    Update,
    Delete,
}

impl ChangeEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeEvent::Insert => "insert",
            ChangeEvent::Update => "update",
            ChangeEvent::Delete => "delete",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(ChangeEvent::Insert),
            "update" => Some(ChangeEvent::Update),
            "delete" => Some(ChangeEvent::Delete),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Filter {
    expr: SupportedExpr,
}

impl Deref for Filter {
    type Target = SupportedExpr;

    fn deref(&self) -> &Self::Target {
        &self.expr
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateChange<'a> {
    pub version: i64,
    #[serde(rename = "type")]
    pub evt_type: ChangeEvent,
    pub table: &'a str,
    pub pk: PrimaryKey,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub data: HashMap<&'a str, SqliteValueRef<'a>>,
}

// TODO: optimize, sometimes we may be able to get a ref from the values vec
type PrimaryKey = Vec<SqliteValue>;

fn split_pk(pk: &str) -> Vec<String> {
    let mut iter = pk.split('|');

    let mut keys = vec![];
    let mut last_key = String::new();
    while let Some(s) = iter.next() {
        trace!("got {s}, last_key = {last_key}");
        if last_key.is_empty() {
            last_key.push_str(s);
        } else {
            last_key.push('|');
            last_key.push_str(s);
        }
        match unquote(last_key.as_str()) {
            Ok(s) => {
                keys.push(s);
                last_key.clear();
            }
            Err(e) => {
                trace!("could not unquote {s}: {e}");
            }
        }
    }
    if !last_key.is_empty() {
        keys.push(last_key);
    }
    keys
}

impl<'a> AggregateChange<'a> {
    pub fn from_changes(changes: &'a [Change], schema: &Schema, version: i64) -> Vec<Self> {
        let grouped = changes
            .iter()
            .group_by(|change| (change.table.as_str(), change.pk.as_str()));

        grouped
            .into_iter()
            .filter_map(|((table, pk), group)| {
                let mut group = group.peekable();
                let (evt_type, pk) = schema.get(table).and_then(|cols| {
                    group
                        .peek()
                        .and_then(|change| {
                            if change.cid == "__crsql_del" {
                                Some(ChangeEvent::Delete)
                            } else {
                                match cols.iter().find(|col| col.name == change.cid) {
                                    Some(col) => {
                                        if col.primary_key {
                                            // can't change a primary key, so if this is present in the change, it's an insert
                                            Some(ChangeEvent::Insert)
                                        } else {
                                            Some(ChangeEvent::Update)
                                        }
                                    }
                                    None => None,
                                }
                            }
                        })
                        .map(|event| {
                            let pk_cols = cols.iter().filter(|col| col.primary_key);

                            let pk = {
                                let mut pks = split_pk(pk);
                                pk_cols
                                    .filter_map(|col| {
                                        let s = pks.remove(0);
                                        Some(match col.sqlite_type {
                                            Type::Integer => {
                                                dbg!(SqliteValue::Integer(s.parse().ok()?))
                                            }
                                            Type::Real => {
                                                dbg!(SqliteValue::Real(s.parse().ok()?))
                                            }
                                            Type::Text => dbg!(SqliteValue::Text(s)),
                                            Type::Blob => return None,
                                            Type::Null => unreachable!(),
                                        })
                                    })
                                    .collect::<PrimaryKey>()
                            };
                            (event, pk)
                        })
                })?;

                let data = match evt_type {
                    ChangeEvent::Insert | ChangeEvent::Update => group
                        .map(|change| (change.cid.as_str(), change.val.as_ref()))
                        .collect(),
                    _ => HashMap::new(),
                };

                Some(AggregateChange {
                    table,
                    pk,
                    version,
                    evt_type,
                    data,
                })
            })
            .collect::<Vec<AggregateChange>>()
    }
}

pub fn match_expr(expr: &SupportedExpr, agg: &AggregateChange) -> bool {
    match expr {
        SupportedExpr::TableName { name, not_equal } => {
            (*not_equal && name != agg.table) || (!*not_equal && name == agg.table)
        }
        SupportedExpr::EventType { evt, not_equal } => match ChangeEvent::from_str(evt.as_str()) {
            Some(evt) => {
                (*not_equal && evt != agg.evt_type) || (!*not_equal && evt == agg.evt_type)
            }
            None => false,
        },
        SupportedExpr::LiteralInteger {
            table,
            col,
            op,
            lhs,
        } => {
            agg.table == table
                && agg.data.iter().any(|(name, value)| {
                    *name == col && match_lit_value(Some(lhs), *op, value.as_integer())
                })
        }
        SupportedExpr::LiteralReal {
            table,
            col,
            op,
            lhs,
        } => {
            agg.table == table
                && agg.data.iter().any(|(name, value)| {
                    *name == col && match_lit_value(Some(lhs), *op, value.as_real())
                })
        }
        SupportedExpr::LiteralText {
            table,
            col,
            op,
            lhs,
        } => {
            agg.table == table
                && agg.data.iter().any(|(name, value)| {
                    *name == col && match_lit_value(Some(lhs.as_str()), *op, value.as_text())
                })
        }
        SupportedExpr::LiteralBlob {
            table,
            col,
            op,
            lhs,
        } => {
            agg.table == table
                && agg.data.iter().any(|(name, value)| {
                    *name == col && match_lit_value(Some(lhs.as_slice()), *op, value.as_blob())
                })
        }
        SupportedExpr::LiteralNull { table, col, op } => {
            agg.table == table
                && agg.data.iter().any(|(name, value)| {
                    *name == col
                        && match op {
                            NullOp::Is => value.is_null(),
                            NullOp::IsNot => !value.is_null(),
                        }
                })
        }
        SupportedExpr::BinaryAnd(a, b) => {
            match_expr(a.as_ref(), agg) && match_expr(b.as_ref(), agg)
        }
        SupportedExpr::BinaryOr(a, b) => match_expr(a.as_ref(), agg) || match_expr(b.as_ref(), agg),
        SupportedExpr::Parenthesized(exprs) => exprs.iter().any(|expr| match_expr(expr, agg)),
        SupportedExpr::InList { list, not } => {
            let any_matched = list.iter().any(|expr| match_expr(expr, agg));
            (!*not && any_matched) || (*not && !any_matched)
        }
        SupportedExpr::IsNull(lhs) => match lhs {
            FilterLhs::Column { table, name, .. } => {
                agg.table == table
                    && agg
                        .data
                        .iter()
                        .any(|(col, value)| name == *col && matches!(value, SqliteValueRef::Null))
            }
            _ => false,
        },
        SupportedExpr::NotNull(lhs) => match lhs {
            FilterLhs::Column { table, name, .. } => {
                agg.table == table
                    && agg
                        .data
                        .iter()
                        .any(|(col, value)| name == *col && !matches!(value, SqliteValueRef::Null))
            }
            _ => false,
        },
    }
}

fn match_lit_value<T: PartialEq + PartialOrd + ?Sized>(
    rhs: Option<&T>,
    op: BinaryOp,
    lhs: Option<&T>,
) -> bool {
    match op {
        BinaryOp::Equals | BinaryOp::Is => rhs == lhs,
        BinaryOp::NotEquals | BinaryOp::IsNot => rhs != lhs,
        BinaryOp::Greater => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Greater) => true,
            _ => false,
        },
        BinaryOp::GreaterEquals => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Greater | Ordering::Equal) => true,
            _ => false,
        },
        BinaryOp::Less => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Less) => true,
            _ => false,
        },
        BinaryOp::LessEquals => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Less | Ordering::Equal) => true,
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{change::Change, sqlite::prepare_sql};

    use super::*;

    #[test]
    fn kitchen_sink() {
        _ = tracing_subscriber::fmt::try_init();

        let mut schema = Schema::default();

        prepare_sql(
            b"
            CREATE TABLE test (
                id BIGINT NOT NULL,
                b TEXT NOT NULL,
                foo JSON NOT NULL DEFAULT '{}',
                bar JSONB NOT NULL DEFAULT '{}',

                PRIMARY KEY (id, b)
            ) WITHOUT ROWID;
        "
            .as_slice(),
            &mut schema,
        )
        .unwrap();

        let jsonb = hex::encode(br#"{"foo": "bar"}"#);

        let expr = parse_expr(
            format!(r#"(evt_type = 'insert' AND test.id = 1 AND test.foo = '{{"foo": "bar"}}') OR (evt_type = 'update' AND test.bar = x'{jsonb}')"#)
                .as_str(),
        )
        .unwrap();

        println!("expr: {expr:#?}");

        let actor1 = Uuid::new_v4();
        let actor2 = Uuid::new_v4();

        let changes = vec![
            // insert
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "id".into(),
                val: crate::change::SqliteValue::Integer(1),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "b".into(),
                val: crate::change::SqliteValue::Text("hello".into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "foo".into(),
                val: crate::change::SqliteValue::Text(r#"{"foo": "bar"}"#.into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Text("{}".into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            // update
            Change {
                table: "test".into(),
                pk: "'2'|'hello'".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Blob(br#"{"foo": "bar"}"#.to_vec()),
                col_version: 2,
                db_version: 123,
                site_id: actor2.to_bytes_le(),
            },
        ];

        let aggs = AggregateChange::from_changes(changes.as_slice(), &schema, 1);
        println!("aggs: {aggs:#?}");

        for agg in aggs {
            println!("matching on {agg:?} ...");
            if match_expr(&expr, &agg) {
                println!("matched! {agg:?}");
            }

            println!("json: {}", serde_json::to_string_pretty(&agg).unwrap());
        }
    }

    #[test]
    fn unquoting() {
        let pk = "'hell|hell'|'world'";
        let mut iter = pk.split('|');

        let mut keys = vec![];
        let mut last_key = String::new();
        while let Some(s) = iter.next() {
            println!("got {s}, last_key = {last_key}");
            if last_key.is_empty() {
                last_key.push_str(s);
            } else {
                last_key.push('|');
                last_key.push_str(s);
            }
            match unquote(last_key.as_str()) {
                Ok(s) => {
                    keys.push(s);
                    last_key.clear();
                }
                Err(e) => {
                    error!("could not unquote {s}: {e}");
                }
            }
        }
        if !last_key.is_empty() {
            keys.push(last_key);
        }

        println!("keys: {keys:#?}");
    }

    //     #[test]
    //     fn matches_field_value_in() {
    //         _ = tracing_subscriber::fmt::try_init();
    //         let schema = &*SCHEMA;

    //         let mut ctx = Context::new(schema);
    //         ctx.set_field_value("network_id", 12345).unwrap();

    //         let input = format!(
    //             "network_id in {{ {} }}",
    //             (1..20000)
    //                 .map(|i| i.to_string())
    //                 .collect::<Vec<String>>()
    //                 .join(" ")
    //         );
    //         let expr = parse_expr(&input).unwrap();
    //         assert!(match_expr(&input, &expr, &ctx));
    //     }

    //     #[test]
    //     fn matches_complex_or() {
    //         _ = tracing_subscriber::fmt::try_init();
    //         let schema = &*SCHEMA;

    //         let mut ctx = Context::new(schema);
    //         ctx.set_field_value("record.type", RecordType::ConsulService)
    //             .unwrap();
    //         ctx.set_field_value("app_id", 2).unwrap();

    //         let input = "((record.type eq consul_service) or (record.type eq ip_assignment)) and app_id in { 1 2 3 }";
    //         let expr = parse_expr(input).unwrap();

    //         assert!(match_expr(input, &expr, &ctx));
    // }
}
