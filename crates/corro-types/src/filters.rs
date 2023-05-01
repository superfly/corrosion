use std::{cmp::Ordering, collections::HashMap, ops::Deref};

use enquote::unquote;
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use rusqlite::types::Type;
// pub use exprt::parser::ast::Expr;
// use exprt::{
//     parser::{
//         ast::{Atom, ExprKind, Field, FunctionCall, Index, Indexed},
//         tokenizer::Span,
//     },
//     typecheck::{
//         schema::{FieldDef, Schema},
//         typecheck::Type,
//     },
// };
use sqlite3_parser::{
    ast::{Cmd, Expr, Literal, OneSelect, Operator, Stmt},
    lexer::sql::Parser,
};
use tracing::{debug, error, trace, warn};

use crate::change::{Change, SqliteValueRef};

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
}

#[derive(Debug)]
pub enum SupportedExpr {
    TableName {
        name: String,
        not_equal: bool,
    },
    EventType {
        evt: ChangeEvent,
        not_equal: bool,
    },

    LiteralInteger(BinaryOp, ColumnLit<i64>),
    LiteralReal(BinaryOp, ColumnLit<f64>),
    LiteralText(BinaryOp, ColumnLit<String>),
    LiteralBlob(BinaryOp, ColumnLit<Vec<u8>>),

    // BinaryLiteral(FilterLhs, BinaryOp, FilterRhs),
    BinaryAnd(Box<SupportedExpr>, Box<SupportedExpr>),
    BinaryOr(Box<SupportedExpr>, Box<SupportedExpr>),
    Parenthesized(Vec<SupportedExpr>),

    ListOfInteger {
        table: String,
        name: String,
        list: Vec<RhsType<i64>>,
        not: bool,
    },
    ListOfReal {
        table: String,
        name: String,
        list: Vec<RhsType<f64>>,
        not: bool,
    },
    ListOfText {
        table: String,
        name: String,
        list: Vec<RhsType<String>>,
        not: bool,
    },
    ListOfBlob {
        table: String,
        name: String,
        list: Vec<RhsType<Vec<u8>>>,
        not: bool,
    },

    IsNull(FilterLhs),
    NotNull(FilterLhs),
}

#[derive(Debug)]
pub enum SupportedRhsLiteral {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct ColumnLit<Rhs> {
    table: String,
    name: String,
    rhs: RhsType<Rhs>,
}

impl<T> Deref for ColumnLit<T> {
    type Target = RhsType<T>;

    fn deref(&self) -> &Self::Target {
        &self.rhs
    }
}

#[derive(Debug)]
pub enum RhsType<T> {
    Nullable(Option<T>),
    NotNullable(T),
}

impl<T> RhsType<T> {
    pub fn is_nullable(&self) -> bool {
        match self {
            RhsType::Nullable(_) => true,
            RhsType::NotNullable(_) => false,
        }
    }
    pub fn as_ref(&self) -> Option<&T> {
        match self {
            RhsType::Nullable(t) => t.as_ref(),
            RhsType::NotNullable(ref t) => Some(t),
        }
    }
}

impl<T: Deref> RhsType<T> {
    pub fn as_deref(&self) -> Option<&T::Target> {
        match self {
            RhsType::Nullable(t) => t.as_deref(),
            RhsType::NotNullable(t) => Some(t.deref()),
        }
    }
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
    Column {
        table: String,
        name: String,
        sqlite_type: Type,
        nullable: bool,
    },
    TableName,
    EventType,
}

#[derive(Debug)]
pub enum FilterRhs {
    Literal(SupportedRhsLiteral),
    TableName(String),
    EventType(ChangeEvent),
}

pub fn parse_filter(schema: &Schema, input: &str) -> Result<Filter, ParseError> {
    // kind of a hack...
    let input = format!("SELECT dummy FROM dummy WHERE {input}");

    let mut parser = Parser::new(input.as_bytes());

    // only parse the first statement
    let cmd = parser.next()?.unwrap();

    let filter = match cmd {
        Cmd::Stmt(stmt) => match stmt {
            Stmt::Select(select) => match select.body.select {
                OneSelect::Select {
                    where_clause: Some(where_clause),
                    ..
                } => Filter {
                    expr: (where_clause, schema).try_into()?,
                },
                select => return Err(ParseError::UnsupportedSelect(select)),
            },
            stmt => return Err(ParseError::UnsupportedStmt(stmt)),
        },
        cmd => return Err(ParseError::UnsupportedCmd(cmd)),
    };

    Ok(filter)
}

impl TryFrom<(Expr, &Schema)> for FilterLhs {
    type Error = ParseError;

    fn try_from((lhs, schema): (Expr, &Schema)) -> Result<Self, Self::Error> {
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
            Expr::Qualified(tbl_name, name) => {
                if let Some(cols) = schema.get(tbl_name.0.as_str()) {
                    if let Some(col) = cols.iter().find(|col| col.name == name.0) {
                        Ok(FilterLhs::Column {
                            table: tbl_name.0.clone(),
                            name: name.0.clone(),
                            sqlite_type: col.sqlite_type.clone(),
                            nullable: col.nullable,
                        })
                    } else {
                        Err(ParseError::InvalidColumn(
                            tbl_name.0.clone(),
                            name.0.clone(),
                        ))
                    }
                } else {
                    Err(ParseError::UnknownTable(tbl_name.0.clone()))
                }
            }
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

impl TryFrom<(Expr, &Schema, FilterLhs, Operator)> for SupportedExpr {
    type Error = ParseError;

    fn try_from(
        (rhs, schema, lhs, op): (Expr, &Schema, FilterLhs, Operator),
    ) -> Result<Self, Self::Error> {
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

                        match schema.get(&name) {
                            Some(_) => Ok(SupportedExpr::TableName { name, not_equal }),
                            None => Err(ParseError::UnknownTable(name)),
                        }
                    }
                    (FilterLhs::TableName, _, _) => Err(ParseError::WrongLiteralType),
                    (FilterLhs::EventType, op, SupportedRhsLiteral::Text(s)) => {
                        let not_equal = match op {
                            Operator::Equals | Operator::Is => false,
                            Operator::NotEquals | Operator::IsNot => true,
                            _ => return Err(ParseError::UnsupportedBinaryOp(op)),
                        };
                        Ok(SupportedExpr::EventType {
                            evt: ChangeEvent::from_str(s.as_str())
                                .ok_or(ParseError::UnknownEvent(s))?,
                            not_equal,
                        })
                    }
                    (FilterLhs::EventType, _, _) => Err(ParseError::WrongLiteralType),
                    (
                        FilterLhs::Column {
                            nullable: false, ..
                        },
                        _,
                        SupportedRhsLiteral::Null,
                    ) => Err(ParseError::WrongLiteralType),
                    (
                        FilterLhs::Column {
                            nullable: true,
                            sqlite_type,
                            table,
                            name,
                        },
                        op,
                        rhs @ SupportedRhsLiteral::Null,
                    ) => {
                        let op = op.try_into()?;
                        let expr = match sqlite_type {
                            Type::Integer => SupportedExpr::LiteralInteger(
                                op,
                                ColumnLit {
                                    table,
                                    name,
                                    rhs: RhsType::Nullable(None),
                                },
                            ),
                            Type::Real => SupportedExpr::LiteralReal(
                                op,
                                ColumnLit {
                                    table,
                                    name,
                                    rhs: RhsType::Nullable(None),
                                },
                            ),
                            Type::Text => SupportedExpr::LiteralText(
                                op,
                                ColumnLit {
                                    table,
                                    name,
                                    rhs: RhsType::Nullable(None),
                                },
                            ),
                            Type::Blob => SupportedExpr::LiteralBlob(
                                op,
                                ColumnLit {
                                    table,
                                    name,
                                    rhs: RhsType::Nullable(None),
                                },
                            ),
                            _ => unreachable!(),
                        };
                        Ok(expr)
                    }
                    (
                        FilterLhs::Column {
                            nullable,
                            sqlite_type,
                            table,
                            name,
                        },
                        op,
                        rhs,
                    ) => {
                        let op = op.try_into()?;
                        let expr = match (sqlite_type, rhs) {
                            (Type::Integer, SupportedRhsLiteral::Integer(i)) => {
                                SupportedExpr::LiteralInteger(
                                    op,
                                    ColumnLit {
                                        table,
                                        name,
                                        rhs: if nullable {
                                            RhsType::Nullable(Some(i))
                                        } else {
                                            RhsType::NotNullable(i)
                                        },
                                    },
                                )
                            }
                            (Type::Real, SupportedRhsLiteral::Real(f)) => {
                                SupportedExpr::LiteralReal(
                                    op,
                                    ColumnLit {
                                        table,
                                        name,
                                        rhs: if nullable {
                                            RhsType::Nullable(Some(f))
                                        } else {
                                            RhsType::NotNullable(f)
                                        },
                                    },
                                )
                            }
                            (Type::Text, SupportedRhsLiteral::Text(s)) => {
                                SupportedExpr::LiteralText(
                                    op,
                                    ColumnLit {
                                        table,
                                        name,
                                        rhs: if nullable {
                                            RhsType::Nullable(Some(s))
                                        } else {
                                            RhsType::NotNullable(s)
                                        },
                                    },
                                )
                            }
                            (Type::Blob, SupportedRhsLiteral::Blob(b)) => {
                                SupportedExpr::LiteralBlob(
                                    op,
                                    ColumnLit {
                                        table,
                                        name,
                                        rhs: if nullable {
                                            RhsType::Nullable(Some(b))
                                        } else {
                                            RhsType::NotNullable(b)
                                        },
                                    },
                                )
                            }
                            _ => unreachable!(),
                        };
                        Ok(expr)
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

impl TryFrom<(Expr, &FilterLhs)> for ColumnLit<i64> {
    type Error = ParseError;

    fn try_from((rhs, lhs): (Expr, &FilterLhs)) -> Result<Self, Self::Error> {
        match (rhs, lhs) {
            (Expr::Literal(lit), lhs) => match (lit.try_into()?, lhs) {
                (
                    SupportedRhsLiteral::Integer(i),
                    FilterLhs::Column {
                        table,
                        name,
                        sqlite_type: Type::Integer,
                        nullable,
                    },
                ) => Ok(ColumnLit {
                    table: table.clone(),
                    name: name.clone(),
                    rhs: (i, *nullable).into(),
                }),
                // FIXME: that's the wrong error
                _ => Err(ParseError::ExpectedLiteral),
            },
            _ => Err(ParseError::ExpectedLiteral),
        }
    }
}

impl TryFrom<(Expr, &FilterLhs)> for ColumnLit<f64> {
    type Error = ParseError;

    fn try_from((rhs, lhs): (Expr, &FilterLhs)) -> Result<Self, Self::Error> {
        match (rhs, lhs) {
            (Expr::Literal(lit), lhs) => match (lit.try_into()?, lhs) {
                (
                    SupportedRhsLiteral::Real(i),
                    FilterLhs::Column {
                        table,
                        name,
                        sqlite_type: Type::Real,
                        nullable,
                    },
                ) => Ok(ColumnLit {
                    table: table.clone(),
                    name: name.clone(),
                    rhs: (i, *nullable).into(),
                }),
                // FIXME: that's the wrong error
                _ => Err(ParseError::ExpectedLiteral),
            },
            _ => Err(ParseError::ExpectedLiteral),
        }
    }
}

impl TryFrom<(Expr, &FilterLhs)> for ColumnLit<String> {
    type Error = ParseError;

    fn try_from((rhs, lhs): (Expr, &FilterLhs)) -> Result<Self, Self::Error> {
        match (rhs, lhs) {
            (Expr::Literal(lit), lhs) => match (lit.try_into()?, lhs) {
                (
                    SupportedRhsLiteral::Text(i),
                    FilterLhs::Column {
                        table,
                        name,
                        sqlite_type: Type::Text,
                        nullable,
                    },
                ) => Ok(ColumnLit {
                    table: table.clone(),
                    name: name.clone(),
                    rhs: (i, *nullable).into(),
                }),
                // FIXME: that's the wrong error
                _ => Err(ParseError::ExpectedLiteral),
            },
            _ => Err(ParseError::ExpectedLiteral),
        }
    }
}

impl TryFrom<(Expr, &FilterLhs)> for ColumnLit<Vec<u8>> {
    type Error = ParseError;

    fn try_from((rhs, lhs): (Expr, &FilterLhs)) -> Result<Self, Self::Error> {
        match (rhs, lhs) {
            (Expr::Literal(lit), lhs) => match (lit.try_into()?, lhs) {
                (
                    SupportedRhsLiteral::Blob(i),
                    FilterLhs::Column {
                        table,
                        name,
                        sqlite_type: Type::Blob,
                        nullable,
                    },
                ) => Ok(ColumnLit {
                    table: table.clone(),
                    name: name.clone(),
                    rhs: (i, *nullable).into(),
                }),
                // FIXME: that's the wrong error
                _ => Err(ParseError::ExpectedLiteral),
            },
            _ => Err(ParseError::ExpectedLiteral),
        }
    }
}

impl From<(i64, bool)> for RhsType<i64> {
    fn from((i, nullable): (i64, bool)) -> Self {
        if nullable {
            RhsType::Nullable(Some(i))
        } else {
            RhsType::NotNullable(i)
        }
    }
}

impl From<(String, bool)> for RhsType<String> {
    fn from((i, nullable): (String, bool)) -> Self {
        if nullable {
            RhsType::Nullable(Some(i))
        } else {
            RhsType::NotNullable(i)
        }
    }
}

impl From<(f64, bool)> for RhsType<f64> {
    fn from((i, nullable): (f64, bool)) -> Self {
        if nullable {
            RhsType::Nullable(Some(i))
        } else {
            RhsType::NotNullable(i)
        }
    }
}

impl From<(Vec<u8>, bool)> for RhsType<Vec<u8>> {
    fn from((i, nullable): (Vec<u8>, bool)) -> Self {
        if nullable {
            RhsType::Nullable(Some(i))
        } else {
            RhsType::NotNullable(i)
        }
    }
}

impl TryFrom<(Expr, &Schema)> for SupportedExpr {
    type Error = ParseError;

    fn try_from((expr, schema): (Expr, &Schema)) -> Result<Self, Self::Error> {
        match expr {
            Expr::Binary(lhs, op, rhs) => match op {
                op @ Operator::And | op @ Operator::Or => {
                    let lhs: SupportedExpr = (*lhs, schema).try_into()?;
                    let rhs: SupportedExpr = (*rhs, schema).try_into()?;
                    match op {
                        Operator::And => Ok(SupportedExpr::BinaryAnd(Box::new(lhs), Box::new(rhs))),
                        Operator::Or => Ok(SupportedExpr::BinaryOr(Box::new(lhs), Box::new(rhs))),
                        _ => unreachable!(),
                    }
                }
                op => {
                    let lhs: FilterLhs = (*lhs, schema).try_into()?;
                    (*rhs, schema, lhs, op).try_into()
                }
            },
            Expr::Parenthesized(exprs) => Ok(SupportedExpr::Parenthesized(
                exprs
                    .into_iter()
                    .map(|expr| (expr, schema).try_into())
                    .collect::<Result<Vec<SupportedExpr>, Self::Error>>()?,
            )),
            Expr::InList {
                ref lhs,
                rhs: Some(ref rhs),
                not,
            } => {
                let lhs: FilterLhs = (*lhs.clone(), schema).try_into()?;

                match &lhs {
                    lhs @ FilterLhs::Column {
                        table,
                        name,
                        sqlite_type,
                        nullable,
                    } => match sqlite_type {
                        Type::Integer => Ok(SupportedExpr::ListOfInteger {
                            table: table.clone(),
                            name: name.clone(),
                            list: rhs
                                .iter()
                                .map(|rhs| {
                                    (rhs.clone(), lhs)
                                        .try_into()
                                        .map(|lit: ColumnLit<_>| lit.rhs)
                                })
                                .collect::<Result<Vec<_>, Self::Error>>()?,
                            not,
                        }),
                        Type::Real => Ok(SupportedExpr::ListOfReal {
                            table: table.clone(),
                            name: name.clone(),
                            list: rhs
                                .iter()
                                .map(|rhs| {
                                    (rhs.clone(), lhs)
                                        .try_into()
                                        .map(|lit: ColumnLit<_>| lit.rhs)
                                })
                                .collect::<Result<Vec<_>, Self::Error>>()?,
                            not,
                        }),
                        Type::Text => Ok(SupportedExpr::ListOfText {
                            table: table.clone(),
                            name: name.clone(),
                            list: rhs
                                .iter()
                                .map(|rhs| {
                                    (rhs.clone(), lhs)
                                        .try_into()
                                        .map(|lit: ColumnLit<_>| lit.rhs)
                                })
                                .collect::<Result<Vec<_>, Self::Error>>()?,
                            not,
                        }),
                        Type::Blob => Ok(SupportedExpr::ListOfBlob {
                            table: table.clone(),
                            name: name.clone(),
                            list: rhs
                                .iter()
                                .map(|rhs| {
                                    (rhs.clone(), lhs)
                                        .try_into()
                                        .map(|lit: ColumnLit<_>| lit.rhs)
                                })
                                .collect::<Result<Vec<_>, Self::Error>>()?,
                            not,
                        }),
                        Type::Null => unreachable!(),
                    },
                    _ => return Err(ParseError::UnsupportedExpr(expr.clone())),
                }
            }
            Expr::IsNull(lhs) => {
                let lhs: FilterLhs = (*lhs, schema).try_into()?;
                if let FilterLhs::Column { nullable: true, .. } = &lhs {
                    Ok(SupportedExpr::IsNull(lhs))
                } else {
                    Err(ParseError::ColumnIsNotNullable)
                }
            }
            Expr::NotNull(lhs) => {
                let lhs: FilterLhs = (*lhs, schema).try_into()?;
                if let FilterLhs::Column { nullable: true, .. } = &lhs {
                    Ok(SupportedExpr::NotNull(lhs))
                } else {
                    Err(ParseError::ColumnIsNotNullable)
                }
            }
            _ => return Err(ParseError::UnsupportedExpr(expr.clone())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeEvent {
    Insert,
    Update,
    Delete,
}

impl ChangeEvent {
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

#[derive(Debug)]
pub enum AggregateChange<'a> {
    Insert {
        table: &'a str,
        pk: &'a str,
        values: Vec<(&'a str, SqliteValueRef<'a>)>,
    },
    Update {
        table: &'a str,
        pk: &'a str,
        values: Vec<(&'a str, SqliteValueRef<'a>)>,
    },
    Delete {
        table: &'a str,
        pk: &'a str,
    },
}

impl<'a> AggregateChange<'a> {
    pub fn from_changes(changes: &'a [Change], schema: &Schema) -> Vec<Self> {
        let grouped = changes
            .iter()
            .group_by(|change| (change.table.as_str(), change.pk.as_str()));

        grouped
            .into_iter()
            .filter_map(|((table, pk), group)| {
                let mut group = group.peekable();
                let event = schema.get(table).and_then(|cols| {
                    group.peek().and_then(|change| {
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
                })?;

                Some(match event {
                    ChangeEvent::Insert => AggregateChange::Insert {
                        table,
                        pk,
                        values: group
                            .map(|change| (change.cid.as_str(), change.val.as_ref()))
                            .collect(),
                    },
                    ChangeEvent::Update => AggregateChange::Update {
                        table,
                        pk,
                        values: group
                            .map(|change| (change.cid.as_str(), change.val.as_ref()))
                            .collect(),
                    },
                    ChangeEvent::Delete => AggregateChange::Delete { table, pk },
                })
            })
            .collect::<Vec<AggregateChange>>()
    }

    pub fn table(&self) -> &str {
        match self {
            AggregateChange::Insert { table, .. } => table,
            AggregateChange::Update { table, .. } => table,
            AggregateChange::Delete { table, .. } => table,
        }
    }

    pub fn as_change_event(&self) -> ChangeEvent {
        match self {
            AggregateChange::Insert { .. } => ChangeEvent::Insert,
            AggregateChange::Update { .. } => ChangeEvent::Update,
            AggregateChange::Delete { .. } => ChangeEvent::Delete,
        }
    }

    pub fn values(&self) -> Option<&Vec<(&'a str, SqliteValueRef)>> {
        match self {
            AggregateChange::Insert { values, .. } => Some(values),
            AggregateChange::Update { values, .. } => Some(values),
            AggregateChange::Delete { .. } => None,
        }
    }
}

fn matching_fun(expr: &SupportedExpr, agg: &AggregateChange) -> bool {
    match expr {
        SupportedExpr::TableName { name, not_equal } => {
            (*not_equal && name != agg.table()) || (!*not_equal && name == agg.table())
        }
        SupportedExpr::EventType { evt, not_equal } => {
            (*not_equal && *evt != agg.as_change_event())
                || (!*not_equal && *evt == agg.as_change_event())
        }
        SupportedExpr::LiteralInteger(op, ColumnLit { table, name, rhs }) => match agg.values() {
            Some(values) => {
                agg.table() == table
                    && values.iter().any(|(col, value)| {
                        name == *col && match_lit_value(rhs.as_ref(), *op, value.as_integer())
                    })
            }
            None => false,
        },
        SupportedExpr::LiteralReal(op, ColumnLit { table, name, rhs }) => match agg.values() {
            Some(values) => {
                agg.table() == table
                    && values.iter().any(|(col, value)| {
                        name == *col && match_lit_value(rhs.as_ref(), *op, value.as_real())
                    })
            }
            None => false,
        },
        SupportedExpr::LiteralText(op, ColumnLit { table, name, rhs }) => match agg.values() {
            Some(values) => {
                agg.table() == table
                    && values.iter().any(|(col, value)| {
                        name == *col && match_lit_value(rhs.as_deref(), *op, value.as_text())
                    })
            }
            None => false,
        },
        SupportedExpr::LiteralBlob(op, ColumnLit { table, name, rhs }) => match agg.values() {
            Some(values) => {
                agg.table() == table
                    && values.iter().any(|(col, value)| {
                        name == *col && match_lit_value(rhs.as_deref(), *op, value.as_blob())
                    })
            }
            None => false,
        },
        SupportedExpr::BinaryAnd(a, b) => {
            matching_fun(a.as_ref(), agg) && matching_fun(b.as_ref(), agg)
        }
        SupportedExpr::BinaryOr(a, b) => {
            matching_fun(a.as_ref(), agg) || matching_fun(b.as_ref(), agg)
        }
        SupportedExpr::Parenthesized(exprs) => exprs.iter().any(|expr| matching_fun(expr, agg)),
        SupportedExpr::ListOfInteger {
            table,
            name,
            list,
            not,
        } => {
            agg.table() == table
                && match agg.values() {
                    Some(values) => values.iter().any(|(col, value)| {
                        name == *col && {
                            let any_matched = list.iter().any(|lit| {
                                match_lit_value(lit.as_ref(), BinaryOp::Equals, value.as_integer())
                            });
                            (!*not && any_matched) || (*not && !any_matched)
                        }
                    }),
                    None => false,
                }
        }
        SupportedExpr::ListOfReal {
            table,
            name,
            list,
            not,
        } => {
            agg.table() == table
                && match agg.values() {
                    Some(values) => values.iter().any(|(col, value)| {
                        name == *col && {
                            let any_matched = list.iter().any(|lit| {
                                match_lit_value(lit.as_ref(), BinaryOp::Equals, value.as_real())
                            });
                            (!*not && any_matched) || (*not && !any_matched)
                        }
                    }),
                    None => false,
                }
        }
        SupportedExpr::ListOfText {
            table,
            name,
            list,
            not,
        } => {
            agg.table() == table
                && match agg.values() {
                    Some(values) => values.iter().any(|(col, value)| {
                        name == *col && {
                            let any_matched = list.iter().any(|lit| {
                                match_lit_value(lit.as_deref(), BinaryOp::Equals, value.as_text())
                            });
                            (!*not && any_matched) || (*not && !any_matched)
                        }
                    }),
                    None => false,
                }
        }
        SupportedExpr::ListOfBlob {
            table,
            name,
            list,
            not,
        } => {
            agg.table() == table
                && match agg.values() {
                    Some(values) => values.iter().any(|(col, value)| {
                        name == *col && {
                            let any_matched = list.iter().any(|lit| {
                                match_lit_value(lit.as_deref(), BinaryOp::Equals, value.as_blob())
                            });
                            (!*not && any_matched) || (*not && !any_matched)
                        }
                    }),
                    None => false,
                }
        }
        SupportedExpr::IsNull(_) => todo!(),
        SupportedExpr::NotNull(_) => todo!(),
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
            Some(Ordering::Less) => true,
            _ => false,
        },
        BinaryOp::GreaterEquals => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Less | Ordering::Equal) => true,
            _ => false,
        },
        BinaryOp::Less => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Greater) => true,
            _ => false,
        },
        BinaryOp::LessEquals => match rhs.partial_cmp(&lhs) {
            Some(Ordering::Greater | Ordering::Equal) => true,
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    //     use crate::pubsub::SCHEMA;

    use itertools::Itertools;
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
                id BIGINT PRIMARY KEY NOT NULL,
                foo JSON NOT NULL DEFAULT '{}',
                bar JSONB NOT NULL DEFAULT '{}'
            ) WITHOUT ROWID;
        "
            .as_slice(),
            &mut schema,
        )
        .unwrap();

        let jsonb = hex::encode(br#"{"foo": "bar"}"#);

        let filter = parse_filter(
            &schema,
            format!(r#"(evt_type = 'insert' AND test.id = 1 AND test.foo = '{{"foo": "bar"}}') OR (evt_type = 'update' AND test.bar = x'{jsonb}')"#)
                .as_str(),
        )
        .unwrap();

        println!("filter: {filter:#?}");

        let actor1 = Uuid::new_v4();
        let actor2 = Uuid::new_v4();

        let changes = vec![
            // insert
            Change {
                table: "test".into(),
                pk: "1".into(),
                cid: "id".into(),
                val: crate::change::SqliteValue::Integer(1),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            Change {
                table: "test".into(),
                pk: "1".into(),
                cid: "foo".into(),
                val: crate::change::SqliteValue::Text(r#"{"foo": "bar"}"#.into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            Change {
                table: "test".into(),
                pk: "1".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Text("{}".into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.to_bytes_le(),
            },
            // update
            Change {
                table: "test".into(),
                pk: "2".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Blob(br#"{"foo": "bar"}"#.to_vec()),
                col_version: 2,
                db_version: 123,
                site_id: actor2.to_bytes_le(),
            },
        ];

        let aggs = AggregateChange::from_changes(changes.as_slice(), &schema);
        println!("aggs: {aggs:#?}");

        for agg in aggs {
            println!("matching on {agg:?} ...");
            if matching_fun(&filter.expr, &agg) {
                println!("matched! {agg:?}");
            }
        }
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
