use std::{cmp::Ordering, collections::HashMap, ops::Deref};

use enquote::unquote;
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlite3_parser::{
    ast::{Cmd, Expr, Literal, OneSelect, Operator, Stmt},
    lexer::sql::Parser,
};
use tracing::error;

use crate::{
    change::{Change, SqliteValue},
    sqlite::{NormalizedSchema, Type},
};

const CORRO_EVENT: &str = "evt_type";
const CORRO_TABLE: &str = "tbl_name";

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: Type,
    pub primary_key: bool,
    pub nullable: bool,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregateChange<'a> {
    pub version: i64,
    pub table: &'a str,
    pub pk: PrimaryKey<'a>,
    #[serde(rename = "type")]
    pub evt_type: ChangeEvent,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub data: HashMap<&'a str, SqliteValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OwnedAggregateChange {
    pub version: i64,
    pub evt_type: ChangeEvent,
    pub table: String,
    pub pk: OwnedPrimaryKey,
    pub data: HashMap<String, SqliteValue>,
}

type PrimaryKey<'a> = HashMap<&'a str, SqliteValue>;
type OwnedPrimaryKey = HashMap<String, SqliteValue>;

fn split_pk(pk: &str) -> Vec<SqliteValue> {
    let mut iter = pk.split('|').peekable();

    let mut keys = vec![];

    let mut last_key = String::new();
    while let Some(s) = iter.next() {
        if last_key.is_empty() {
            last_key.push_str(s);
        } else {
            last_key.push('|');
            last_key.push_str(s);
        }
        if let Some(key) = parse_sqlite_quoted_str(last_key.as_str()) {
            keys.push(key);
            last_key.clear();
        }
    }
    if !last_key.is_empty() {
        if let Some(key) = parse_sqlite_quoted_str(last_key.as_str()) {
            keys.push(key)
        } else {
            error!("unparsable str for sql value: {last_key}");
        }
    }
    keys
}

impl<'a> AggregateChange<'a> {
    pub fn to_owned(&self) -> OwnedAggregateChange {
        OwnedAggregateChange {
            version: self.version,
            evt_type: self.evt_type,
            table: self.table.to_owned(),
            pk: self
                .pk
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
            data: self
                .data
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.to_owned()))
                .collect(),
        }
    }

    pub fn from_changes<I: Iterator<Item = &'a Change>>(
        changes: I,
        schema: &'a NormalizedSchema,
        version: i64,
    ) -> Vec<Self> {
        let grouped = changes.group_by(|change| (change.table.as_str(), change.pk.as_str()));

        grouped
            .into_iter()
            .filter_map(|((table, pk), group)| {
                schema.tables.get(table).and_then(|schema_table| {
                    let pk: PrimaryKey = {
                        let pk = split_pk(pk);
                        schema_table
                            .pk
                            .iter()
                            .enumerate()
                            .filter_map(|(i, pk_name)| {
                                pk.get(i).map(|v| (pk_name.as_str(), v.clone()))
                            })
                            .collect()
                    };

                    let mut group = group.peekable();
                    let change_event = group.peek().map(|change| {
                        if change.cid == "__crsql_del" {
                            ChangeEvent::Delete
                        } else {
                            if change.col_version == 1 {
                                ChangeEvent::Insert
                            } else {
                                ChangeEvent::Update
                            }
                        }
                    })?;

                    Some(AggregateChange {
                        version,
                        table,
                        pk,
                        evt_type: change_event,
                        data: match change_event {
                            ChangeEvent::Insert | ChangeEvent::Update => group
                                .map(|change| {
                                    (
                                        change.cid.as_str(),
                                        match &change.val {
                                            v @ SqliteValue::Text(ref input) => {
                                                match parse_sqlite_quoted_str(input) {
                                                    Some(v) => v,
                                                    None => v.clone(),
                                                }
                                            }
                                            v => v.clone(),
                                        },
                                    )
                                })
                                .collect(),
                            ChangeEvent::Delete => HashMap::new(),
                        },
                    })
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
                        .any(|(col, value)| name == *col && value.is_null())
            }
            _ => false,
        },
        SupportedExpr::NotNull(lhs) => match lhs {
            FilterLhs::Column { table, name, .. } => {
                agg.table == table
                    && agg
                        .data
                        .iter()
                        .any(|(col, value)| name == *col && !value.is_null())
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

pub fn parse_sqlite_quoted_str(input: &str) -> Option<SqliteValue> {
    use nom::{
        bytes::complete::{escaped_transform, tag, take_while},
        character::complete::none_of,
        combinator::{map_parser, map_res, recognize},
        multi::{many0, separated_list0},
        sequence::delimited,
        AsChar, IResult,
    };

    fn parse_quoted_blob(input: &str) -> IResult<&str, Vec<u8>> {
        let decoder = map_res(take_while(AsChar::is_hex_digit), hex::decode);
        delimited(tag("X'"), decoder, tag("'"))(input)
    }

    fn parse_quoted_text(input: &str) -> IResult<&str, String> {
        let seq = recognize(separated_list0(tag("''"), many0(none_of("'"))));
        let unquote = escaped_transform(none_of("'"), '\'', tag("'"));
        let res = delimited(tag("'"), map_parser(seq, unquote), tag("'"))(input)?;

        Ok(res)
    }

    Some(if let Ok(i) = input.parse::<i64>() {
        SqliteValue::Integer(i)
    } else if let Ok(f) = input.parse::<f64>() {
        SqliteValue::Real(f)
    } else if input == "NULL" {
        SqliteValue::Null
    } else if let Ok((_, v)) = parse_quoted_blob(input) {
        SqliteValue::Blob(v)
    } else {
        return parse_quoted_text(input)
            .map(|(_, s)| SqliteValue::Text(s))
            .ok();
    })
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use crate::{change::Change, sqlite::parse_sql};

    use super::*;

    #[test]
    fn kitchen_sink() {
        _ = tracing_subscriber::fmt::try_init();

        let schema = parse_sql(
            "
            CREATE TABLE test (
                id BIGINT NOT NULL,
                b TEXT NOT NULL,
                foo JSON NOT NULL DEFAULT '{}',
                bar JSONB NOT NULL DEFAULT '{}',

                PRIMARY KEY (id, b)
            ) WITHOUT ROWID;
        ",
        )
        .unwrap();

        let jsonb = hex::encode(br#"{"foo": "bar"}"#);

        let expr = parse_expr(
            format!(r#"(evt_type = 'insert' AND test.id = 1 AND test.foo = '{{"foo": "bar"}}') OR (evt_type = 'update' AND test.bar = x'{jsonb}')"#)
                .as_str(),
        )
        .unwrap();

        println!("expr: {expr:#?}");

        let actor1 = Ulid::new();
        let actor2 = Ulid::new();

        let changes = vec![
            // insert
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "id".into(),
                val: crate::change::SqliteValue::Integer(1),
                col_version: 1,
                db_version: 123,
                site_id: actor1.0.to_be_bytes(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "b".into(),
                val: crate::change::SqliteValue::Text("hello".into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.0.to_be_bytes(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "foo".into(),
                val: crate::change::SqliteValue::Text(r#"{"foo": "bar"}"#.into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.0.to_be_bytes(),
            },
            Change {
                table: "test".into(),
                pk: "'1'|'hello'".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Text("{}".into()),
                col_version: 1,
                db_version: 123,
                site_id: actor1.0.to_be_bytes(),
            },
            // update
            Change {
                table: "test".into(),
                pk: "'2'|'hello'".into(),
                cid: "bar".into(),
                val: crate::change::SqliteValue::Blob(br#"{"foo": "bar"}"#.to_vec()),
                col_version: 2,
                db_version: 123,
                site_id: actor2.0.to_be_bytes(),
            },
        ];

        let aggs = AggregateChange::from_changes(changes.iter(), &schema, 1);
        println!("aggs: {aggs:#?}");

        for agg in aggs {
            println!("matching on {agg:?} ...");
            if match_expr(&expr, &agg) {
                println!("matched! {agg:?}");
            }

            println!(
                "json: {}",
                serde_json::to_string_pretty(&agg.to_owned()).unwrap()
            );
        }
    }

    #[test]
    fn unquoting() {
        let pk = "'hell|hell'|1.2345|NULL|'world'|X'010203'|123456";

        assert_eq!(
            dbg!(split_pk(pk)),
            vec![
                SqliteValue::Text("hell|hell".to_owned()),
                SqliteValue::Real(1.2345),
                SqliteValue::Null,
                SqliteValue::Text("world".into()),
                SqliteValue::Blob(vec![1, 2, 3]),
                SqliteValue::Integer(123456),
            ],
        );

        let tests = vec!["'mystring'", "123456", "1.23456", "X'010203'", "NULL"];

        let parsed = tests
            .into_iter()
            .filter_map(|s| parse_sqlite_quoted_str(s))
            .collect::<Vec<_>>();

        println!("parsed: {parsed:?}");

        assert_eq!(
            parsed,
            vec![
                SqliteValue::Text("mystring".into()),
                SqliteValue::Integer(123456),
                SqliteValue::Real(1.23456),
                SqliteValue::Blob(vec![1, 2, 3]),
                SqliteValue::Null
            ]
        );
    }
}
