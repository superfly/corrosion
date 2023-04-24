use std::{
    net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr},
    num::ParseIntError,
    ops::RangeInclusive,
};

pub use exprt::parser::ast::Expr;
use exprt::{
    parser::{
        ast::{Atom, ExprKind, Field, FunctionCall, Index, Indexed},
        tokenizer::Span,
    },
    typecheck::{
        schema::{FieldDef, Schema},
        typecheck::Type,
    },
};
use tracing::{debug, trace, warn};

#[derive(Debug, Clone)]
pub enum FilterValue<'v> {
    Ip(IpAddr),
    String(&'v str),
    Integer(i64),
    Bool(bool),
}

impl<'v> FilterValue<'v> {
    fn get_type(&self) -> Type {
        match self {
            FilterValue::Ip(ip) => match ip {
                IpAddr::V4(_) => Type::Ipv4,
                IpAddr::V6(_) => Type::Ipv6,
            },
            FilterValue::String(_) => Type::String,
            FilterValue::Integer(_) => Type::Integer,
            FilterValue::Bool(_) => Type::Bool,
        }
    }
    fn is_type(&self, t: &Type) -> bool {
        match t {
            t if !t.is_const() => &self.get_type() == t,
            Type::Const(t) => &self.get_type() == t.as_ref(),
            _ => false,
        }
    }
}

impl<'v> From<i64> for FilterValue<'v> {
    fn from(i: i64) -> Self {
        FilterValue::Integer(i)
    }
}

impl<'v> From<&'v str> for FilterValue<'v> {
    fn from(v: &'v str) -> Self {
        FilterValue::String(v)
    }
}

impl<'v> From<IpAddr> for FilterValue<'v> {
    fn from(ip: IpAddr) -> Self {
        FilterValue::Ip(ip)
    }
}

impl<'v> From<bool> for FilterValue<'v> {
    fn from(b: bool) -> Self {
        FilterValue::Bool(b)
    }
}

pub struct Context<'s> {
    schema: &'s Schema,
    values: Vec<Option<FilterValue<'s>>>,
}

impl<'s> Context<'s> {
    pub fn new(schema: &'s Schema) -> Self {
        Self {
            schema,
            values: vec![None; schema.fields.len()],
        }
    }

    pub fn set_field_value<'v: 's, V: Into<FilterValue<'v>>>(
        &mut self,
        name: &str,
        value: V,
    ) -> Result<(), ContextError> {
        let (field_idx, field) = self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, fd)| fd.name == name)
            .ok_or_else(|| ContextError::NoSuchField(name.to_owned()))?;
        let value = value.into();
        let field_type = &field.r#type;
        let value_type = value.get_type();
        if field_type != &value_type {
            return Err(ContextError::FieldTypeMismatch(
                field_type.clone(),
                value_type,
            ));
        }

        self.values[field_idx] = Some(value);

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("no such field: {0}")]
    NoSuchField(String),
    #[error("filter value type mismatch. expected: {0:?}, got: {1:?}")]
    FieldTypeMismatch(Type, Type),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error(transparent)]
    Parse(#[from] exprt::parser::parser::ParseError),
    #[error("unimplemented filter type '{0}'")]
    Unimplemented(&'static str),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
    #[error(transparent)]
    ParseAddr(#[from] AddrParseError),
}

pub fn parse_expr(input: &str) -> Result<Expr, ParseError> {
    let mut expr = exprt::parser::parser::parse(input)?;

    trace!("parsed expr: {expr:#?}");

    normalize_expr(&mut expr)?;

    Ok(expr)
}

fn normalize_expr(expr: &mut Expr) -> Result<(), ParseError> {
    match &mut expr.inner {
        ExprKind::Atom(atom) => match atom {
            Atom::StringLiteral(_) => expr.r#type = Type::Const(Box::new(Type::String)),
            Atom::NumberLiteral(_) => expr.r#type = Type::Const(Box::new(Type::Integer)),
            Atom::Ipv4(_) => expr.r#type = Type::Const(Box::new(Type::Ipv4)),
            Atom::Ipv4Cidr(_) => return Err(ParseError::Unimplemented("ipv4 cidr")),
            Atom::Ipv6(_) => expr.r#type = Type::Const(Box::new(Type::Ipv6)),
            Atom::Ipv6Cidr(_) => return Err(ParseError::Unimplemented("ipv6 cidr")),
        },
        ExprKind::Field(_) => {}
        ExprKind::FunctionCall(FunctionCall { ref mut args, .. }) => {
            for expr in args.iter_mut() {
                normalize_expr(expr)?;
            }
        }
        ExprKind::Array {
            ref mut elements, ..
        } => {
            if !elements.is_empty() {
                for expr in elements.iter_mut() {
                    normalize_expr(expr)?;
                }
                expr.r#type = Type::Array(Box::new(elements[0].r#type.clone()));
            }
        }
        ExprKind::Indexed(Indexed {
            ref mut kind,
            ref mut expr,
        }) => {
            if let Index::Expr(ref mut expr) = kind {
                normalize_expr(expr)?;
            }
            normalize_expr(expr.as_mut())?;
        }
        ExprKind::DynamicField(_) => {}
        ExprKind::Not(ref mut expr) => {
            normalize_expr(expr.as_mut())?;
        }
        ExprKind::BitwiseAnd(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::BitwiseOr(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Xor(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Add(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Sub(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Mul(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Mod(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Div(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Or(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::And(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Eq(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::NEq(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Gt(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Gte(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Lt(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Lte(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Contains(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::Matches(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
        ExprKind::In(ref mut expr1, ref mut expr2) => {
            normalize_expr(expr1)?;
            normalize_expr(expr2)?;
        }
    }

    Ok(())
}

pub fn eq_value(
    input: &str,
    ctx: &Context,
    field: &FieldDef,
    field_idx: usize,
    expr: &Expr,
) -> bool {
    match ctx.values.get(field_idx) {
        Some(Some(value)) => match &expr.r#type {
            Type::Placeholder => match &expr.inner {
                ExprKind::Field(Field { .. }) => match field.name.as_str() {
                    t => {
                        warn!("unimplemented eq rhs enum type: {t:?}");
                        false
                    }
                },
                inner => {
                    warn!("expected a field, got: {inner:?}");
                    false
                }
            },

            Type::Integer => match &expr.inner {
                ExprKind::Atom(Atom::NumberLiteral(Span { range, .. })) => match value {
                    FilterValue::Integer(v) => input[range.clone()]
                        .parse::<i64>()
                        .map(|parsed| parsed == *v)
                        .unwrap_or_default(),
                    _ => false,
                },
                t => {
                    warn!("could not match type Integer with inner: {t:?}");
                    false
                }
            },

            Type::String => match &expr.inner {
                ExprKind::Atom(Atom::StringLiteral(Span { range, .. })) => match value {
                    FilterValue::String(v) => &input[(range.start() + 1)..=(range.end() - 1)] == *v,
                    _ => false,
                },
                t => {
                    warn!("could not match type String with inner: {t:?}");
                    false
                }
            },

            Type::Const(_) => match &expr.inner {
                ExprKind::Atom(atom) => match atom {
                    Atom::StringLiteral(Span { range, .. }) => match value {
                        FilterValue::String(v) => {
                            &input[(range.start() + 1)..=(range.end() - 1)] == *v
                        }
                        _ => false,
                    },
                    Atom::NumberLiteral(Span { range, .. }) => match value {
                        FilterValue::Integer(v) => input[range.clone()]
                            .parse::<i64>()
                            .map(|parsed| parsed == *v)
                            .unwrap_or_default(),
                        _ => false,
                    },
                    Atom::Ipv4(Span { range, .. }) => match value {
                        FilterValue::Ip(IpAddr::V4(v)) => input[range.clone()]
                            .parse::<Ipv4Addr>()
                            .map(|parsed| parsed == *v)
                            .unwrap_or_default(),
                        _ => false,
                    },
                    Atom::Ipv6(Span { range, .. }) => match value {
                        FilterValue::Ip(IpAddr::V6(v)) => input[range.clone()]
                            .parse::<Ipv6Addr>()
                            .map(|parsed| parsed == *v)
                            .unwrap_or_default(),
                        _ => false,
                    },
                    Atom::Ipv4Cidr(_) | Atom::Ipv6Cidr(_) => {
                        warn!("unsupport atom: cidr");
                        false
                    }
                },
                t => {
                    warn!("const types should have an inner type of atom, got: {t:?}");
                    false
                }
            },

            Type::Array(arr_type) => {
                trace!("checking arr type for {arr_type:?}");
                match arr_type {
                    t if t.as_ref() == &value.get_type() => {
                        // only go ahead if they're the same type, arrays are homogenous
                        field_value_in(input, ctx, field, field_idx, expr)
                    }
                    _ => false,
                }
            }
            t => {
                warn!("unimplemented eq rhs type: {t:?}");
                false
            }
        },
        None | Some(None) => false,
    }
}

fn field_value_in(
    input: &str,
    ctx: &Context,
    field: &FieldDef,
    field_idx: usize,
    expr: &Expr,
) -> bool {
    match ctx.values.get(field_idx) {
        Some(Some(value)) => match &expr.r#type {
            Type::Array(arr_type) if !value.is_type(arr_type.as_ref()) => {
                trace!("no correct type: {arr_type:?} vs {:?}", value.get_type());
                false
            }
            Type::Array(_) => match &expr.inner {
                ExprKind::Array { elements, .. } => {
                    for ex in elements.iter() {
                        if eq_value(input, ctx, field, field_idx, ex) {
                            return true;
                        }
                    }
                    trace!("did not find any matching!");
                    false
                }
                k => {
                    warn!("expected array, got {k:?}");
                    false
                }
            },
            t => {
                trace!("wrong type: {t:?}");
                false
            }
        },

        None | Some(None) => false,
    }
}

pub fn match_expr(input: &str, expr: &Expr, ctx: &Context) -> bool {
    trace!("match expr {:#?}: {}", expr.inner, expr.to_text(input));
    match &expr.inner {
        ExprKind::Eq(field_expr, expr2) => match match_field(input, field_expr, ctx) {
            Some((field_idx, field)) => eq_value(input, ctx, field, field_idx, expr2),
            None => false,
        },
        ExprKind::NEq(field_expr, expr2) => match match_field(input, field_expr, ctx) {
            Some((field_idx, field)) => !eq_value(input, ctx, field, field_idx, expr2),
            None => false,
        },
        ExprKind::Not(expr) => !match_expr(input, expr.as_ref(), ctx),
        ExprKind::Or(expr1, expr2) => {
            match_expr(input, expr1.as_ref(), ctx) || match_expr(input, expr2.as_ref(), ctx)
        }
        ExprKind::And(expr1, expr2) => {
            match_expr(input, expr1.as_ref(), ctx) && match_expr(input, expr2.as_ref(), ctx)
        }
        ExprKind::In(field_expr, expr_arr) => match match_field(input, field_expr, ctx) {
            Some((field_idx, field)) => field_value_in(input, ctx, field, field_idx, expr_arr),
            None => false,
        },
        inner => {
            warn!("unimplemented expr kind: {inner:?}");
            false
        }
    }
}

pub fn match_field<'s>(
    input: &'s str,
    expr: &'s Expr,
    ctx: &'s Context,
) -> Option<(usize, &'s FieldDef)> {
    debug!("match field: {expr:?}");
    match &expr.inner {
        ExprKind::Field(x) => {
            let start_idx = x.chain.first().unwrap().range.start();
            let end_idx = x.chain.last().unwrap().range.end();
            let name = &input[RangeInclusive::new(*start_idx, *end_idx)];
            ctx.schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, fd)| fd.name == name)
        }
        inner => {
            warn!("asked to match field with non-field expr: {inner:?}");
            None
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::pubsub::SCHEMA;

//     use super::*;

//     #[test]
//     fn matches_custom_corrosion_stuff() {
//         _ = tracing_subscriber::fmt::try_init();
//         let schema = &*SCHEMA;

//         let mut ctx = Context::new(schema);
//         ctx.set_field_value("op.type", OpType::Upsert).unwrap();
//         ctx.set_field_value("record.type", RecordType::App).unwrap();

//         let input = r#"op.type eq upsert"#;
//         let expr = parse_expr(input).unwrap();
//         assert!(match_expr(input, &expr, &ctx));

//         let input = r#"record.type eq app"#;
//         let expr = parse_expr(input).unwrap();
//         assert!(match_expr(input, &expr, &ctx));
//     }

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
//     }
// }
