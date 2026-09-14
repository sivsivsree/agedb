//! Scalar expressions.

use std::collections::BTreeSet;
use std::fmt;

use adb_core::{AdbError, DataType, Result, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    /// SQL `LIKE` with `%` and `_` wildcards.
    Like,
}

impl BinaryOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "and",
            Self::Or => "or",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Like => "like",
        }
    }

    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }

    pub fn is_logical(self) -> bool {
        matches!(self, Self::And | Self::Or)
    }

    pub fn is_arithmetic(self) -> bool {
        matches!(self, Self::Add | Self::Sub | Self::Mul | Self::Div)
    }

    /// The operator that means the same thing with the operands swapped.
    pub fn flipped(self) -> Option<Self> {
        Some(match self {
            Self::Eq => Self::Eq,
            Self::NotEq => Self::NotEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            _ => return None,
        })
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
    Literal(Value),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Value>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Value,
        high: Value,
        negated: bool,
    },
}

impl Expr {
    pub fn col(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    pub fn lit(value: Value) -> Self {
        Self::Literal(value)
    }

    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Self {
        Self::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn eq(self, other: Expr) -> Self {
        Self::binary(BinaryOp::Eq, self, other)
    }

    pub fn gt(self, other: Expr) -> Self {
        Self::binary(BinaryOp::Gt, self, other)
    }

    pub fn lt(self, other: Expr) -> Self {
        Self::binary(BinaryOp::Lt, self, other)
    }

    pub fn and(self, other: Expr) -> Self {
        Self::binary(BinaryOp::And, self, other)
    }

    pub fn or(self, other: Expr) -> Self {
        Self::binary(BinaryOp::Or, self, other)
    }

    /// Combine predicates with `AND`.
    pub fn all(predicates: impl IntoIterator<Item = Expr>) -> Option<Expr> {
        predicates.into_iter().reduce(|acc, p| acc.and(p))
    }

    /// Split an `AND` tree into its conjuncts.
    pub fn conjuncts(&self) -> Vec<&Expr> {
        match self {
            Self::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut out = left.conjuncts();
                out.extend(right.conjuncts());
                out
            }
            other => vec![other],
        }
    }

    pub fn referenced_columns(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        self.collect_columns(&mut out);
        out
    }

    fn collect_columns(&self, out: &mut BTreeSet<String>) {
        match self {
            Self::Column(name) => {
                out.insert(name.clone());
            }
            Self::Literal(_) => {}
            Self::Binary { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => {
                inner.collect_columns(out)
            }
            Self::InList { expr, .. } | Self::Between { expr, .. } => expr.collect_columns(out),
        }
    }

    /// Rename column references according to `map` (alias -> source column).
    ///
    /// Needed when a filter or sort sits above a projection that renamed things:
    /// the physical pipeline applies projection late, so references have to be
    /// pushed back onto the underlying names.
    pub fn rewrite_columns(&self, map: &dyn Fn(&str) -> Option<String>) -> Result<Expr> {
        Ok(match self {
            Self::Column(name) => {
                Self::Column(map(name).ok_or_else(|| AdbError::not_found("column", name))?)
            }
            Self::Literal(v) => Self::Literal(v.clone()),
            Self::Binary { op, left, right } => Self::Binary {
                op: *op,
                left: Box::new(left.rewrite_columns(map)?),
                right: Box::new(right.rewrite_columns(map)?),
            },
            Self::Not(inner) => Self::Not(Box::new(inner.rewrite_columns(map)?)),
            Self::IsNull(inner) => Self::IsNull(Box::new(inner.rewrite_columns(map)?)),
            Self::IsNotNull(inner) => Self::IsNotNull(Box::new(inner.rewrite_columns(map)?)),
            Self::InList {
                expr,
                list,
                negated,
            } => Self::InList {
                expr: Box::new(expr.rewrite_columns(map)?),
                list: list.clone(),
                negated: *negated,
            },
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => Self::Between {
                expr: Box::new(expr.rewrite_columns(map)?),
                low: low.clone(),
                high: high.clone(),
                negated: *negated,
            },
        })
    }

    /// The single column this expression is a bare reference to, if any.
    pub fn as_column(&self) -> Option<&str> {
        match self {
            Self::Column(name) => Some(name),
            _ => None,
        }
    }

    pub fn as_literal(&self) -> Option<&Value> {
        match self {
            Self::Literal(value) => Some(value),
            _ => None,
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(name) => write!(f, "{name}"),
            Self::Literal(v) => write!(f, "{v}"),
            Self::Binary { op, left, right } => write!(f, "({left} {op} {right})"),
            Self::Not(inner) => write!(f, "not({inner})"),
            Self::IsNull(inner) => write!(f, "{inner} is null"),
            Self::IsNotNull(inner) => write!(f, "{inner} is not null"),
            Self::InList {
                expr,
                list,
                negated,
            } => {
                let items = list
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let not = if *negated { "not " } else { "" };
                write!(f, "{expr} {not}in ({items})")
            }
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let not = if *negated { "not " } else { "" };
                write!(f, "{expr} {not}between {low} and {high}")
            }
        }
    }
}

/// Coerce a caller-supplied literal to a column's declared type.
///
/// This is what lets an agent write `created_at > "2026-01-01"` or `amount > 5`
/// against a float column: coercion happens once, in the validator, so the
/// executor and the segment-pruning logic both see a literal whose type matches
/// the column exactly. Without it, pruning against min/max statistics would be
/// comparing across types.
pub fn coerce_literal(value: &Value, target: DataType) -> Result<Value> {
    use adb_core::types::{normalize_epoch_to_micros, parse_date_days, parse_timestamp_micros};

    if value.is_null() {
        return Ok(Value::Null);
    }
    let mismatch = || AdbError::TypeMismatch {
        expected: target.name().to_string(),
        actual: format!("{value}"),
    };
    Ok(match (target, value) {
        (DataType::Bool, Value::Bool(_)) => value.clone(),
        (DataType::Int64, Value::Int(_)) => value.clone(),
        (DataType::Int64, Value::Float(f)) if f.fract() == 0.0 => Value::Int(*f as i64),
        (DataType::Float64, Value::Float(_)) => value.clone(),
        (DataType::Float64, Value::Int(i)) => Value::Float(*i as f64),
        (DataType::Utf8 | DataType::Uuid | DataType::Json, Value::Str(_)) => value.clone(),
        (DataType::Timestamp, Value::Timestamp(_)) => value.clone(),
        (DataType::Timestamp, Value::Str(s)) => Value::Timestamp(parse_timestamp_micros(s)?),
        (DataType::Timestamp, Value::Int(i)) => Value::Timestamp(normalize_epoch_to_micros(*i)),
        (DataType::Date, Value::Date(_)) => value.clone(),
        (DataType::Date, Value::Str(s)) => Value::Date(parse_date_days(s)?),
        (DataType::Date, Value::Int(i)) => Value::Date(i32::try_from(*i).map_err(|_| mismatch())?),
        _ => return Err(mismatch()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conjuncts_flatten_nested_ands_only() {
        let e = Expr::col("a")
            .eq(Expr::lit(Value::Int(1)))
            .and(Expr::col("b").gt(Expr::lit(Value::Int(2))))
            .and(
                Expr::col("c")
                    .eq(Expr::lit(Value::Int(3)))
                    .or(Expr::col("d").eq(Expr::lit(Value::Int(4)))),
            );
        let parts = e.conjuncts();
        assert_eq!(parts.len(), 3);
        // The OR stays whole: it is not a conjunct.
        assert!(matches!(
            parts[2],
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn referenced_columns_covers_every_variant() {
        let e = Expr::Between {
            expr: Box::new(Expr::col("at")),
            low: Value::Int(1),
            high: Value::Int(2),
            negated: false,
        }
        .and(Expr::InList {
            expr: Box::new(Expr::col("country")),
            list: vec![Value::Str("uae".into())],
            negated: false,
        })
        .and(Expr::IsNull(Box::new(Expr::col("note"))))
        .and(Expr::Not(Box::new(Expr::col("flag"))));
        assert_eq!(
            e.referenced_columns().into_iter().collect::<Vec<_>>(),
            vec!["at", "country", "flag", "note"]
        );
    }

    #[test]
    fn rewrite_columns_maps_aliases_and_reports_unknowns() {
        let e = Expr::col("revenue").gt(Expr::lit(Value::Int(10)));
        let mapped = e
            .rewrite_columns(&|name| match name {
                "revenue" => Some("amount".to_string()),
                _ => None,
            })
            .unwrap();
        assert_eq!(mapped.to_string(), "(amount > 10)");
        assert!(e.rewrite_columns(&|_| None).is_err());
    }

    #[test]
    fn flipped_operators_preserve_meaning() {
        assert_eq!(BinaryOp::Lt.flipped(), Some(BinaryOp::Gt));
        assert_eq!(BinaryOp::GtEq.flipped(), Some(BinaryOp::LtEq));
        assert_eq!(BinaryOp::Eq.flipped(), Some(BinaryOp::Eq));
        assert_eq!(BinaryOp::And.flipped(), None);
    }

    #[test]
    fn literals_coerce_to_the_column_type() {
        assert_eq!(
            coerce_literal(&Value::Str("2026-01-01".into()), DataType::Timestamp).unwrap(),
            Value::Timestamp(1_767_225_600_000_000)
        );
        assert_eq!(
            coerce_literal(&Value::Int(5), DataType::Float64).unwrap(),
            Value::Float(5.0)
        );
        assert_eq!(
            coerce_literal(&Value::Float(5.0), DataType::Int64).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            coerce_literal(&Value::Null, DataType::Int64).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn impossible_coercions_are_errors_not_guesses() {
        assert!(coerce_literal(&Value::Str("abc".into()), DataType::Int64).is_err());
        assert!(coerce_literal(&Value::Float(1.5), DataType::Int64).is_err());
        assert!(coerce_literal(&Value::Bool(true), DataType::Utf8).is_err());
        assert!(coerce_literal(&Value::Str("not a date".into()), DataType::Timestamp).is_err());
    }

    #[test]
    fn display_is_readable_for_error_messages() {
        let e = Expr::col("amount").gt(Expr::lit(Value::Float(10.0)));
        assert_eq!(e.to_string(), "(amount > 10)");
    }
}
