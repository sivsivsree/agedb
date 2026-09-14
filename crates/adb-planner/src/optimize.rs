//! Optimizer passes.
//!
//! v0.1 does the three that matter most for an analytical scan:
//!
//! * **Predicate extraction for segment pruning**: turn the parts of a filter
//!   that compare a column to constants into atoms the executor can evaluate
//!   against segment statistics *without opening the file* (see "Segments and pruning" in ARCHITECTURE.md).
//! * **Projection pushdown**: read only the columns the plan uses. Done in
//!   [`crate::physical::build`], where the alias mapping is known.
//! * **Limit / top-N pushdown**: `sort` + `limit` becomes a bounded heap rather
//!   than a full sort, also in `physical`.
//!
//! Pruning is only ever allowed to skip data that *cannot* match. The residual
//! filter is still evaluated on every row that is read, so a missed pruning
//! opportunity costs time, never correctness.

use adb_core::Value;
use serde::{Deserialize, Serialize};

use crate::expr::{BinaryOp, Expr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    /// Any of `values`.
    In,
    IsNull,
    IsNotNull,
}

/// A conjunctive constraint on one column, in the column's own type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneAtom {
    pub column: String,
    pub op: PruneOp,
    /// Empty for `IsNull` / `IsNotNull`.
    pub values: Vec<Value>,
}

impl PruneAtom {
    fn binary(column: &str, op: PruneOp, value: Value) -> Self {
        Self {
            column: column.to_string(),
            op,
            values: vec![value],
        }
    }

    fn unary(column: &str, op: PruneOp) -> Self {
        Self {
            column: column.to_string(),
            op,
            values: Vec::new(),
        }
    }
}

fn prune_op(op: BinaryOp) -> Option<PruneOp> {
    Some(match op {
        BinaryOp::Eq => PruneOp::Eq,
        BinaryOp::Lt => PruneOp::Lt,
        BinaryOp::LtEq => PruneOp::LtEq,
        BinaryOp::Gt => PruneOp::Gt,
        BinaryOp::GtEq => PruneOp::GtEq,
        // `!=` could prune a segment where min == max, which is rare enough not
        // to be worth the extra case in the executor.
        _ => return None,
    })
}

/// Pull conjunctive column-vs-constant constraints out of a validated predicate.
///
/// Only conjuncts are usable: a disjunct can be satisfied by the other branch,
/// so pruning on it would drop matching rows. `NOT`, `!=` and negated `IN` are
/// skipped for the same reason.
pub fn extract_prune_atoms(predicate: &Expr) -> Vec<PruneAtom> {
    let mut out = Vec::new();
    for conjunct in predicate.conjuncts() {
        match conjunct {
            Expr::Binary { op, left, right } if op.is_comparison() => {
                if let (Some(column), Some(value)) = (left.as_column(), right.as_literal()) {
                    if !value.is_null() {
                        if let Some(op) = prune_op(*op) {
                            out.push(PruneAtom::binary(column, op, value.clone()));
                        }
                    }
                } else if let (Some(value), Some(column)) = (left.as_literal(), right.as_column()) {
                    // `100 < amount` is `amount > 100`.
                    if !value.is_null() {
                        if let Some(op) = op.flipped().and_then(prune_op) {
                            out.push(PruneAtom::binary(column, op, value.clone()));
                        }
                    }
                }
            }
            Expr::Between {
                expr,
                low,
                high,
                negated: false,
            } => {
                if let Some(column) = expr.as_column() {
                    out.push(PruneAtom::binary(column, PruneOp::GtEq, low.clone()));
                    out.push(PruneAtom::binary(column, PruneOp::LtEq, high.clone()));
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                if let Some(column) = expr.as_column() {
                    if !list.is_empty() && list.iter().all(|v| !v.is_null()) {
                        out.push(PruneAtom {
                            column: column.to_string(),
                            op: PruneOp::In,
                            values: list.clone(),
                        });
                    }
                }
            }
            Expr::IsNull(inner) => {
                if let Some(column) = inner.as_column() {
                    out.push(PruneAtom::unary(column, PruneOp::IsNull));
                }
            }
            Expr::IsNotNull(inner) => {
                if let Some(column) = inner.as_column() {
                    out.push(PruneAtom::unary(column, PruneOp::IsNotNull));
                }
            }
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atoms(predicate: &Expr) -> Vec<(String, PruneOp, Vec<Value>)> {
        extract_prune_atoms(predicate)
            .into_iter()
            .map(|a| (a.column, a.op, a.values))
            .collect()
    }

    #[test]
    fn extracts_comparisons_in_both_orientations() {
        let p = Expr::col("amount")
            .gt(Expr::lit(Value::Float(100.0)))
            .and(Expr::lit(Value::Int(5)).lt(Expr::col("qty")));
        assert_eq!(
            atoms(&p),
            vec![
                ("amount".to_string(), PruneOp::Gt, vec![Value::Float(100.0)]),
                ("qty".to_string(), PruneOp::Gt, vec![Value::Int(5)]),
            ]
        );
    }

    #[test]
    fn between_becomes_a_closed_range() {
        let p = Expr::Between {
            expr: Box::new(Expr::col("at")),
            low: Value::Int(10),
            high: Value::Int(20),
            negated: false,
        };
        assert_eq!(
            atoms(&p),
            vec![
                ("at".to_string(), PruneOp::GtEq, vec![Value::Int(10)]),
                ("at".to_string(), PruneOp::LtEq, vec![Value::Int(20)]),
            ]
        );
    }

    #[test]
    fn in_lists_and_null_checks_are_usable() {
        let p = Expr::InList {
            expr: Box::new(Expr::col("country")),
            list: vec![Value::Str("uae".into()), Value::Str("usa".into())],
            negated: false,
        }
        .and(Expr::IsNull(Box::new(Expr::col("note"))))
        .and(Expr::IsNotNull(Box::new(Expr::col("email"))));
        let extracted = atoms(&p);
        assert_eq!(extracted[0].1, PruneOp::In);
        assert_eq!(extracted[0].2.len(), 2);
        assert_eq!(extracted[1].1, PruneOp::IsNull);
        assert_eq!(extracted[2].1, PruneOp::IsNotNull);
    }

    #[test]
    fn disjunctions_are_never_pruned_on() {
        // Pruning on either branch of an OR would drop rows the other branch
        // matches, so nothing is extracted.
        let p = Expr::col("amount")
            .gt(Expr::lit(Value::Int(100)))
            .or(Expr::col("country").eq(Expr::lit(Value::Str("uae".into()))));
        assert!(atoms(&p).is_empty());
    }

    #[test]
    fn conjuncts_inside_a_disjunction_stay_out_of_reach() {
        let p = Expr::col("a").eq(Expr::lit(Value::Int(1))).and(
            Expr::col("b")
                .eq(Expr::lit(Value::Int(2)))
                .or(Expr::col("c").eq(Expr::lit(Value::Int(3)))),
        );
        let extracted = atoms(&p);
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0].0, "a");
    }

    #[test]
    fn negations_and_unusable_shapes_are_skipped() {
        let cases = vec![
            Expr::Not(Box::new(Expr::col("a").eq(Expr::lit(Value::Int(1))))),
            Expr::col("a").eq(Expr::lit(Value::Null)),
            Expr::binary(BinaryOp::NotEq, Expr::col("a"), Expr::lit(Value::Int(1))),
            Expr::InList {
                expr: Box::new(Expr::col("a")),
                list: vec![Value::Int(1)],
                negated: true,
            },
            Expr::Between {
                expr: Box::new(Expr::col("a")),
                low: Value::Int(1),
                high: Value::Int(2),
                negated: true,
            },
            // Column-to-column comparisons have no constant to prune with.
            Expr::col("a").eq(Expr::col("b")),
        ];
        for case in cases {
            assert!(atoms(&case).is_empty(), "{case} should not yield atoms");
        }
    }
}
