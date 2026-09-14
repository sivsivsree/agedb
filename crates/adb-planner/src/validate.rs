//! Plan validation: the only door into the executor.
//!
//! Everything an agent (or an LLM) sends arrives as data, and this is where it
//! stops being untrusted. The validator:
//!
//! * resolves every column reference against the table schema,
//! * coerces literals to the column type once, up front,
//! * type-checks predicates and aggregations,
//! * refuses aggregations that are almost certainly mistakes (`sum(id)`),
//! * clamps `limit` to the caller's row budget,
//! * and rejects the parts of the IR v0.1 does not execute, rather than
//!   silently producing wrong answers.
//!
//! A validated plan carries its own output schema, so downstream code never has
//! to re-derive types.

use std::collections::BTreeSet;

use adb_core::ids::validate_ident;
use adb_core::{AdbError, DataType, QueryLimits, Result, SemanticType, TableSchema, Value};

use crate::expr::{coerce_literal, BinaryOp, Expr};
use crate::ir::{AggregateExpr, AggregateFunc, ColumnMeta, OutputSchema, Query, SortExpr};

/// A plan that is safe to execute, plus its shape.
#[derive(Debug, Clone)]
pub struct ValidatedQuery {
    /// Canonicalized plan: literals coerced, projections resolved.
    pub query: Query,
    pub schema: OutputSchema,
    /// Adjustments the caller should know about (e.g. a clamped limit).
    pub warnings: Vec<String>,
}

/// Validate `query` against `table`, under `limits`.
pub fn validate(
    query: &Query,
    table: &TableSchema,
    limits: &QueryLimits,
) -> Result<ValidatedQuery> {
    let mut warnings = Vec::new();
    let (query, schema) = walk(query, table, limits, &mut warnings)?;
    if schema.columns.is_empty() {
        return Err(AdbError::bad_request("query selects no columns"));
    }
    Ok(ValidatedQuery {
        query,
        schema,
        warnings,
    })
}

fn walk(
    query: &Query,
    table: &TableSchema,
    limits: &QueryLimits,
    warnings: &mut Vec<String>,
) -> Result<(Query, OutputSchema)> {
    match query {
        Query::Scan {
            table: table_ref,
            columns,
        } => {
            if table_ref.table != table.name {
                return Err(AdbError::Internal(format!(
                    "plan scans {} but was validated against {}",
                    table_ref.table, table.name
                )));
            }
            let names = match columns {
                Some(requested) if requested.is_empty() => {
                    return Err(AdbError::bad_request("scan requests zero columns"))
                }
                Some(requested) => {
                    let mut seen = BTreeSet::new();
                    for name in requested {
                        table.require_column(name)?;
                        if !seen.insert(name.as_str()) {
                            return Err(AdbError::bad_request(format!(
                                "column {name:?} is requested twice"
                            )));
                        }
                    }
                    requested.clone()
                }
                // `*` deliberately omits sensitive columns; an agent has to name
                // them to see them.
                None => table.default_projection(),
            };
            let schema = OutputSchema::new(
                names
                    .iter()
                    .map(|name| {
                        let col = table.require_column(name)?;
                        Ok(ColumnMeta {
                            name: col.name.clone(),
                            data_type: col.data_type,
                            nullable: col.nullable,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Ok((
                Query::Scan {
                    table: table_ref.clone(),
                    columns: Some(names),
                },
                schema,
            ))
        }

        Query::Filter { input, predicate } => {
            let (input, schema) = walk(input, table, limits, warnings)?;
            let (predicate, ty) = check_expr(predicate, &schema, table)?;
            match ty {
                Some(DataType::Bool) | None => {}
                Some(other) => {
                    return Err(AdbError::TypeMismatch {
                        expected: "a boolean filter".to_string(),
                        actual: format!("{other} ({predicate})"),
                    })
                }
            }
            Ok((
                Query::Filter {
                    input: Box::new(input),
                    predicate,
                },
                schema,
            ))
        }

        Query::Project { input, exprs } => {
            let (input, schema) = walk(input, table, limits, warnings)?;
            if exprs.is_empty() {
                return Err(AdbError::bad_request("projection selects no columns"));
            }
            let mut columns = Vec::with_capacity(exprs.len());
            let mut seen = BTreeSet::new();
            let mut resolved = Vec::with_capacity(exprs.len());
            for (expr, alias) in exprs {
                // v0.1 has no computed projections: `select amount * 2` is a
                // follow-up, and pretending otherwise would produce a plan the
                // executor cannot run.
                let source = expr.as_column().ok_or_else(|| {
                    AdbError::Unsupported(format!(
                        "computed projection ({expr}); project columns and aggregate instead"
                    ))
                })?;
                let meta = schema
                    .column(source)
                    .ok_or_else(|| AdbError::not_found("column", source))?;
                validate_ident("column", alias)?;
                if !seen.insert(alias.clone()) {
                    return Err(AdbError::bad_request(format!(
                        "output name {alias:?} is used twice"
                    )));
                }
                columns.push(ColumnMeta {
                    name: alias.clone(),
                    data_type: meta.data_type,
                    nullable: meta.nullable,
                });
                resolved.push((Expr::col(source), alias.clone()));
            }
            Ok((
                Query::Project {
                    input: Box::new(input),
                    exprs: resolved,
                },
                OutputSchema::new(columns),
            ))
        }

        Query::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let (input, schema) = walk(input, table, limits, warnings)?;
            if aggregates.is_empty() {
                return Err(AdbError::bad_request(
                    "aggregate needs at least one metric (try count)",
                ));
            }
            let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
            let mut names = BTreeSet::new();
            for key in group_by {
                let meta = schema
                    .column(key)
                    .ok_or_else(|| AdbError::not_found("column", key))?;
                if !meta.data_type.is_ordered() {
                    return Err(AdbError::bad_request(format!(
                        "column {key:?} of type {} cannot be grouped",
                        meta.data_type
                    )));
                }
                if !names.insert(key.clone()) {
                    return Err(AdbError::bad_request(format!(
                        "group key {key:?} is repeated"
                    )));
                }
                columns.push(meta.clone());
            }
            let mut resolved = Vec::with_capacity(aggregates.len());
            for agg in aggregates {
                let checked = check_aggregate(agg, &schema, table)?;
                if !names.insert(checked.alias.clone()) {
                    return Err(AdbError::bad_request(format!(
                        "output name {:?} is used twice",
                        checked.alias
                    )));
                }
                let arg_type = checked
                    .column
                    .as_ref()
                    .and_then(|c| schema.column(c))
                    .map(|m| m.data_type);
                columns.push(ColumnMeta {
                    name: checked.alias.clone(),
                    data_type: checked.func.output_type(arg_type),
                    // Only `count` is guaranteed non-null: a group whose values
                    // are all NULL sums to NULL.
                    nullable: checked.func != AggregateFunc::Count,
                });
                resolved.push(checked);
            }
            Ok((
                Query::Aggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: resolved,
                },
                OutputSchema::new(columns),
            ))
        }

        Query::Sort { input, exprs } => {
            let (input, schema) = walk(input, table, limits, warnings)?;
            if exprs.is_empty() {
                return Err(AdbError::bad_request("sort needs at least one key"));
            }
            let mut resolved = Vec::with_capacity(exprs.len());
            for key in exprs {
                let meta = schema
                    .column(&key.column)
                    .ok_or_else(|| AdbError::not_found("column", &key.column))?;
                if !meta.data_type.is_ordered() {
                    return Err(AdbError::bad_request(format!(
                        "column {:?} of type {} cannot be sorted",
                        key.column, meta.data_type
                    )));
                }
                resolved.push(SortExpr {
                    column: key.column.clone(),
                    ascending: key.ascending,
                    nulls_first: key.nulls_first,
                });
            }
            Ok((
                Query::Sort {
                    input: Box::new(input),
                    exprs: resolved,
                },
                schema,
            ))
        }

        Query::Limit {
            input,
            limit,
            offset,
        } => {
            let (input, schema) = walk(input, table, limits, warnings)?;
            let mut limit = *limit;
            if limit > limits.max_rows {
                warnings.push(format!(
                    "limit {limit} exceeds the {} row budget for this request and was reduced",
                    limits.max_rows
                ));
                limit = limits.max_rows;
            }
            Ok((
                Query::Limit {
                    input: Box::new(input),
                    limit,
                    offset: *offset,
                },
                schema,
            ))
        }

        Query::Join { .. } => Err(AdbError::Unsupported(
            "joins are not available in v0.1: query one table at a time".to_string(),
        )),
    }
}

fn check_aggregate(
    agg: &AggregateExpr,
    schema: &OutputSchema,
    table: &TableSchema,
) -> Result<AggregateExpr> {
    let alias = if agg.alias.is_empty() {
        AggregateExpr::default_alias(agg.func, agg.column.as_deref())
    } else {
        agg.alias.clone()
    };
    validate_ident("column", &alias)?;

    let Some(column) = agg.column.as_ref() else {
        if agg.func != AggregateFunc::Count {
            return Err(AdbError::bad_request(format!(
                "{} needs a column; only count can be applied to all rows",
                agg.func
            )));
        }
        return Ok(AggregateExpr {
            func: agg.func,
            column: None,
            alias,
        });
    };

    let meta = schema
        .column(column)
        .ok_or_else(|| AdbError::not_found("column", column))?;
    let ok = match agg.func {
        AggregateFunc::Count => true,
        AggregateFunc::Sum | AggregateFunc::Avg => meta.data_type.is_additive(),
        AggregateFunc::Min | AggregateFunc::Max => meta.data_type.is_ordered(),
    };
    if !ok {
        return Err(AdbError::bad_request(format!(
            "{}({column}) is not defined for type {}",
            agg.func, meta.data_type
        )));
    }
    // Semantic guardrail: identifiers and emails are numeric-looking but adding
    // them up is meaningless, and an LLM will occasionally try.
    if matches!(agg.func, AggregateFunc::Sum | AggregateFunc::Avg) {
        if let Some(col) = table.column(column) {
            if col
                .semantic_type
                .as_ref()
                .map(SemanticType::is_aggregation_hostile)
                .unwrap_or(false)
            {
                return Err(AdbError::bad_request(format!(
                    "{}({column}) is not meaningful: {column} is a {} column, not a measure",
                    agg.func,
                    col.semantic_type.as_ref().expect("checked above").as_str()
                )));
            }
        }
    }
    Ok(AggregateExpr {
        func: agg.func,
        column: Some(column.clone()),
        alias,
    })
}

/// Type-check an expression and canonicalize its literals.
///
/// Returns `None` for the type of an untyped `NULL` literal.
fn check_expr(
    expr: &Expr,
    schema: &OutputSchema,
    table: &TableSchema,
) -> Result<(Expr, Option<DataType>)> {
    match expr {
        Expr::Column(name) => {
            let meta = schema
                .column(name)
                .ok_or_else(|| AdbError::not_found("column", name))?;
            Ok((Expr::Column(name.clone()), Some(meta.data_type)))
        }
        Expr::Literal(value) => Ok((Expr::Literal(value.clone()), value.data_type())),
        Expr::Not(inner) => {
            let (inner, ty) = check_expr(inner, schema, table)?;
            expect_bool(&inner, ty)?;
            Ok((Expr::Not(Box::new(inner)), Some(DataType::Bool)))
        }
        Expr::IsNull(inner) => {
            let (inner, _) = check_expr(inner, schema, table)?;
            Ok((Expr::IsNull(Box::new(inner)), Some(DataType::Bool)))
        }
        Expr::IsNotNull(inner) => {
            let (inner, _) = check_expr(inner, schema, table)?;
            Ok((Expr::IsNotNull(Box::new(inner)), Some(DataType::Bool)))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let (inner, ty) = check_expr(expr, schema, table)?;
            if list.is_empty() {
                return Err(AdbError::bad_request("`in` needs at least one value"));
            }
            let coerced = match ty {
                Some(target) => list
                    .iter()
                    .map(|v| coerce_literal(v, target))
                    .collect::<Result<Vec<_>>>()?,
                None => list.clone(),
            };
            Ok((
                Expr::InList {
                    expr: Box::new(inner),
                    list: coerced,
                    negated: *negated,
                },
                Some(DataType::Bool),
            ))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let (inner, ty) = check_expr(expr, schema, table)?;
            let (low, high) = match ty {
                Some(target) => (coerce_literal(low, target)?, coerce_literal(high, target)?),
                None => (low.clone(), high.clone()),
            };
            if low > high {
                return Err(AdbError::bad_request(format!(
                    "`between {low} and {high}` can never match: the bounds are reversed"
                )));
            }
            Ok((
                Expr::Between {
                    expr: Box::new(inner),
                    low,
                    high,
                    negated: *negated,
                },
                Some(DataType::Bool),
            ))
        }
        Expr::Binary { op, left, right } => check_binary(*op, left, right, schema, table),
    }
}

fn check_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &OutputSchema,
    table: &TableSchema,
) -> Result<(Expr, Option<DataType>)> {
    let (mut left, mut left_ty) = check_expr(left, schema, table)?;
    let (mut right, mut right_ty) = check_expr(right, schema, table)?;

    if op.is_logical() {
        expect_bool(&left, left_ty)?;
        expect_bool(&right, right_ty)?;
        return Ok((Expr::binary(op, left, right), Some(DataType::Bool)));
    }

    if op == BinaryOp::Like {
        if left_ty.map(|t| matches!(t, DataType::Utf8 | DataType::Uuid)) == Some(false) {
            return Err(AdbError::TypeMismatch {
                expected: "text on the left of `like`".to_string(),
                actual: format!("{} ({left})", left_ty.expect("checked").name()),
            });
        }
        match right.as_literal() {
            Some(Value::Str(_)) => {}
            _ => {
                return Err(AdbError::bad_request(
                    "`like` needs a string pattern on the right".to_string(),
                ))
            }
        }
        return Ok((Expr::binary(op, left, right), Some(DataType::Bool)));
    }

    // Coerce a literal on either side to the other side's type, so comparisons
    // and pruning both see matching types.
    if let (Some(target), Expr::Literal(value)) = (left_ty, &right) {
        right = Expr::Literal(coerce_literal(value, target)?);
        right_ty = Some(target);
    } else if let (Expr::Literal(value), Some(target)) = (&left, right_ty) {
        left = Expr::Literal(coerce_literal(value, target)?);
        left_ty = Some(target);
    }

    if op.is_arithmetic() {
        let lt = require_type(left_ty, &left)?;
        let rt = require_type(right_ty, &right)?;
        if !lt.is_numeric() || !rt.is_numeric() {
            return Err(AdbError::TypeMismatch {
                expected: "numeric operands".to_string(),
                actual: format!("{lt} {op} {rt}"),
            });
        }
        let result = if lt == DataType::Float64 || rt == DataType::Float64 {
            DataType::Float64
        } else {
            DataType::Int64
        };
        return Ok((Expr::binary(op, left, right), Some(result)));
    }

    // Comparison.
    if let (Some(lt), Some(rt)) = (left_ty, right_ty) {
        let compatible = lt == rt || (lt.is_numeric() && rt.is_numeric());
        if !compatible {
            return Err(AdbError::TypeMismatch {
                expected: format!("a value comparable with {lt}"),
                actual: format!("{rt} ({right})"),
            });
        }
        if !lt.is_ordered() && !matches!(op, BinaryOp::Eq | BinaryOp::NotEq) {
            return Err(AdbError::bad_request(format!(
                "type {lt} supports only = and !=, not {op}"
            )));
        }
    }
    Ok((Expr::binary(op, left, right), Some(DataType::Bool)))
}

fn expect_bool(expr: &Expr, ty: Option<DataType>) -> Result<()> {
    match ty {
        Some(DataType::Bool) | None => Ok(()),
        Some(other) => Err(AdbError::TypeMismatch {
            expected: "bool".to_string(),
            actual: format!("{other} ({expr})"),
        }),
    }
}

fn require_type(ty: Option<DataType>, expr: &Expr) -> Result<DataType> {
    ty.ok_or_else(|| AdbError::bad_request(format!("cannot infer the type of {expr}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{AggregateFunc, SortExpr, TableRef};
    use adb_core::{Aggregation, ColumnSchema, TableName};

    fn orders() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("amount", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum),
                ColumnSchema::new("created_at", DataType::Timestamp)
                    .semantic(SemanticType::Timestamp),
                ColumnSchema::new("email", DataType::Utf8)
                    .semantic(SemanticType::Email)
                    .sensitive(),
                ColumnSchema::new("payload", DataType::Json),
            ],
        )
        .with_primary_key(vec!["id".to_string()])
    }

    fn limits() -> QueryLimits {
        QueryLimits {
            max_rows: 100,
            ..QueryLimits::default()
        }
    }

    fn scan() -> Query {
        Query::scan(TableName::new("orders").unwrap())
    }

    #[test]
    fn star_projection_omits_sensitive_columns() {
        let v = validate(&scan(), &orders(), &limits()).unwrap();
        assert_eq!(
            v.schema.names(),
            vec!["id", "country", "amount", "created_at", "payload"]
        );
        // But naming it explicitly works.
        let explicit =
            Query::scan_columns(TableName::new("orders").unwrap(), vec!["email".to_string()]);
        assert_eq!(
            validate(&explicit, &orders(), &limits())
                .unwrap()
                .schema
                .names(),
            vec!["email"]
        );
    }

    #[test]
    fn unknown_columns_are_rejected_everywhere() {
        let cases = vec![
            Query::scan_columns(TableName::new("orders").unwrap(), vec!["nope".into()]),
            scan().filter(Expr::col("nope").eq(Expr::lit(Value::Int(1)))),
            scan().project(vec![(Expr::col("nope"), "x".to_string())]),
            scan().aggregate(vec!["nope".into()], vec![AggregateExpr::count_star("n")]),
            scan().sort(vec![SortExpr::asc("nope")]),
        ];
        for query in cases {
            let err = validate(&query, &orders(), &limits()).unwrap_err();
            assert_eq!(err.code(), "not_found", "{}", query.explain());
        }
    }

    #[test]
    fn literals_are_coerced_to_the_column_type() {
        let query =
            scan().filter(Expr::col("created_at").gt(Expr::lit(Value::Str("2026-01-01".into()))));
        let v = validate(&query, &orders(), &limits()).unwrap();
        let Query::Filter { predicate, .. } = &v.query else {
            panic!("expected a filter")
        };
        assert!(
            predicate
                .to_string()
                .contains("2026-01-01T00:00:00.000000Z"),
            "{predicate}"
        );
    }

    #[test]
    fn non_boolean_filters_are_rejected() {
        let query = scan().filter(Expr::col("amount"));
        assert_eq!(
            validate(&query, &orders(), &limits()).unwrap_err().code(),
            "type_mismatch"
        );
    }

    #[test]
    fn incomparable_types_are_rejected() {
        let query = scan().filter(Expr::col("country").gt(Expr::lit(Value::Int(3))));
        assert_eq!(
            validate(&query, &orders(), &limits()).unwrap_err().code(),
            "type_mismatch"
        );
    }

    #[test]
    fn json_columns_compare_only_by_equality() {
        let ok = scan().filter(Expr::col("payload").eq(Expr::lit(Value::Str("{}".into()))));
        validate(&ok, &orders(), &limits()).unwrap();
        let bad = scan().filter(Expr::col("payload").lt(Expr::lit(Value::Str("{}".into()))));
        assert_eq!(
            validate(&bad, &orders(), &limits()).unwrap_err().code(),
            "bad_request"
        );
        let bad_sort = scan().sort(vec![SortExpr::asc("payload")]);
        assert!(validate(&bad_sort, &orders(), &limits()).is_err());
    }

    #[test]
    fn aggregations_are_checked_against_types_and_semantics() {
        let sum_text = scan().aggregate(
            vec![],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("country".into()),
                "x",
            )],
        );
        assert_eq!(
            validate(&sum_text, &orders(), &limits())
                .unwrap_err()
                .code(),
            "bad_request"
        );

        // Numerically possible, semantically nonsense: summing an id.
        let sum_id = scan().aggregate(
            vec![],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("id".into()),
                "x",
            )],
        );
        let err = validate(&sum_id, &orders(), &limits()).unwrap_err();
        assert!(err.to_string().contains("not a measure"), "{err}");

        // But counting and min/max over an id are fine.
        let counted = scan().aggregate(
            vec![],
            vec![
                AggregateExpr::new(AggregateFunc::Count, Some("id".into()), "n"),
                AggregateExpr::new(AggregateFunc::Max, Some("id".into()), "newest"),
            ],
        );
        validate(&counted, &orders(), &limits()).unwrap();
    }

    #[test]
    fn aggregate_output_schema_names_and_types_are_derived() {
        let query = scan().aggregate(
            vec!["country".to_string()],
            vec![
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
                AggregateExpr::count_star("orders"),
            ],
        );
        let v = validate(&query, &orders(), &limits()).unwrap();
        assert_eq!(v.schema.names(), vec!["country", "revenue", "orders"]);
        assert_eq!(
            v.schema.column("revenue").unwrap().data_type,
            DataType::Float64
        );
        assert_eq!(
            v.schema.column("orders").unwrap().data_type,
            DataType::Int64
        );
        assert!(!v.schema.column("orders").unwrap().nullable);
        assert!(v.schema.column("revenue").unwrap().nullable);
    }

    #[test]
    fn duplicate_output_names_are_rejected() {
        let query = scan().aggregate(
            vec!["country".to_string()],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("amount".into()),
                "country",
            )],
        );
        assert_eq!(
            validate(&query, &orders(), &limits()).unwrap_err().code(),
            "bad_request"
        );

        let query = scan().project(vec![
            (Expr::col("id"), "x".to_string()),
            (Expr::col("country"), "x".to_string()),
        ]);
        assert_eq!(
            validate(&query, &orders(), &limits()).unwrap_err().code(),
            "bad_request"
        );
    }

    #[test]
    fn empty_aggregate_alias_gets_a_predictable_default() {
        let query = scan().aggregate(
            vec![],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("amount".into()),
                "",
            )],
        );
        let v = validate(&query, &orders(), &limits()).unwrap();
        assert_eq!(v.schema.names(), vec!["sum_amount"]);
    }

    #[test]
    fn limits_are_clamped_with_a_warning_not_an_error() {
        let query = scan().limit(1_000_000);
        let v = validate(&query, &orders(), &limits()).unwrap();
        let Query::Limit { limit, .. } = &v.query else {
            panic!("expected a limit")
        };
        assert_eq!(*limit, 100);
        assert_eq!(v.warnings.len(), 1);
        assert!(v.warnings[0].contains("row budget"), "{:?}", v.warnings);
    }

    #[test]
    fn joins_and_computed_projections_are_refused_explicitly() {
        let join = Query::Join {
            left: Box::new(scan()),
            right: Box::new(scan()),
            condition: Expr::col("id").eq(Expr::col("id")),
        };
        assert_eq!(
            validate(&join, &orders(), &limits()).unwrap_err().code(),
            "unsupported"
        );

        let computed = scan().project(vec![(
            Expr::binary(BinaryOp::Mul, Expr::col("amount"), Expr::lit(Value::Int(2))),
            "doubled".to_string(),
        )]);
        let err = validate(&computed, &orders(), &limits()).unwrap_err();
        assert_eq!(err.code(), "unsupported");
    }

    #[test]
    fn reversed_between_bounds_are_caught_early() {
        let query = scan().filter(Expr::Between {
            expr: Box::new(Expr::col("amount")),
            low: Value::Int(100),
            high: Value::Int(1),
            negated: false,
        });
        let err = validate(&query, &orders(), &limits()).unwrap_err();
        assert!(err.to_string().contains("never match"), "{err}");
    }

    #[test]
    fn in_lists_are_coerced_and_must_be_non_empty() {
        let query = scan().filter(Expr::InList {
            expr: Box::new(Expr::col("amount")),
            list: vec![Value::Int(1), Value::Int(2)],
            negated: false,
        });
        let v = validate(&query, &orders(), &limits()).unwrap();
        let Query::Filter { predicate, .. } = &v.query else {
            panic!()
        };
        let Expr::InList { list, .. } = predicate else {
            panic!()
        };
        assert_eq!(list, &vec![Value::Float(1.0), Value::Float(2.0)]);

        let empty = scan().filter(Expr::InList {
            expr: Box::new(Expr::col("amount")),
            list: vec![],
            negated: false,
        });
        assert!(validate(&empty, &orders(), &limits()).is_err());
    }

    #[test]
    fn a_plan_for_another_table_is_an_internal_error() {
        let query = Query::Scan {
            table: TableRef::new(TableName::new("customers").unwrap()),
            columns: None,
        };
        assert_eq!(
            validate(&query, &orders(), &limits()).unwrap_err().code(),
            "internal_error"
        );
    }
}
