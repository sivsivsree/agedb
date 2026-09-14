//! Vectorized expression evaluation.
//!
//! Every operator is an Arrow kernel over whole arrays; there is no row-at-a-time
//! path. Two details matter for correctness:
//!
//! * **Three-valued logic.** `AND`/`OR` use the Kleene kernels so `NULL`
//!   propagates the way SQL says it does, and a predicate that evaluates to
//!   `NULL` filters the row out (never in).
//! * **Typed scalars.** A literal is materialized as a one-element array with
//!   *exactly* the other side's Arrow type, including timestamp timezone.
//!   Arrow's comparison kernels reject mismatched types, and silently casting
//!   would be how you end up comparing microseconds to seconds.

use std::sync::Arc;

use adb_core::{AdbError, Result, Value};
use adb_planner::{BinaryOp, Expr};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, Scalar,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowType, TimeUnit};

/// Either a column or a broadcast scalar (held as a length-1 array).
enum Operand {
    Array(ArrayRef),
    Scalar(ArrayRef),
}

impl Operand {
    fn data_type(&self) -> ArrowType {
        match self {
            Self::Array(a) | Self::Scalar(a) => a.data_type().clone(),
        }
    }

    fn as_datum(&self) -> Box<dyn arrow::array::Datum + '_> {
        match self {
            Self::Array(a) => Box::new(a.clone()),
            Self::Scalar(a) => Box::new(Scalar::new(a.clone())),
        }
    }

    fn cast_to(self, target: &ArrowType) -> Result<Self> {
        if &self.data_type() == target {
            return Ok(self);
        }
        Ok(match self {
            Self::Array(a) => Self::Array(arrow::compute::cast(&a, target)?),
            Self::Scalar(a) => Self::Scalar(arrow::compute::cast(&a, target)?),
        })
    }

    fn into_array(self, rows: usize) -> Result<ArrayRef> {
        match self {
            Self::Array(a) => Ok(a),
            Self::Scalar(a) => {
                let indices = arrow::array::UInt32Array::from(vec![0u32; rows]);
                Ok(arrow::compute::take(&a, &indices, None)?)
            }
        }
    }
}

/// Evaluate `expr` over `batch`, returning one value per row.
pub fn eval(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    eval_operand(expr, batch, None)?.into_array(batch.num_rows())
}

/// Evaluate a boolean expression, *preserving* nulls.
///
/// Null preservation has to run all the way through the expression tree, not
/// just at the end: `NOT (country = 'uae')` must stay unknown for a row where
/// `country` is NULL, and collapsing that to `false` early would turn it into
/// `true`.
pub fn eval_bool(expr: &Expr, batch: &RecordBatch) -> Result<BooleanArray> {
    let array = eval(expr, batch)?;
    Ok(array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            AdbError::Internal(format!(
                "predicate produced {}, not a boolean",
                array.data_type()
            ))
        })?
        .clone())
}

/// Evaluate a predicate into a filter mask, where unknown means "not returned".
pub fn eval_predicate(expr: &Expr, batch: &RecordBatch) -> Result<BooleanArray> {
    let mask = eval_bool(expr, batch)?;
    Ok(if mask.null_count() == 0 {
        mask
    } else {
        // SQL: a row whose predicate is unknown is not returned.
        arrow::compute::prep_null_mask_filter(&mask)
    })
}

/// Apply a predicate to a batch.
pub fn filter_batch(expr: &Expr, batch: &RecordBatch) -> Result<RecordBatch> {
    let mask = eval_predicate(expr, batch)?;
    arrow::compute::filter_record_batch(batch, &mask).map_err(Into::into)
}

fn eval_operand(expr: &Expr, batch: &RecordBatch, hint: Option<&ArrowType>) -> Result<Operand> {
    match expr {
        Expr::Column(name) => {
            let idx = batch
                .schema()
                .index_of(name)
                .map_err(|_| AdbError::not_found("column", name))?;
            Ok(Operand::Array(batch.column(idx).clone()))
        }
        Expr::Literal(value) => Ok(Operand::Scalar(literal_array(value, hint)?)),
        Expr::Not(inner) => {
            let mask = eval_bool(inner, batch)?;
            Ok(Operand::Array(Arc::new(
                arrow::compute::kernels::boolean::not(&mask)?,
            )))
        }
        Expr::IsNull(inner) => {
            let array = eval(inner, batch)?;
            Ok(Operand::Array(Arc::new(
                arrow::compute::kernels::boolean::is_null(&array)?,
            )))
        }
        Expr::IsNotNull(inner) => {
            let array = eval(inner, batch)?;
            Ok(Operand::Array(Arc::new(
                arrow::compute::kernels::boolean::is_not_null(&array)?,
            )))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = eval_operand(expr, batch, None)?;
            let ty = target.data_type();
            let mut mask: Option<BooleanArray> = None;
            for value in list {
                let scalar = Operand::Scalar(literal_array(value, Some(&ty))?);
                let eq = arrow::compute::kernels::cmp::eq(
                    target.as_datum().as_ref(),
                    scalar.as_datum().as_ref(),
                )?;
                mask = Some(match mask {
                    None => eq,
                    Some(previous) => arrow::compute::kernels::boolean::or_kleene(&previous, &eq)?,
                });
            }
            let mask =
                mask.ok_or_else(|| AdbError::bad_request("`in` needs at least one value"))?;
            let mask = if *negated {
                arrow::compute::kernels::boolean::not(&mask)?
            } else {
                mask
            };
            Ok(Operand::Array(Arc::new(mask)))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let lower = Expr::binary(BinaryOp::GtEq, (**expr).clone(), Expr::Literal(low.clone()));
            let upper = Expr::binary(
                BinaryOp::LtEq,
                (**expr).clone(),
                Expr::Literal(high.clone()),
            );
            let combined = lower.and(upper);
            let mask = eval_bool(&combined, batch)?;
            let mask = if *negated {
                arrow::compute::kernels::boolean::not(&mask)?
            } else {
                mask
            };
            Ok(Operand::Array(Arc::new(mask)))
        }
        Expr::Binary { op, left, right } => eval_binary(*op, left, right, batch),
    }
}

fn eval_binary(op: BinaryOp, left: &Expr, right: &Expr, batch: &RecordBatch) -> Result<Operand> {
    use arrow::compute::kernels::{boolean, cmp, numeric};

    if op.is_logical() {
        let l = eval_bool(left, batch)?;
        let r = eval_bool(right, batch)?;
        let out = match op {
            BinaryOp::And => boolean::and_kleene(&l, &r)?,
            BinaryOp::Or => boolean::or_kleene(&l, &r)?,
            _ => unreachable!("checked by is_logical"),
        };
        return Ok(Operand::Array(Arc::new(out)));
    }

    // Evaluate the non-literal side first so a literal can adopt its type.
    let (mut lhs, mut rhs) = if matches!(right, Expr::Literal(_)) {
        let lhs = eval_operand(left, batch, None)?;
        let ty = lhs.data_type();
        let rhs = eval_operand(right, batch, Some(&ty))?;
        (lhs, rhs)
    } else if matches!(left, Expr::Literal(_)) {
        let rhs = eval_operand(right, batch, None)?;
        let ty = rhs.data_type();
        let lhs = eval_operand(left, batch, Some(&ty))?;
        (lhs, rhs)
    } else {
        (
            eval_operand(left, batch, None)?,
            eval_operand(right, batch, None)?,
        )
    };

    // Arrow kernels require identical types; unify numerics through Float64.
    if lhs.data_type() != rhs.data_type() {
        let unified =
            unify(&lhs.data_type(), &rhs.data_type()).ok_or_else(|| AdbError::TypeMismatch {
                expected: lhs.data_type().to_string(),
                actual: rhs.data_type().to_string(),
            })?;
        lhs = lhs.cast_to(&unified)?;
        rhs = rhs.cast_to(&unified)?;
    }

    let l = lhs.as_datum();
    let r = rhs.as_datum();
    let out: ArrayRef = match op {
        BinaryOp::Eq => Arc::new(cmp::eq(l.as_ref(), r.as_ref())?),
        BinaryOp::NotEq => Arc::new(cmp::neq(l.as_ref(), r.as_ref())?),
        BinaryOp::Lt => Arc::new(cmp::lt(l.as_ref(), r.as_ref())?),
        BinaryOp::LtEq => Arc::new(cmp::lt_eq(l.as_ref(), r.as_ref())?),
        BinaryOp::Gt => Arc::new(cmp::gt(l.as_ref(), r.as_ref())?),
        BinaryOp::GtEq => Arc::new(cmp::gt_eq(l.as_ref(), r.as_ref())?),
        BinaryOp::Like => Arc::new(arrow::compute::kernels::comparison::like(
            l.as_ref(),
            r.as_ref(),
        )?),
        BinaryOp::Add => numeric::add(l.as_ref(), r.as_ref())?,
        BinaryOp::Sub => numeric::sub(l.as_ref(), r.as_ref())?,
        BinaryOp::Mul => numeric::mul(l.as_ref(), r.as_ref())?,
        BinaryOp::Div => numeric::div(l.as_ref(), r.as_ref())?,
        BinaryOp::And | BinaryOp::Or => unreachable!("handled above"),
    };
    Ok(Operand::Array(out))
}

fn unify(left: &ArrowType, right: &ArrowType) -> Option<ArrowType> {
    use ArrowType::*;
    match (left, right) {
        (Int64, Float64) | (Float64, Int64) => Some(Float64),
        // Timestamps that differ only in timezone annotation.
        (Timestamp(TimeUnit::Microsecond, _), Timestamp(TimeUnit::Microsecond, tz)) => {
            Some(Timestamp(TimeUnit::Microsecond, tz.clone()))
        }
        _ => None,
    }
}

/// Materialize a literal as a length-1 array of the target Arrow type.
fn literal_array(value: &Value, target: Option<&ArrowType>) -> Result<ArrayRef> {
    let target = match target {
        Some(t) => t.clone(),
        None => match value {
            Value::Null => ArrowType::Null,
            Value::Bool(_) => ArrowType::Boolean,
            Value::Int(_) => ArrowType::Int64,
            Value::Float(_) => ArrowType::Float64,
            Value::Str(_) => ArrowType::Utf8,
            Value::Timestamp(_) => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Value::Date(_) => ArrowType::Date32,
        },
    };
    if value.is_null() {
        return Ok(arrow::array::new_null_array(&target, 1));
    }
    let array: ArrayRef = match (&target, value) {
        (ArrowType::Boolean, Value::Bool(v)) => Arc::new(BooleanArray::from(vec![*v])),
        (ArrowType::Int64, Value::Int(v)) => Arc::new(Int64Array::from(vec![*v])),
        (ArrowType::Int64, Value::Float(v)) if v.fract() == 0.0 => {
            Arc::new(Int64Array::from(vec![*v as i64]))
        }
        (ArrowType::Float64, Value::Float(v)) => Arc::new(Float64Array::from(vec![*v])),
        (ArrowType::Float64, Value::Int(v)) => Arc::new(Float64Array::from(vec![*v as f64])),
        (ArrowType::Utf8, Value::Str(v)) => Arc::new(StringArray::from(vec![v.clone()])),
        (ArrowType::Timestamp(TimeUnit::Microsecond, tz), Value::Timestamp(v)) => {
            let array = TimestampMicrosecondArray::from(vec![*v]);
            match tz {
                Some(tz) => Arc::new(array.with_timezone(tz.clone())),
                None => Arc::new(array),
            }
        }
        (ArrowType::Date32, Value::Date(v)) => Arc::new(Date32Array::from(vec![*v])),
        (target, value) => {
            return Err(AdbError::TypeMismatch {
                expected: target.to_string(),
                actual: format!("{value} ({})", value.type_name()),
            })
        }
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{ColumnSchema, DataType, TableName, TableSchema};
    use adb_storage::rows;
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64).required(),
                ColumnSchema::new("country", DataType::Utf8),
                ColumnSchema::new("amount", DataType::Float64),
                ColumnSchema::new("at", DataType::Timestamp),
            ],
        )
    }

    fn batch() -> RecordBatch {
        let schema = schema();
        let rows: Vec<_> = json!([
            {"id": 1, "country": "uae", "amount": 100.0, "at": "2026-01-01T00:00:00Z"},
            {"id": 2, "country": "usa", "amount": 250.0, "at": "2026-02-01T00:00:00Z"},
            {"id": 3, "country": null, "amount": null, "at": "2026-03-01T00:00:00Z"}
        ])
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
        rows::batch_from_json_rows(&schema, &rows).unwrap()
    }

    fn matching_ids(predicate: &Expr) -> Vec<i64> {
        let filtered = filter_batch(predicate, &batch()).unwrap();
        rows::column_values(filtered.column(0).as_ref())
            .unwrap()
            .into_iter()
            .map(|v| v.as_i64().unwrap())
            .collect()
    }

    #[test]
    fn comparisons_against_literals_work_for_every_type() {
        assert_eq!(
            matching_ids(&Expr::col("id").gt(Expr::lit(Value::Int(1)))),
            vec![2, 3]
        );
        assert_eq!(
            matching_ids(&Expr::col("country").eq(Expr::lit(Value::Str("uae".into())))),
            vec![1]
        );
        assert_eq!(
            matching_ids(&Expr::col("amount").gt(Expr::lit(Value::Float(200.0)))),
            vec![2]
        );
        assert_eq!(
            matching_ids(&Expr::col("at").gt(Expr::lit(Value::Timestamp(1_767_225_600_000_000)))),
            vec![2, 3]
        );
    }

    #[test]
    fn integer_literals_compare_against_float_columns() {
        assert_eq!(
            matching_ids(&Expr::col("amount").gt(Expr::lit(Value::Int(200)))),
            vec![2]
        );
    }

    #[test]
    fn null_rows_never_satisfy_a_predicate() {
        // Row 3 has NULL amount: it must not appear for `>` or for `<`.
        assert_eq!(
            matching_ids(&Expr::col("amount").gt(Expr::lit(Value::Int(0)))),
            vec![1, 2]
        );
        assert_eq!(
            matching_ids(&Expr::col("amount").lt(Expr::lit(Value::Int(0)))),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn is_null_and_is_not_null_find_nulls() {
        assert_eq!(
            matching_ids(&Expr::IsNull(Box::new(Expr::col("country")))),
            vec![3]
        );
        assert_eq!(
            matching_ids(&Expr::IsNotNull(Box::new(Expr::col("country")))),
            vec![1, 2]
        );
    }

    #[test]
    fn kleene_logic_matches_sql() {
        // (amount > 0) AND (country = 'uae'): row 3 is NULL on both sides.
        let p = Expr::col("amount")
            .gt(Expr::lit(Value::Int(0)))
            .and(Expr::col("country").eq(Expr::lit(Value::Str("uae".into()))));
        assert_eq!(matching_ids(&p), vec![1]);

        // OR keeps a row where one side is true and the other unknown.
        let p = Expr::col("id")
            .eq(Expr::lit(Value::Int(3)))
            .or(Expr::col("country").eq(Expr::lit(Value::Str("uae".into()))));
        assert_eq!(matching_ids(&p), vec![1, 3]);

        // NOT of unknown stays unknown, so row 3 is excluded.
        let p = Expr::Not(Box::new(
            Expr::col("country").eq(Expr::lit(Value::Str("uae".into()))),
        ));
        assert_eq!(matching_ids(&p), vec![2]);
    }

    #[test]
    fn in_list_and_between_and_their_negations() {
        let p = Expr::InList {
            expr: Box::new(Expr::col("country")),
            list: vec![Value::Str("uae".into()), Value::Str("uk".into())],
            negated: false,
        };
        assert_eq!(matching_ids(&p), vec![1]);

        let p = Expr::InList {
            expr: Box::new(Expr::col("id")),
            list: vec![Value::Int(1), Value::Int(3)],
            negated: true,
        };
        assert_eq!(matching_ids(&p), vec![2]);

        let p = Expr::Between {
            expr: Box::new(Expr::col("amount")),
            low: Value::Float(50.0),
            high: Value::Float(150.0),
            negated: false,
        };
        assert_eq!(matching_ids(&p), vec![1]);

        let p = Expr::Between {
            expr: Box::new(Expr::col("id")),
            low: Value::Int(2),
            high: Value::Int(3),
            negated: true,
        };
        assert_eq!(matching_ids(&p), vec![1]);
    }

    #[test]
    fn like_matches_patterns() {
        let p = Expr::binary(
            BinaryOp::Like,
            Expr::col("country"),
            Expr::lit(Value::Str("u%".into())),
        );
        assert_eq!(matching_ids(&p), vec![1, 2]);
        let p = Expr::binary(
            BinaryOp::Like,
            Expr::col("country"),
            Expr::lit(Value::Str("_ae".into())),
        );
        assert_eq!(matching_ids(&p), vec![1]);
    }

    #[test]
    fn arithmetic_is_evaluated_inside_predicates() {
        let doubled = Expr::binary(BinaryOp::Mul, Expr::col("amount"), Expr::lit(Value::Int(2)));
        let p = Expr::binary(BinaryOp::Gt, doubled, Expr::lit(Value::Int(400)));
        assert_eq!(matching_ids(&p), vec![2]);
    }

    #[test]
    fn flipped_operands_are_supported() {
        let p = Expr::binary(BinaryOp::Lt, Expr::lit(Value::Int(1)), Expr::col("id"));
        assert_eq!(matching_ids(&p), vec![2, 3]);
    }

    #[test]
    fn column_to_column_comparison_works_across_numeric_types() {
        let p = Expr::binary(BinaryOp::Lt, Expr::col("id"), Expr::col("amount"));
        assert_eq!(matching_ids(&p), vec![1, 2]);
    }

    #[test]
    fn unknown_columns_and_bad_literals_are_errors() {
        let err = eval(&Expr::col("nope"), &batch()).unwrap_err();
        assert_eq!(err.code(), "not_found");
        let err = eval(
            &Expr::col("id").eq(Expr::lit(Value::Str("not a number".into()))),
            &batch(),
        )
        .unwrap_err();
        assert_eq!(err.code(), "type_mismatch");
    }
}
