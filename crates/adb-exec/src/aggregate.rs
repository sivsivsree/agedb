//! Hash aggregation with partial/final split.
//!
//! Each partition builds its own hash table and the coordinator merges them
//! (see "Concurrency" in ARCHITECTURE.md), so a `GROUP BY` never ships raw rows
//! between partitions,
//! only one entry per group. `AVG` carries `(sum, count)` so merging stays exact
//! instead of averaging averages.
//!
//! The ungrouped case takes a vectorized fast path through Arrow's aggregate
//! kernels; grouped aggregation walks rows because the group key is dynamically
//! typed. That row loop is the main known performance gap in v0.1.

use std::collections::HashMap;

use adb_core::{AdbError, DataType, Result, Value};
use adb_planner::{AggregateFunc, AggregateSpec, ColumnMeta, OutputSchema};
use adb_storage::rows;
use arrow::array::{Array, ArrayRef, RecordBatch};

#[derive(Debug, Clone)]
enum AggState {
    /// `count(*)` or `count(col)`; the latter ignores nulls.
    Count(u64),
    SumInt {
        acc: i64,
        any: bool,
    },
    SumFloat {
        acc: f64,
        any: bool,
    },
    Avg {
        sum: f64,
        count: u64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggState {
    fn new(func: AggregateFunc, ty: Option<DataType>) -> Self {
        match func {
            AggregateFunc::Count => Self::Count(0),
            AggregateFunc::Sum => match ty {
                Some(DataType::Int64) => Self::SumInt { acc: 0, any: false },
                _ => Self::SumFloat {
                    acc: 0.0,
                    any: false,
                },
            },
            AggregateFunc::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunc::Min => Self::Min(None),
            AggregateFunc::Max => Self::Max(None),
        }
    }

    fn update(&mut self, value: Option<&Value>) {
        match self {
            Self::Count(n) => {
                // `None` means count(*): every row counts. A NULL column value
                // does not.
                match value {
                    None => *n += 1,
                    Some(v) if !v.is_null() => *n += 1,
                    Some(_) => {}
                }
            }
            Self::SumInt { acc, any } => {
                if let Some(v) = value.and_then(|v| v.as_i64()) {
                    *acc = acc.saturating_add(v);
                    *any = true;
                }
            }
            Self::SumFloat { acc, any } => {
                if let Some(v) = value.and_then(|v| v.as_f64()) {
                    *acc += v;
                    *any = true;
                }
            }
            Self::Avg { sum, count } => {
                if let Some(v) = value.and_then(|v| v.as_f64()) {
                    *sum += v;
                    *count += 1;
                }
            }
            Self::Min(current) => {
                if let Some(v) = value.filter(|v| !v.is_null()) {
                    if current.as_ref().map(|c| v < c).unwrap_or(true) {
                        *current = Some(v.clone());
                    }
                }
            }
            Self::Max(current) => {
                if let Some(v) = value.filter(|v| !v.is_null()) {
                    if current.as_ref().map(|c| v > c).unwrap_or(true) {
                        *current = Some(v.clone());
                    }
                }
            }
        }
    }

    fn merge(&mut self, other: &AggState) -> Result<()> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => *a += b,
            (Self::SumInt { acc, any }, Self::SumInt { acc: b, any: b_any }) => {
                *acc = acc.saturating_add(*b);
                *any |= b_any;
            }
            (Self::SumFloat { acc, any }, Self::SumFloat { acc: b, any: b_any }) => {
                *acc += b;
                *any |= b_any;
            }
            (
                Self::Avg { sum, count },
                Self::Avg {
                    sum: b_sum,
                    count: b_count,
                },
            ) => {
                *sum += b_sum;
                *count += b_count;
            }
            (Self::Min(a), Self::Min(b)) => {
                if let Some(b) = b {
                    if a.as_ref().map(|x| b < x).unwrap_or(true) {
                        *a = Some(b.clone());
                    }
                }
            }
            (Self::Max(a), Self::Max(b)) => {
                if let Some(b) = b {
                    if a.as_ref().map(|x| b > x).unwrap_or(true) {
                        *a = Some(b.clone());
                    }
                }
            }
            _ => {
                return Err(AdbError::Internal(
                    "merging incompatible aggregate states".into(),
                ))
            }
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        match self {
            Self::Count(n) => Value::Int(*n as i64),
            Self::SumInt { acc, any } => {
                if *any {
                    Value::Int(*acc)
                } else {
                    Value::Null
                }
            }
            Self::SumFloat { acc, any } => {
                if *any {
                    Value::Float(*acc)
                } else {
                    Value::Null
                }
            }
            Self::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float(sum / *count as f64)
                }
            }
            Self::Min(v) | Self::Max(v) => v.clone().unwrap_or(Value::Null),
        }
    }
}

/// Accumulates one partition's contribution to an aggregation.
#[derive(Debug)]
pub struct Aggregator {
    spec: AggregateSpec,
    /// Declared types of the group columns and aggregate arguments.
    argument_types: Vec<Option<DataType>>,
    lookup: HashMap<Vec<Value>, usize>,
    /// Insertion-ordered groups, so unsorted output is at least deterministic.
    groups: Vec<(Vec<Value>, Vec<AggState>)>,
    output: OutputSchema,
}

impl Aggregator {
    pub fn new(spec: &AggregateSpec, input: &OutputSchema, output: &OutputSchema) -> Self {
        let argument_types = spec
            .aggregates
            .iter()
            .map(|agg| {
                agg.column
                    .as_ref()
                    .and_then(|c| input.column(c))
                    .map(|meta| meta.data_type)
            })
            .collect();
        Self {
            spec: spec.clone(),
            argument_types,
            lookup: HashMap::new(),
            groups: Vec::new(),
            output: output.clone(),
        }
    }

    pub fn is_grouped(&self) -> bool {
        !self.spec.group_by.is_empty()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    fn fresh_states(&self) -> Vec<AggState> {
        self.spec
            .aggregates
            .iter()
            .zip(&self.argument_types)
            .map(|(agg, ty)| AggState::new(agg.func, *ty))
            .collect()
    }

    /// Fold a batch of input rows into the table.
    pub fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let group_arrays = self
            .spec
            .group_by
            .iter()
            .map(|name| column_by_name(batch, name))
            .collect::<Result<Vec<_>>>()?;
        let value_arrays = self
            .spec
            .aggregates
            .iter()
            .map(|agg| match &agg.column {
                None => Ok(None),
                Some(name) => column_by_name(batch, name).map(Some),
            })
            .collect::<Result<Vec<_>>>()?;

        // Ungrouped aggregation has a single state vector; skip hashing entirely.
        if group_arrays.is_empty() {
            if self.groups.is_empty() {
                let states = self.fresh_states();
                self.groups.push((Vec::new(), states));
                self.lookup.insert(Vec::new(), 0);
            }
            let states = &mut self.groups[0].1;
            for (slot, array) in states.iter_mut().zip(&value_arrays) {
                accumulate_column(slot, array.as_deref(), batch.num_rows())?;
            }
            return Ok(());
        }

        // Pre-materialize group columns so the row loop does not re-dispatch on
        // Arrow types for every cell.
        let group_values = group_arrays
            .iter()
            .map(|array| rows::column_values(array.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let value_values = value_arrays
            .iter()
            .map(|array| match array {
                None => Ok(None),
                Some(array) => rows::column_values(array.as_ref()).map(Some),
            })
            .collect::<Result<Vec<_>>>()?;

        for row in 0..batch.num_rows() {
            let key: Vec<Value> = group_values.iter().map(|col| col[row].clone()).collect();
            let index = match self.lookup.get(&key) {
                Some(index) => *index,
                None => {
                    let index = self.groups.len();
                    let states = self.fresh_states();
                    self.groups.push((key.clone(), states));
                    self.lookup.insert(key, index);
                    index
                }
            };
            let states = &mut self.groups[index].1;
            for (slot, values) in states.iter_mut().zip(&value_values) {
                match values {
                    None => slot.update(None),
                    Some(column) => slot.update(Some(&column[row])),
                }
            }
        }
        Ok(())
    }

    /// Fold another partition's partial result into this one.
    pub fn merge(&mut self, other: Aggregator) -> Result<()> {
        for (key, states) in other.groups {
            match self.lookup.get(&key) {
                Some(index) => {
                    let target = &mut self.groups[*index].1;
                    for (slot, state) in target.iter_mut().zip(&states) {
                        slot.merge(state)?;
                    }
                }
                None => {
                    let index = self.groups.len();
                    self.lookup.insert(key.clone(), index);
                    self.groups.push((key, states));
                }
            }
        }
        Ok(())
    }

    /// Produce the result batch: group columns followed by aggregate columns.
    pub fn finish(mut self) -> Result<RecordBatch> {
        // An ungrouped aggregation over zero rows still returns one row
        // (`count = 0`), which is what SQL and every agent expects.
        if self.groups.is_empty() && !self.is_grouped() {
            let states = self.fresh_states();
            self.groups.push((Vec::new(), states));
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.output.columns.len());
        for (position, _key_column) in self.spec.group_by.iter().enumerate() {
            let meta = &self.output.columns[position];
            let values: Vec<Value> = self
                .groups
                .iter()
                .map(|(key, _)| key[position].clone())
                .collect();
            arrays.push(build_array(&values, meta.data_type)?);
        }
        for (position, _agg) in self.spec.aggregates.iter().enumerate() {
            let meta = &self.output.columns[self.spec.group_by.len() + position];
            let values: Vec<Value> = self
                .groups
                .iter()
                .map(|(_, states)| states[position].finish())
                .collect();
            arrays.push(build_array(&values, meta.data_type)?);
        }

        let fields = self
            .output
            .columns
            .iter()
            .map(|meta: &ColumnMeta| {
                arrow::datatypes::Field::new(&meta.name, meta.data_type.arrow_type(), true)
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            arrays,
        )
        .map_err(Into::into)
    }
}

/// Vectorized accumulation for the ungrouped case.
fn accumulate_column(state: &mut AggState, array: Option<&dyn Array>, rows: usize) -> Result<()> {
    use arrow::compute::kernels::aggregate;
    use arrow::datatypes::{Float64Type, Int64Type};

    let Some(array) = array else {
        // count(*): no column to look at.
        if let AggState::Count(n) = state {
            *n += rows as u64;
        }
        return Ok(());
    };

    match state {
        AggState::Count(n) => {
            *n += (array.len() - array.null_count()) as u64;
            Ok(())
        }
        AggState::SumInt { acc, any } => {
            if let Some(typed) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
                if let Some(sum) = aggregate::sum::<Int64Type>(typed) {
                    *acc = acc.saturating_add(sum);
                    *any = true;
                }
                return Ok(());
            }
            fallback_rows(state, array)
        }
        AggState::SumFloat { acc, any } => {
            if let Some(typed) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                if let Some(sum) = aggregate::sum::<Float64Type>(typed) {
                    *acc += sum;
                    *any = true;
                }
                return Ok(());
            }
            fallback_rows(state, array)
        }
        AggState::Avg { sum, count } => {
            if let Some(typed) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                if let Some(batch_sum) = aggregate::sum::<Float64Type>(typed) {
                    *sum += batch_sum;
                }
                *count += (array.len() - array.null_count()) as u64;
                return Ok(());
            }
            if let Some(typed) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
                if let Some(batch_sum) = aggregate::sum::<Int64Type>(typed) {
                    *sum += batch_sum as f64;
                }
                *count += (array.len() - array.null_count()) as u64;
                return Ok(());
            }
            fallback_rows(state, array)
        }
        // Min/Max over dynamically typed values: fall back to the row path,
        // which is cheap relative to a scan and always correct.
        AggState::Min(_) | AggState::Max(_) => fallback_rows(state, array),
    }
}

fn fallback_rows(state: &mut AggState, array: &dyn Array) -> Result<()> {
    for value in rows::column_values(array)? {
        state.update(Some(&value));
    }
    Ok(())
}

fn column_by_name(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| AdbError::not_found("column", name))?;
    Ok(batch.column(index).clone())
}

/// Build a typed Arrow array from dynamically typed values.
pub fn build_array(values: &[Value], ty: DataType) -> Result<ArrayRef> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    };
    use std::sync::Arc;

    macro_rules! build {
        ($builder:expr, $push:expr) => {{
            let mut builder = $builder;
            for value in values {
                if value.is_null() {
                    builder.append_null();
                } else {
                    #[allow(clippy::redundant_closure_call)]
                    $push(&mut builder, value)?;
                }
            }
            builder
        }};
    }

    Ok(match ty {
        DataType::Bool => {
            let b = build!(
                BooleanBuilder::new(),
                |b: &mut BooleanBuilder, v: &Value| {
                    b.append_value(v.as_bool().ok_or_else(|| type_error(v, "bool"))?);
                    Ok::<(), AdbError>(())
                }
            );
            Arc::new({
                let mut b = b;
                b.finish()
            })
        }
        DataType::Int64 => {
            let b = build!(Int64Builder::new(), |b: &mut Int64Builder, v: &Value| {
                b.append_value(v.as_i64().ok_or_else(|| type_error(v, "int64"))?);
                Ok::<(), AdbError>(())
            });
            Arc::new({
                let mut b = b;
                b.finish()
            })
        }
        DataType::Float64 => {
            let b = build!(
                Float64Builder::new(),
                |b: &mut Float64Builder, v: &Value| {
                    b.append_value(v.as_f64().ok_or_else(|| type_error(v, "float64"))?);
                    Ok::<(), AdbError>(())
                }
            );
            Arc::new({
                let mut b = b;
                b.finish()
            })
        }
        DataType::Utf8 | DataType::Uuid | DataType::Json => {
            let b = build!(StringBuilder::new(), |b: &mut StringBuilder, v: &Value| {
                b.append_value(v.as_str().ok_or_else(|| type_error(v, "utf8"))?);
                Ok::<(), AdbError>(())
            });
            Arc::new({
                let mut b = b;
                b.finish()
            })
        }
        DataType::Timestamp => {
            let b = build!(
                TimestampMicrosecondBuilder::new(),
                |b: &mut TimestampMicrosecondBuilder, v: &Value| {
                    b.append_value(v.as_i64().ok_or_else(|| type_error(v, "timestamp"))?);
                    Ok::<(), AdbError>(())
                }
            );
            Arc::new({
                let mut b = b;
                b.finish().with_timezone("UTC")
            })
        }
        DataType::Date => {
            let b = build!(Date32Builder::new(), |b: &mut Date32Builder, v: &Value| {
                let days = v.as_i64().ok_or_else(|| type_error(v, "date"))?;
                b.append_value(days as i32);
                Ok::<(), AdbError>(())
            });
            Arc::new({
                let mut b = b;
                b.finish()
            })
        }
    })
}

fn type_error(value: &Value, expected: &str) -> AdbError {
    AdbError::TypeMismatch {
        expected: expected.to_string(),
        actual: format!("{value}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{ColumnSchema, TableName, TableSchema};
    use adb_planner::{AggregateExpr, Query};
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("country", DataType::Utf8),
                ColumnSchema::new("amount", DataType::Float64),
                ColumnSchema::new("qty", DataType::Int64),
            ],
        )
    }

    fn batch(value: serde_json::Value) -> RecordBatch {
        let rows: Vec<_> = value
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_object().unwrap().clone())
            .collect();
        rows::batch_from_json_rows(&schema(), &rows).unwrap()
    }

    fn plan(
        group_by: Vec<&str>,
        aggregates: Vec<AggregateExpr>,
    ) -> (AggregateSpec, OutputSchema, OutputSchema) {
        let query = Query::scan(TableName::new("orders").unwrap())
            .aggregate(group_by.iter().map(|s| s.to_string()).collect(), aggregates);
        let validated =
            adb_planner::validate(&query, &schema(), &adb_core::QueryLimits::unlimited()).unwrap();
        let input = adb_planner::validate(
            &Query::scan(TableName::new("orders").unwrap()),
            &schema(),
            &adb_core::QueryLimits::unlimited(),
        )
        .unwrap()
        .schema;
        let physical = adb_planner::physical::build(&validated).unwrap();
        (physical.aggregate.clone().unwrap(), input, validated.schema)
    }

    fn to_json(batch: RecordBatch) -> Vec<serde_json::Map<String, serde_json::Value>> {
        rows::batch_to_json_rows(&batch).unwrap()
    }

    #[test]
    fn ungrouped_aggregation_uses_the_vectorized_path() {
        let (spec, input, output) = plan(
            vec![],
            vec![
                AggregateExpr::count_star("n"),
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "total"),
                AggregateExpr::new(AggregateFunc::Avg, Some("amount".into()), "mean"),
                AggregateExpr::new(AggregateFunc::Min, Some("qty".into()), "smallest"),
                AggregateExpr::new(AggregateFunc::Max, Some("qty".into()), "largest"),
            ],
        );
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(json!([
            {"country": "uae", "amount": 100.0, "qty": 2},
            {"country": "usa", "amount": 200.0, "qty": 5},
            {"country": "uk", "amount": null, "qty": null}
        ])))
        .unwrap();
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["n"], json!(3));
        assert_eq!(out[0]["total"], json!(300.0));
        assert_eq!(out[0]["mean"], json!(150.0));
        assert_eq!(out[0]["smallest"], json!(2));
        assert_eq!(out[0]["largest"], json!(5));
    }

    #[test]
    fn count_of_a_column_ignores_nulls_but_count_star_does_not() {
        let (spec, input, output) = plan(
            vec![],
            vec![
                AggregateExpr::count_star("rows"),
                AggregateExpr::new(AggregateFunc::Count, Some("amount".into()), "with_amount"),
            ],
        );
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(json!([
            {"country": "a", "amount": 1.0},
            {"country": "b", "amount": null}
        ])))
        .unwrap();
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out[0]["rows"], json!(2));
        assert_eq!(out[0]["with_amount"], json!(1));
    }

    #[test]
    fn grouping_keeps_one_row_per_key_in_insertion_order() {
        let (spec, input, output) = plan(
            vec!["country"],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("amount".into()),
                "revenue",
            )],
        );
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(json!([
            {"country": "usa", "amount": 10.0},
            {"country": "uae", "amount": 20.0},
            {"country": "usa", "amount": 5.0}
        ])))
        .unwrap();
        assert_eq!(agg.group_count(), 2);
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out[0]["country"], json!("usa"));
        assert_eq!(out[0]["revenue"], json!(15.0));
        assert_eq!(out[1]["country"], json!("uae"));
        assert_eq!(out[1]["revenue"], json!(20.0));
    }

    #[test]
    fn null_group_keys_form_their_own_group() {
        let (spec, input, output) = plan(vec!["country"], vec![AggregateExpr::count_star("n")]);
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(json!([
            {"country": null, "amount": 1.0},
            {"country": null, "amount": 2.0},
            {"country": "uae", "amount": 3.0}
        ])))
        .unwrap();
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["country"], json!(null));
        assert_eq!(out[0]["n"], json!(2));
    }

    #[test]
    fn merging_partials_is_exact_including_avg() {
        let (spec, input, output) = plan(
            vec!["country"],
            vec![
                AggregateExpr::new(AggregateFunc::Avg, Some("amount".into()), "mean"),
                AggregateExpr::new(AggregateFunc::Sum, Some("qty".into()), "units"),
                AggregateExpr::new(AggregateFunc::Min, Some("amount".into()), "low"),
            ],
        );
        let mut left = Aggregator::new(&spec, &input, &output);
        left.update(&batch(json!([
            {"country": "uae", "amount": 10.0, "qty": 1},
            {"country": "uae", "amount": 20.0, "qty": 2}
        ])))
        .unwrap();
        let mut right = Aggregator::new(&spec, &input, &output);
        right
            .update(&batch(json!([
                {"country": "uae", "amount": 60.0, "qty": 3},
                {"country": "usa", "amount": 5.0, "qty": 4}
            ])))
            .unwrap();
        left.merge(right).unwrap();
        let out = to_json(left.finish().unwrap());
        // (10 + 20 + 60) / 3 = 30, not the average of two partition averages.
        assert_eq!(out[0]["country"], json!("uae"));
        assert_eq!(out[0]["mean"], json!(30.0));
        assert_eq!(out[0]["units"], json!(6));
        assert_eq!(out[0]["low"], json!(10.0));
        assert_eq!(out[1]["country"], json!("usa"));
        assert_eq!(out[1]["mean"], json!(5.0));
    }

    #[test]
    fn sums_over_only_nulls_are_null_but_counts_are_zero() {
        let (spec, input, output) = plan(
            vec![],
            vec![
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "total"),
                AggregateExpr::new(AggregateFunc::Avg, Some("amount".into()), "mean"),
                AggregateExpr::count_star("n"),
            ],
        );
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(json!([{"country": "a", "amount": null}])))
            .unwrap();
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out[0]["total"], json!(null));
        assert_eq!(out[0]["mean"], json!(null));
        assert_eq!(out[0]["n"], json!(1));
    }

    #[test]
    fn an_empty_ungrouped_aggregation_still_returns_a_row() {
        let (spec, input, output) = plan(vec![], vec![AggregateExpr::count_star("n")]);
        let agg = Aggregator::new(&spec, &input, &output);
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["n"], json!(0));
    }

    #[test]
    fn an_empty_grouped_aggregation_returns_no_rows() {
        let (spec, input, output) = plan(vec!["country"], vec![AggregateExpr::count_star("n")]);
        let agg = Aggregator::new(&spec, &input, &output);
        assert_eq!(agg.finish().unwrap().num_rows(), 0);
    }

    #[test]
    fn integer_sums_stay_integers() {
        let (spec, input, output) = plan(
            vec![],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("qty".into()),
                "units",
            )],
        );
        let mut agg = Aggregator::new(&spec, &input, &output);
        agg.update(&batch(
            json!([{"country": "a", "qty": 2}, {"country": "b", "qty": 3}]),
        ))
        .unwrap();
        let out = to_json(agg.finish().unwrap());
        assert_eq!(out[0]["units"], json!(5));
    }
}
