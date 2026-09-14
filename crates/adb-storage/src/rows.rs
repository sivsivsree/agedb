//! Row plumbing: JSON <-> Arrow <-> Arrow IPC.
//!
//! Agents hand us JSON objects; the engine wants columnar batches; the WAL wants
//! bytes. All three conversions live here so type coercion happens exactly once,
//! at ingest, keeping WAL replay deterministic (see `wal.rs`).

use std::sync::Arc;

use adb_core::{AdbError, ColumnSchema, DataType, Result, TableSchema, Value};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Float64Array,
    Float64Builder, Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowType, TimeUnit};
use serde_json::{Map as JsonMap, Value as Json};

/// Builds an Arrow `RecordBatch` from agent-supplied JSON rows.
///
/// Strict on purpose: unknown columns are an error rather than being dropped,
/// because a typo in a column name is one of the most common agent mistakes and
/// silently discarding data would be the worst possible response to it.
pub struct RowBatchBuilder<'a> {
    schema: &'a TableSchema,
    rows: usize,
    builders: Vec<ColumnBuilder>,
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Date(Date32Builder),
}

impl ColumnBuilder {
    fn new(ty: DataType) -> Self {
        match ty {
            DataType::Bool => Self::Bool(BooleanBuilder::new()),
            DataType::Int64 => Self::Int(Int64Builder::new()),
            DataType::Float64 => Self::Float(Float64Builder::new()),
            DataType::Utf8 | DataType::Uuid | DataType::Json => Self::Str(StringBuilder::new()),
            DataType::Timestamp => Self::Timestamp(TimestampMicrosecondBuilder::new()),
            DataType::Date => Self::Date(Date32Builder::new()),
        }
    }

    fn append(&mut self, value: &Value) -> Result<()> {
        match (self, value) {
            (Self::Bool(b), Value::Null) => b.append_null(),
            (Self::Bool(b), Value::Bool(v)) => b.append_value(*v),
            (Self::Int(b), Value::Null) => b.append_null(),
            (Self::Int(b), Value::Int(v)) => b.append_value(*v),
            (Self::Float(b), Value::Null) => b.append_null(),
            (Self::Float(b), Value::Float(v)) => b.append_value(*v),
            (Self::Float(b), Value::Int(v)) => b.append_value(*v as f64),
            (Self::Str(b), Value::Null) => b.append_null(),
            (Self::Str(b), Value::Str(v)) => b.append_value(v),
            (Self::Timestamp(b), Value::Null) => b.append_null(),
            (Self::Timestamp(b), Value::Timestamp(v)) => b.append_value(*v),
            (Self::Date(b), Value::Null) => b.append_null(),
            (Self::Date(b), Value::Date(v)) => b.append_value(*v),
            (this, other) => {
                return Err(AdbError::TypeMismatch {
                    expected: this.type_name().to_string(),
                    actual: other.type_name().to_string(),
                })
            }
        }
        Ok(())
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::Int(_) => "int64",
            Self::Float(_) => "float64",
            Self::Str(_) => "utf8",
            Self::Timestamp(_) => "timestamp",
            Self::Date(_) => "date",
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Int(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Float(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Str(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Timestamp(b) => Arc::new(b.finish().with_timezone("UTC")) as ArrayRef,
            Self::Date(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

impl<'a> RowBatchBuilder<'a> {
    pub fn new(schema: &'a TableSchema) -> Self {
        let builders = schema
            .columns
            .iter()
            .map(|c| ColumnBuilder::new(c.data_type))
            .collect();
        Self {
            schema,
            rows: 0,
            builders,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Append one JSON object, coercing each field to its declared type.
    pub fn push_json(&mut self, row: &JsonMap<String, Json>) -> Result<()> {
        for key in row.keys() {
            if self.schema.column(key).is_none() {
                return Err(AdbError::not_found(
                    "column",
                    format!("{}.{}", self.schema.name, key),
                ));
            }
        }
        for (idx, col) in self.schema.columns.iter().enumerate() {
            let value = match row.get(&col.name) {
                None | Some(Json::Null) => {
                    if !col.nullable {
                        return Err(AdbError::bad_request(format!(
                            "column {:?} is required but missing or null",
                            col.name
                        )));
                    }
                    Value::Null
                }
                Some(json) => Value::from_json(json, col.data_type).map_err(|e| match e {
                    AdbError::TypeMismatch { expected, actual } => AdbError::TypeMismatch {
                        expected: format!("{} for column {:?}", expected, col.name),
                        actual,
                    },
                    other => other,
                })?,
            };
            self.builders[idx].append(&value)?;
        }
        self.rows += 1;
        Ok(())
    }

    /// Append one already-typed row, in schema column order.
    pub fn push_values(&mut self, values: &[Value]) -> Result<()> {
        if values.len() != self.schema.columns.len() {
            return Err(AdbError::bad_request(format!(
                "expected {} values, got {}",
                self.schema.columns.len(),
                values.len()
            )));
        }
        for (idx, value) in values.iter().enumerate() {
            let col = &self.schema.columns[idx];
            if value.is_null() && !col.nullable {
                return Err(AdbError::bad_request(format!(
                    "column {:?} is required but null",
                    col.name
                )));
            }
            self.builders[idx].append(value)?;
        }
        self.rows += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        RecordBatch::try_new(self.schema.arrow_schema(), arrays).map_err(Into::into)
    }
}

/// Convert JSON rows into a single batch.
pub fn batch_from_json_rows(
    schema: &TableSchema,
    rows: &[JsonMap<String, Json>],
) -> Result<RecordBatch> {
    let mut builder = RowBatchBuilder::new(schema);
    for (i, row) in rows.iter().enumerate() {
        builder.push_json(row).map_err(|e| match e {
            AdbError::BadRequest(msg) => AdbError::BadRequest(format!("row {i}: {msg}")),
            AdbError::TypeMismatch { expected, actual } => AdbError::TypeMismatch {
                expected: format!("{expected} (row {i})"),
                actual,
            },
            other => other,
        })?;
    }
    builder.finish()
}

/// Read one cell as a [`Value`]. Used by statistics, key extraction and group
/// keys, so it must cover every Arrow type our schemas can produce.
pub fn value_at(array: &dyn Array, idx: usize) -> Result<Value> {
    if array.is_null(idx) {
        return Ok(Value::Null);
    }
    let v = match array.data_type() {
        ArrowType::Boolean => Value::Bool(downcast::<BooleanArray>(array)?.value(idx)),
        ArrowType::Int64 => Value::Int(downcast::<Int64Array>(array)?.value(idx)),
        ArrowType::Float64 => Value::Float(downcast::<Float64Array>(array)?.value(idx)),
        ArrowType::Utf8 => Value::Str(downcast::<StringArray>(array)?.value(idx).to_string()),
        ArrowType::Timestamp(TimeUnit::Microsecond, _) => {
            Value::Timestamp(downcast::<TimestampMicrosecondArray>(array)?.value(idx))
        }
        ArrowType::Date32 => Value::Date(downcast::<Date32Array>(array)?.value(idx)),
        other => {
            return Err(AdbError::Internal(format!(
                "unsupported arrow type in value_at: {other}"
            )))
        }
    };
    Ok(v)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        AdbError::Internal(format!("array downcast failed for {}", array.data_type()))
    })
}

/// All values of a column, in row order.
pub fn column_values(array: &dyn Array) -> Result<Vec<Value>> {
    (0..array.len()).map(|i| value_at(array, i)).collect()
}

/// Render a batch as JSON rows for an API response.
///
/// `json`-typed columns come back as strings: the output schema of a query is
/// Arrow, which has no notion of our `json` semantic type. Callers that know the
/// column is JSON can re-parse it.
pub fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<JsonMap<String, Json>>> {
    let mut out = Vec::with_capacity(batch.num_rows());
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for row in 0..batch.num_rows() {
        let mut obj = JsonMap::with_capacity(names.len());
        for (col, name) in names.iter().enumerate() {
            obj.insert(
                name.to_string(),
                value_at(batch.column(col).as_ref(), row)?.to_json(),
            );
        }
        out.push(obj);
    }
    Ok(out)
}

/// Extract the primary-key tuple for one row.
pub fn key_at(batch: &RecordBatch, pk_indices: &[usize], row: usize) -> Result<Vec<Value>> {
    pk_indices
        .iter()
        .map(|&i| value_at(batch.column(i).as_ref(), row))
        .collect()
}

/// Serialize a batch as Arrow IPC (the WAL payload for inserts).
pub fn encode_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size());
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Inverse of [`encode_ipc`]; concatenates if the payload holds several batches.
pub fn decode_ipc(bytes: &[u8]) -> Result<RecordBatch> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().expect("length checked"));
    }
    arrow::compute::concat_batches(&schema, &batches).map_err(Into::into)
}

/// Column-order projection of a batch by name.
pub fn project_batch(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
    let indices = columns
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| AdbError::not_found("column", name))
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(Into::into)
}

/// Empty batch with the shape a query over `schema` projecting `columns` returns.
pub fn empty_batch(schema: &TableSchema, columns: &[String]) -> Result<RecordBatch> {
    let fields = columns
        .iter()
        .map(|name| schema.require_column(name).map(ColumnSchema::arrow_field))
        .collect::<Result<Vec<_>>>()?;
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    Ok(RecordBatch::new_empty(arrow_schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{ColumnSchema, TableName};
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableName::new("events").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64).required(),
                ColumnSchema::new("name", DataType::Utf8),
                ColumnSchema::new("amount", DataType::Float64),
                ColumnSchema::new("at", DataType::Timestamp),
                ColumnSchema::new("day", DataType::Date),
                ColumnSchema::new("ok", DataType::Bool),
            ],
        )
    }

    fn rows(v: Json) -> Vec<JsonMap<String, Json>> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_object().unwrap().clone())
            .collect()
    }

    #[test]
    fn json_rows_become_a_typed_batch() {
        let batch = batch_from_json_rows(
            &schema(),
            &rows(json!([
                {"id": 1, "name": "a", "amount": 1.5, "at": "2026-01-01T00:00:00Z", "day": "2026-01-01", "ok": true},
                {"id": 2}
            ])),
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);
        assert_eq!(
            value_at(batch.column(1).as_ref(), 0).unwrap(),
            Value::Str("a".into())
        );
        assert_eq!(value_at(batch.column(1).as_ref(), 1).unwrap(), Value::Null);
        assert_eq!(
            value_at(batch.column(3).as_ref(), 0).unwrap(),
            Value::Timestamp(1_767_225_600_000_000)
        );
    }

    #[test]
    fn integers_widen_into_float_columns() {
        let batch =
            batch_from_json_rows(&schema(), &rows(json!([{"id": 1, "amount": 3}]))).unwrap();
        assert_eq!(
            value_at(batch.column(2).as_ref(), 0).unwrap(),
            Value::Float(3.0)
        );
    }

    #[test]
    fn unknown_columns_are_rejected_not_dropped() {
        let err =
            batch_from_json_rows(&schema(), &rows(json!([{"id": 1, "nmae": "typo"}]))).unwrap_err();
        assert_eq!(err.code(), "not_found");
        assert!(err.to_string().contains("nmae"));
    }

    #[test]
    fn missing_required_column_names_the_row() {
        let err =
            batch_from_json_rows(&schema(), &rows(json!([{"id": 1}, {"name": "x"}]))).unwrap_err();
        assert!(err.to_string().contains("row 1"), "{err}");
        assert!(err.to_string().contains("\"id\""), "{err}");
    }

    #[test]
    fn ipc_round_trip_preserves_data_and_schema() {
        let batch = batch_from_json_rows(
            &schema(),
            &rows(json!([{"id": 1, "name": "a", "at": "2026-01-01T00:00:00Z"}])),
        )
        .unwrap();
        let decoded = decode_ipc(&encode_ipc(&batch).unwrap()).unwrap();
        assert_eq!(decoded.schema(), batch.schema());
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(
            batch_to_json_rows(&decoded).unwrap(),
            batch_to_json_rows(&batch).unwrap()
        );
    }

    #[test]
    fn json_output_uses_iso_dates() {
        let batch = batch_from_json_rows(
            &schema(),
            &rows(json!([{"id": 1, "at": 1767225600, "day": "2026-01-02"}])),
        )
        .unwrap();
        let out = batch_to_json_rows(&batch).unwrap();
        assert_eq!(out[0]["at"], json!("2026-01-01T00:00:00.000000Z"));
        assert_eq!(out[0]["day"], json!("2026-01-02"));
    }

    #[test]
    fn projection_and_empty_batches_follow_the_requested_order() {
        let batch =
            batch_from_json_rows(&schema(), &rows(json!([{"id": 1, "name": "a"}]))).unwrap();
        let cols = vec!["name".to_string(), "id".to_string()];
        let projected = project_batch(&batch, &cols).unwrap();
        assert_eq!(projected.schema().field(0).name(), "name");
        let empty = empty_batch(&schema(), &cols).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema().fields().len(), 2);
        assert_eq!(empty.schema().field(1).name(), "id");
    }

    #[test]
    fn key_extraction_reads_pk_columns_in_key_order() {
        let batch =
            batch_from_json_rows(&schema(), &rows(json!([{"id": 7, "name": "a"}]))).unwrap();
        assert_eq!(
            key_at(&batch, &[1, 0], 0).unwrap(),
            vec![Value::Str("a".into()), Value::Int(7)]
        );
    }
}
