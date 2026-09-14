//! Immutable column segments (see "Segments and pruning" in ARCHITECTURE.md).
//!
//! A segment is one Parquet file plus the statistics needed to *skip* it:
//! per-column min/max, null count, and an optional bloom filter. Those
//! statistics are mirrored into the manifest, so pruning a segment costs no I/O
//! at all, which is the whole reason analytical column stores are fast.

use std::collections::BTreeMap;
use std::sync::Arc;

use adb_core::{AdbError, DataType, Result, TableSchema, Value};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::bloom::{self, BloomFilter};
use crate::object_store::ObjectStore;
use crate::rows;

/// Rows per Parquet row group. Smaller groups mean finer-grained reads; larger
/// ones compress better. 128k is a middle ground for analytical scans.
const ROW_GROUP_ROWS: usize = 128 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Smallest non-null value, if any.
    pub min: Option<Value>,
    /// Largest non-null value, if any.
    pub max: Option<Value>,
    pub null_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bloom: Option<BloomFilter>,
}

impl ColumnStats {
    fn empty() -> Self {
        Self {
            min: None,
            max: None,
            null_count: 0,
            bloom: None,
        }
    }

    /// True when no non-null value is present.
    pub fn all_null(&self) -> bool {
        self.min.is_none()
    }
}

/// Everything the planner needs to decide whether to read a segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: u64,
    /// Object key of the Parquet file.
    pub key: String,
    pub row_count: u64,
    /// Size of the Parquet file on disk.
    pub bytes: u64,
    /// Schema version the segment was written with.
    pub schema_version: u32,
    pub columns: BTreeMap<String, ColumnStats>,
}

impl SegmentMeta {
    pub fn stats(&self, column: &str) -> Option<&ColumnStats> {
        self.columns.get(column)
    }
}

/// Compute per-column statistics for a batch about to be written.
pub fn compute_stats(
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<BTreeMap<String, ColumnStats>> {
    let mut out = BTreeMap::new();
    for (idx, col) in schema.columns.iter().enumerate() {
        if idx >= batch.num_columns() {
            break;
        }
        let array = batch.column(idx);
        let values = rows::column_values(array.as_ref())?;
        let mut stats = ColumnStats::empty();
        for v in &values {
            if v.is_null() {
                stats.null_count += 1;
                continue;
            }
            // JSON has no meaningful order, so we keep only null counts for it.
            if col.data_type == DataType::Json {
                continue;
            }
            match &stats.min {
                Some(min) if v >= min => {}
                _ => stats.min = Some(v.clone()),
            }
            match &stats.max {
                Some(max) if v <= max => {}
                _ => stats.max = Some(v.clone()),
            }
        }
        if col.wants_bloom_filter() && bloom::is_filterable(col.data_type) {
            stats.bloom = bloom::build(values.into_iter());
        }
        out.insert(col.name.clone(), stats);
    }
    Ok(out)
}

/// Write `batch` as a new segment and return its metadata.
pub fn write_segment(
    store: &dyn ObjectStore,
    key: &str,
    id: u64,
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<SegmentMeta> {
    let columns = compute_stats(schema, batch)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).map_err(|e| AdbError::internal(format!("zstd level: {e}")))?,
        ))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| AdbError::storage(format!("parquet writer: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| AdbError::storage(format!("parquet write: {e}")))?;
        writer
            .close()
            .map_err(|e| AdbError::storage(format!("parquet close: {e}")))?;
    }
    let bytes = buf.len() as u64;
    // A segment file is immutable, so a plain durable put is enough; the
    // manifest rename is what publishes it.
    store.put(key, &buf)?;

    Ok(SegmentMeta {
        id,
        key: key.to_string(),
        row_count: batch.num_rows() as u64,
        bytes,
        schema_version: schema.version,
        columns,
    })
}

/// Read a segment, optionally projecting columns.
///
/// Columns absent from the file (added by a later schema evolution) are filled
/// with nulls so callers always get the requested shape.
pub fn read_segment(
    store: &dyn ObjectStore,
    meta: &SegmentMeta,
    schema: &TableSchema,
    projection: Option<&[String]>,
) -> Result<RecordBatch> {
    let bytes = store.get(&meta.key)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .map_err(|e| AdbError::storage(format!("parquet open {}: {e}", meta.key)))?;
    let file_schema = reader.schema().clone();
    let reader = reader
        .with_batch_size(8192)
        .build()
        .map_err(|e| AdbError::storage(format!("parquet reader {}: {e}", meta.key)))?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| AdbError::storage(format!("parquet read {}: {e}", meta.key)))?;
    let batch = if batches.is_empty() {
        RecordBatch::new_empty(file_schema)
    } else if batches.len() == 1 {
        batches.into_iter().next().expect("length checked")
    } else {
        let schema_ref = batches[0].schema();
        arrow::compute::concat_batches(&schema_ref, &batches)?
    };

    match projection {
        None => Ok(batch),
        Some(cols) => project_with_backfill(&batch, schema, cols),
    }
}

/// Project `columns` out of `batch`, backfilling nulls for columns the segment
/// predates.
pub fn project_with_backfill(
    batch: &RecordBatch,
    schema: &TableSchema,
    columns: &[String],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for name in columns {
        let col = schema.require_column(name)?;
        fields.push(Arc::new(col.arrow_field()));
        match batch.schema().index_of(name) {
            Ok(idx) => arrays.push(batch.column(idx).clone()),
            Err(_) => arrays.push(arrow::array::new_null_array(
                &col.data_type.arrow_type(),
                batch.num_rows(),
            )),
        }
    }
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(
        fields.into_iter().map(|f| (*f).clone()).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(arrow_schema, arrays).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::LocalFsStore;
    use adb_core::{ColumnSchema, SemanticType, TableName};
    use serde_json::json;
    use tempfile::TempDir;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("amount", DataType::Float64),
                ColumnSchema::new("at", DataType::Timestamp),
            ],
        )
    }

    fn batch(schema: &TableSchema) -> RecordBatch {
        let rows: Vec<_> = json!([
            {"id": 1, "country": "uae", "amount": 12000.0, "at": "2026-01-01T00:00:00Z"},
            {"id": 2, "country": "usa", "amount": 8400.0, "at": "2026-02-01T00:00:00Z"},
            {"id": 3, "country": null, "amount": null, "at": "2026-03-01T00:00:00Z"}
        ])
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
        rows::batch_from_json_rows(schema, &rows).unwrap()
    }

    #[test]
    fn stats_capture_min_max_and_nulls() {
        let schema = schema();
        let stats = compute_stats(&schema, &batch(&schema)).unwrap();
        assert_eq!(stats["id"].min, Some(Value::Int(1)));
        assert_eq!(stats["id"].max, Some(Value::Int(3)));
        assert_eq!(stats["id"].null_count, 0);
        assert_eq!(stats["country"].null_count, 1);
        assert_eq!(stats["country"].min, Some(Value::Str("uae".into())));
        assert_eq!(stats["amount"].max, Some(Value::Float(12000.0)));
        assert_eq!(
            stats["at"].min,
            Some(Value::Timestamp(1_767_225_600_000_000))
        );
    }

    #[test]
    fn bloom_filters_are_built_only_for_filterable_semantic_columns() {
        let schema = schema();
        let stats = compute_stats(&schema, &batch(&schema)).unwrap();
        // `id` is a semantic id of type int64: filterable.
        let id_bloom = stats["id"]
            .bloom
            .as_ref()
            .expect("id should have a bloom filter");
        assert!(id_bloom.contains(&Value::Int(2)));
        assert!(!id_bloom.contains(&Value::Int(999)));
        // `country` is a category: filterable.
        assert!(stats["country"].bloom.is_some());
        // `amount` is a plain float: never filtered.
        assert!(stats["amount"].bloom.is_none());
    }

    #[test]
    fn segment_round_trips_through_parquet() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let schema = schema();
        let original = batch(&schema);
        let meta =
            write_segment(&store, "t/p0/segments/seg-1.parquet", 1, &schema, &original).unwrap();
        assert_eq!(meta.row_count, 3);
        assert!(meta.bytes > 0);

        let read = read_segment(&store, &meta, &schema, None).unwrap();
        assert_eq!(read.num_rows(), 3);
        assert_eq!(
            rows::batch_to_json_rows(&read).unwrap(),
            rows::batch_to_json_rows(&original).unwrap()
        );
    }

    #[test]
    fn projection_selects_and_orders_columns() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let schema = schema();
        let meta = write_segment(&store, "s.parquet", 1, &schema, &batch(&schema)).unwrap();
        let cols = vec!["country".to_string(), "id".to_string()];
        let read = read_segment(&store, &meta, &schema, Some(&cols)).unwrap();
        assert_eq!(read.num_columns(), 2);
        assert_eq!(read.schema().field(0).name(), "country");
        assert_eq!(read.schema().field(1).name(), "id");
    }

    #[test]
    fn columns_added_after_the_segment_was_written_read_back_as_null() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let old_schema = schema();
        let meta = write_segment(&store, "s.parquet", 1, &old_schema, &batch(&old_schema)).unwrap();

        let mut new_schema = old_schema.clone();
        new_schema
            .columns
            .push(ColumnSchema::new("note", DataType::Utf8));
        new_schema.version = 2;

        let cols = vec!["id".to_string(), "note".to_string()];
        let read = read_segment(&store, &meta, &new_schema, Some(&cols)).unwrap();
        assert_eq!(read.num_rows(), 3);
        assert!(read.column(1).is_null(0));
    }
}
