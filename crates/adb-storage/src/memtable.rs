//! In-memory write buffer.
//!
//! Rows land here as Arrow batches after being logged, and are flushed to an
//! immutable segment once a threshold is crossed (see "Writes, updates and deletes" in ARCHITECTURE.md). Batches are
//! kept as-is rather than concatenated on every append: concatenation is O(rows)
//! and would turn bulk ingest quadratic.

use std::sync::Arc;

use adb_core::{Result, TableSchema};
use arrow::array::RecordBatch;

#[derive(Debug)]
pub struct Memtable {
    schema: Arc<TableSchema>,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
}

impl Memtable {
    pub fn new(schema: Arc<TableSchema>) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
        }
    }

    pub fn append(&mut self, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        self.rows += batch.num_rows();
        self.bytes += batch.get_array_memory_size();
        self.batches.push(batch);
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Batches paired with the global row ordinal each one starts at.
    ///
    /// Ordinals are how suppression bitmaps and the key index address memtable
    /// rows, so this pairing is the bridge between the two.
    pub fn batches_with_offsets(&self) -> Vec<(u32, &RecordBatch)> {
        let mut out = Vec::with_capacity(self.batches.len());
        let mut offset = 0u32;
        for batch in &self.batches {
            out.push((offset, batch));
            offset += batch.num_rows() as u32;
        }
        out
    }

    /// Resolve a global ordinal to `(batch index, row within batch)`.
    pub fn locate(&self, ordinal: u32) -> Option<(usize, usize)> {
        let mut start = 0u32;
        for (idx, batch) in self.batches.iter().enumerate() {
            let end = start + batch.num_rows() as u32;
            if ordinal < end {
                return Some((idx, (ordinal - start) as usize));
            }
            start = end;
        }
        None
    }

    /// Everything buffered, as one batch (`None` when empty).
    pub fn concat(&self) -> Result<Option<RecordBatch>> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        if self.batches.len() == 1 {
            return Ok(Some(self.batches[0].clone()));
        }
        let schema = self.schema.arrow_schema();
        Ok(Some(arrow::compute::concat_batches(
            &schema,
            &self.batches,
        )?))
    }

    pub fn clear(&mut self) {
        self.batches.clear();
        self.rows = 0;
        self.bytes = 0;
    }

    /// Replace the schema after an accepted evolution. Existing batches keep
    /// their old shape; reads backfill missing columns with nulls.
    pub fn set_schema(&mut self, schema: Arc<TableSchema>) {
        self.schema = schema;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows;
    use adb_core::{ColumnSchema, DataType, TableName};
    use serde_json::json;

    fn schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::new(
            TableName::new("t").unwrap(),
            vec![ColumnSchema::new("id", DataType::Int64).required()],
        ))
    }

    fn batch(schema: &TableSchema, ids: &[i64]) -> RecordBatch {
        let rows: Vec<_> = ids
            .iter()
            .map(|id| json!({"id": id}).as_object().unwrap().clone())
            .collect();
        rows::batch_from_json_rows(schema, &rows).unwrap()
    }

    #[test]
    fn tracks_rows_and_bytes() {
        let schema = schema();
        let mut mt = Memtable::new(schema.clone());
        assert!(mt.is_empty());
        mt.append(batch(&schema, &[1, 2, 3]));
        mt.append(batch(&schema, &[4]));
        assert_eq!(mt.rows(), 4);
        assert!(mt.bytes() > 0);
        assert_eq!(mt.batches().len(), 2);
    }

    #[test]
    fn empty_batches_are_ignored() {
        let schema = schema();
        let mut mt = Memtable::new(schema.clone());
        mt.append(batch(&schema, &[]));
        assert!(mt.is_empty());
        assert_eq!(mt.batches().len(), 0);
    }

    #[test]
    fn ordinals_address_rows_across_batches() {
        let schema = schema();
        let mut mt = Memtable::new(schema.clone());
        mt.append(batch(&schema, &[1, 2, 3]));
        mt.append(batch(&schema, &[4, 5]));
        assert_eq!(mt.locate(0), Some((0, 0)));
        assert_eq!(mt.locate(2), Some((0, 2)));
        assert_eq!(mt.locate(3), Some((1, 0)));
        assert_eq!(mt.locate(4), Some((1, 1)));
        assert_eq!(mt.locate(5), None);
        assert_eq!(
            mt.batches_with_offsets()
                .iter()
                .map(|(o, _)| *o)
                .collect::<Vec<_>>(),
            vec![0, 3]
        );
    }

    #[test]
    fn concat_preserves_arrival_order() {
        let schema = schema();
        let mut mt = Memtable::new(schema.clone());
        mt.append(batch(&schema, &[1, 2]));
        mt.append(batch(&schema, &[3]));
        let all = mt.concat().unwrap().unwrap();
        assert_eq!(all.num_rows(), 3);
        let ids = rows::column_values(all.column(0).as_ref()).unwrap();
        assert_eq!(
            ids,
            vec![
                adb_core::Value::Int(1),
                adb_core::Value::Int(2),
                adb_core::Value::Int(3)
            ]
        );
        mt.clear();
        assert!(mt.concat().unwrap().is_none());
    }
}
