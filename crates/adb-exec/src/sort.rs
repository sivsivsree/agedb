//! Sorting and top-N.
//!
//! `sort` + `limit` is the single most common analytical shape ("top 20
//! customers by revenue"), so the limit is pushed into Arrow's lexicographic
//! sort, which then only materializes the rows it needs.

use adb_core::{AdbError, Result};
use adb_planner::SortExpr;
use arrow::array::RecordBatch;
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};

/// Sort `batch` by `keys`, keeping at most `limit` rows.
pub fn sort_batch(
    batch: &RecordBatch,
    keys: &[SortExpr],
    limit: Option<usize>,
) -> Result<RecordBatch> {
    if keys.is_empty() || batch.num_rows() == 0 {
        return Ok(match limit {
            Some(n) if n < batch.num_rows() => batch.slice(0, n),
            _ => batch.clone(),
        });
    }
    let columns = keys
        .iter()
        .map(|key| {
            let index = batch
                .schema()
                .index_of(&key.column)
                .map_err(|_| AdbError::not_found("column", &key.column))?;
            Ok(SortColumn {
                values: batch.column(index).clone(),
                options: Some(SortOptions {
                    descending: !key.ascending,
                    nulls_first: key.nulls_first,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&columns, limit)?;
    arrow::compute::take_record_batch(batch, &indices).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{ColumnSchema, DataType, TableName, TableSchema};
    use adb_storage::rows;
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema::new(
            TableName::new("t").unwrap(),
            vec![
                ColumnSchema::new("country", DataType::Utf8),
                ColumnSchema::new("revenue", DataType::Float64),
            ],
        )
    }

    fn batch() -> RecordBatch {
        let rows: Vec<_> = json!([
            {"country": "uk", "revenue": 19300.0},
            {"country": "uae", "revenue": 12000.0},
            {"country": "usa", "revenue": 8400.0},
            {"country": "de", "revenue": null}
        ])
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
        rows::batch_from_json_rows(&schema(), &rows).unwrap()
    }

    fn countries(batch: &RecordBatch) -> Vec<String> {
        rows::column_values(batch.column(0).as_ref())
            .unwrap()
            .into_iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect()
    }

    #[test]
    fn descending_sort_puts_nulls_last() {
        let sorted = sort_batch(&batch(), &[SortExpr::desc("revenue")], None).unwrap();
        assert_eq!(countries(&sorted), vec!["uk", "uae", "usa", "de"]);
    }

    #[test]
    fn ascending_sort_puts_nulls_last_too() {
        let sorted = sort_batch(&batch(), &[SortExpr::asc("revenue")], None).unwrap();
        assert_eq!(countries(&sorted), vec!["usa", "uae", "uk", "de"]);
    }

    #[test]
    fn nulls_first_is_honoured_when_asked() {
        let key = SortExpr {
            column: "revenue".into(),
            ascending: true,
            nulls_first: true,
        };
        let sorted = sort_batch(&batch(), &[key], None).unwrap();
        assert_eq!(countries(&sorted), vec!["de", "usa", "uae", "uk"]);
    }

    #[test]
    fn top_n_returns_only_the_requested_rows() {
        let sorted = sort_batch(&batch(), &[SortExpr::desc("revenue")], Some(2)).unwrap();
        assert_eq!(countries(&sorted), vec!["uk", "uae"]);
    }

    #[test]
    fn multiple_keys_break_ties_in_order() {
        let rows: Vec<_> = json!([
            {"country": "b", "revenue": 1.0},
            {"country": "a", "revenue": 1.0},
            {"country": "c", "revenue": 2.0}
        ])
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
        let batch = rows::batch_from_json_rows(&schema(), &rows).unwrap();
        let sorted = sort_batch(
            &batch,
            &[SortExpr::asc("revenue"), SortExpr::asc("country")],
            None,
        )
        .unwrap();
        assert_eq!(countries(&sorted), vec!["a", "b", "c"]);
    }

    #[test]
    fn no_keys_just_truncates() {
        let out = sort_batch(&batch(), &[], Some(2)).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(countries(&out), vec!["uk", "uae"]);
    }

    #[test]
    fn unknown_sort_columns_are_reported() {
        assert_eq!(
            sort_batch(&batch(), &[SortExpr::asc("nope")], None)
                .unwrap_err()
                .code(),
            "not_found"
        );
    }
}
