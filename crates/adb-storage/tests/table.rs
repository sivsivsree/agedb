//! End-to-end storage tests: durability, row visibility, compaction.
//!
//! These exercise `TableStore` through its public API only, which is how the
//! engine uses it, so they double as the durability evidence for the WAL and
//! manifest ordering rules.

use std::sync::Arc;

use adb_core::{
    Aggregation, ColumnSchema, DataType, DatabaseName, SemanticType, TableName, TableSchema,
    TenantId, Value,
};
use adb_storage::object_store::LocalFsStore;
use adb_storage::table::{StorageConfig, TableStore};
use adb_storage::{rows, ObjectStore};
use arrow::array::RecordBatch;
use serde_json::json;
use tempfile::TempDir;

fn tenant() -> TenantId {
    TenantId::new("tenant_1").unwrap()
}

fn database() -> DatabaseName {
    DatabaseName::new("crm").unwrap()
}

fn customers(partitions: u32) -> Arc<TableSchema> {
    Arc::new(
        TableSchema::new(
            TableName::new("customers").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("revenue", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum),
            ],
        )
        .with_primary_key(vec!["id".to_string()])
        .with_partitions(partitions),
    )
}

fn events(partitions: u32) -> Arc<TableSchema> {
    Arc::new(
        TableSchema::new(
            TableName::new("events").unwrap(),
            vec![
                ColumnSchema::new("at", DataType::Timestamp).required(),
                ColumnSchema::new("kind", DataType::Utf8).semantic(SemanticType::Category),
            ],
        )
        .with_partitions(partitions),
    )
}

fn batch(schema: &TableSchema, value: serde_json::Value) -> RecordBatch {
    let rows: Vec<_> = value
        .as_array()
        .expect("array of rows")
        .iter()
        .map(|r| r.as_object().expect("row object").clone())
        .collect();
    rows::batch_from_json_rows(schema, &rows).unwrap()
}

fn open(dir: &TempDir, schema: Arc<TableSchema>, config: StorageConfig) -> TableStore {
    let store = Arc::new(LocalFsStore::new(dir.path()).unwrap());
    TableStore::open(store, &tenant(), &database(), schema, config).unwrap()
}

fn small_memtable() -> StorageConfig {
    StorageConfig {
        memtable_max_rows: 4,
        auto_compact: false,
        ..Default::default()
    }
}

/// Collect every visible row as JSON, partition by partition.
fn visible_rows(table: &TableStore) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let schema = table.schema();
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut out = Vec::new();
    for snapshot in table.snapshots().unwrap() {
        for (offset, batch) in snapshot.memtable_with_offsets() {
            let projected =
                adb_storage::segment::project_with_backfill(batch, &schema, &columns).unwrap();
            for row in 0..projected.num_rows() {
                if snapshot.memtable_suppressed.contains(offset + row as u32) {
                    continue;
                }
                out.extend(rows::batch_to_json_rows(&projected.slice(row, 1)).unwrap());
            }
        }
        for entry in &snapshot.segments {
            let batch = adb_storage::segment::read_segment(
                snapshot.store.as_ref(),
                &entry.meta,
                &schema,
                Some(&columns),
            )
            .unwrap();
            for row in 0..batch.num_rows() {
                if entry.suppressed.contains(row as u32) {
                    continue;
                }
                out.extend(rows::batch_to_json_rows(&batch.slice(row, 1)).unwrap());
            }
        }
    }
    out.sort_by_key(|r| r.values().next().map(|v| v.to_string()).unwrap_or_default());
    out
}

fn ids(table: &TableStore) -> Vec<i64> {
    let mut ids: Vec<i64> = visible_rows(table)
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn inserted_rows_are_visible_before_any_flush() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    let outcome = table
        .insert(batch(
            &schema,
            json!([
                {"id": 1, "country": "uae", "revenue": 12000.0},
                {"id": 2, "country": "usa", "revenue": 8400.0}
            ]),
        ))
        .unwrap();
    assert_eq!(outcome.rows, 2);
    assert_eq!(outcome.segments_flushed, 0);
    assert_eq!(table.row_count().unwrap(), 2);
    assert_eq!(ids(&table), vec![1, 2]);
    // Nothing has been written to a segment yet.
    assert_eq!(table.stored_bytes().unwrap(), 0);
}

#[test]
fn unflushed_writes_survive_a_restart_by_wal_replay() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    {
        let table = open(&dir, schema.clone(), StorageConfig::default());
        table
            .insert(batch(
                &schema,
                json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
            ))
            .unwrap();
        table
            .insert(batch(
                &schema,
                json!([{"id": 2, "country": "usa", "revenue": 2.0}]),
            ))
            .unwrap();
        // Deliberately no flush: this is the crash case.
    }
    let table = open(&dir, schema.clone(), StorageConfig::default());
    assert_eq!(table.row_count().unwrap(), 2);
    assert_eq!(ids(&table), vec![1, 2]);
    // And the recovered rows are still writable afterwards.
    table
        .insert(batch(
            &schema,
            json!([{"id": 3, "country": "uk", "revenue": 3.0}]),
        ))
        .unwrap();
    assert_eq!(ids(&table), vec![1, 2, 3]);
}

#[test]
fn flush_writes_a_segment_and_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    {
        let table = open(&dir, schema.clone(), StorageConfig::default());
        table
            .insert(batch(
                &schema,
                json!([{"id": 1, "country": "uae", "revenue": 1.0}, {"id": 2, "country": "usa", "revenue": 2.0}]),
            ))
            .unwrap();
        assert_eq!(table.flush().unwrap(), 1);
        assert!(table.stored_bytes().unwrap() > 0);
    }
    let table = open(&dir, schema.clone(), StorageConfig::default());
    assert_eq!(table.row_count().unwrap(), 2);
    assert!(
        table.stored_bytes().unwrap() > 0,
        "segment should be picked up from the manifest"
    );
    let rows = visible_rows(&table);
    assert_eq!(rows[0]["country"], json!("uae"));
}

#[test]
fn the_memtable_flushes_itself_when_it_fills_up() {
    let dir = TempDir::new().unwrap();
    let schema = events(1);
    let table = open(&dir, schema.clone(), small_memtable());
    for i in 0..8 {
        table
            .insert(batch(
                &schema,
                json!([{"at": 1_767_225_600 + i, "kind": "click"}]),
            ))
            .unwrap();
    }
    assert!(
        table.stored_bytes().unwrap() > 0,
        "a flush should have happened automatically"
    );
    assert_eq!(table.row_count().unwrap(), 8);
}

#[test]
fn upsert_replaces_a_row_without_growing_the_table() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 100.0}]),
        ))
        .unwrap();
    table
        .upsert(batch(
            &schema,
            json!([{"id": 1, "country": "uk", "revenue": 250.0}]),
        ))
        .unwrap();
    assert_eq!(table.row_count().unwrap(), 1);
    let rows = visible_rows(&table);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["country"], json!("uk"));
    assert_eq!(rows[0]["revenue"], json!(250.0));
}

#[test]
fn upsert_replaces_rows_that_already_live_in_a_segment() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 100.0}]),
        ))
        .unwrap();
    table.flush().unwrap();
    table
        .upsert(batch(
            &schema,
            json!([{"id": 1, "country": "uk", "revenue": 250.0}]),
        ))
        .unwrap();

    assert_eq!(table.row_count().unwrap(), 1);
    assert_eq!(visible_rows(&table)[0]["country"], json!("uk"));

    // The suppression must be durable, not just in memory.
    table.flush().unwrap();
    drop(table);
    let table = open(&dir, schema, StorageConfig::default());
    assert_eq!(table.row_count().unwrap(), 1);
    assert_eq!(visible_rows(&table)[0]["country"], json!("uk"));
}

#[test]
fn delete_hides_rows_durably() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([
                {"id": 1, "country": "uae", "revenue": 1.0},
                {"id": 2, "country": "usa", "revenue": 2.0},
                {"id": 3, "country": "uk", "revenue": 3.0}
            ]),
        ))
        .unwrap();
    table.flush().unwrap();

    assert_eq!(table.delete(&[vec![Value::Int(2)]]).unwrap(), 1);
    // Deleting a key that is not there is not an error, and counts as zero.
    assert_eq!(table.delete(&[vec![Value::Int(99)]]).unwrap(), 0);
    assert_eq!(table.row_count().unwrap(), 2);
    assert_eq!(ids(&table), vec![1, 3]);

    drop(table);
    let table = open(&dir, schema, StorageConfig::default());
    assert_eq!(ids(&table), vec![1, 3], "delete must survive WAL replay");
}

#[test]
fn duplicate_primary_keys_are_rejected_rather_than_silently_merged() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
        ))
        .unwrap();

    let err = table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "usa", "revenue": 9.0}]),
        ))
        .unwrap_err();
    assert_eq!(err.code(), "already_exists");

    let err = table
        .insert(batch(
            &schema,
            json!([{"id": 5, "country": "a", "revenue": 1.0}, {"id": 5, "country": "b", "revenue": 2.0}]),
        ))
        .unwrap_err();
    assert_eq!(err.code(), "bad_request");

    // The rejected batch left nothing behind.
    assert_eq!(ids(&table), vec![1]);
}

#[test]
fn upsert_and_delete_need_a_primary_key() {
    let dir = TempDir::new().unwrap();
    let schema = events(1);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    let rows = batch(&schema, json!([{"at": 1_767_225_600, "kind": "click"}]));
    assert_eq!(table.upsert(rows).unwrap_err().code(), "bad_request");
    assert_eq!(
        table.delete(&[vec![Value::Int(1)]]).unwrap_err().code(),
        "bad_request"
    );
    assert_eq!(
        table.get(&[vec![Value::Int(1)]]).unwrap_err().code(),
        "bad_request"
    );
}

#[test]
fn append_only_writes_spread_across_partitions() {
    let dir = TempDir::new().unwrap();
    let schema = events(4);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    let rows: Vec<_> = (0..100)
        .map(|i| json!({"at": 1_767_225_600 + i, "kind": "click"}))
        .collect();
    table.insert(batch(&schema, json!(rows))).unwrap();
    let snapshots = table.snapshots().unwrap();
    assert_eq!(snapshots.len(), 4);
    for snapshot in &snapshots {
        assert!(
            snapshot.visible_rows() > 0,
            "partition {} got no rows",
            snapshot.partition
        );
    }
    assert_eq!(table.row_count().unwrap(), 100);
}

#[test]
fn keyed_writes_route_by_key_and_point_lookups_return_request_order() {
    let dir = TempDir::new().unwrap();
    let schema = customers(4);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    let rows: Vec<_> = (1..=20)
        .map(|i| json!({"id": i, "country": "uae", "revenue": i as f64}))
        .collect();
    table.insert(batch(&schema, json!(rows))).unwrap();
    table.flush().unwrap();

    let wanted = vec![
        vec![Value::Int(7)],
        vec![Value::Int(3)],
        vec![Value::Int(999)],
    ];
    let found = table.get(&wanted).unwrap();
    assert_eq!(found.num_rows(), 2, "the missing key is simply absent");
    let json_rows = rows::batch_to_json_rows(&found).unwrap();
    assert_eq!(json_rows[0]["id"], json!(7));
    assert_eq!(json_rows[1]["id"], json!(3));
}

#[test]
fn point_lookups_see_the_memtable_and_the_latest_version() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
        ))
        .unwrap();
    table.flush().unwrap();
    table
        .upsert(batch(
            &schema,
            json!([{"id": 1, "country": "uk", "revenue": 2.0}]),
        ))
        .unwrap();
    let found = rows::batch_to_json_rows(&table.get(&[vec![Value::Int(1)]]).unwrap()).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["country"], json!("uk"));
}

#[test]
fn compaction_merges_segments_and_preserves_query_results() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    let config = StorageConfig {
        auto_compact: false,
        compaction: adb_storage::compaction::CompactionPolicy {
            min_segments: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let table = open(&dir, schema.clone(), config);
    for i in 1..=6 {
        table
            .insert(batch(
                &schema,
                json!([{"id": i, "country": "uae", "revenue": i as f64}]),
            ))
            .unwrap();
        table.flush().unwrap();
    }
    let before = visible_rows(&table);
    let segments_before: usize = table
        .snapshots()
        .unwrap()
        .iter()
        .map(|s| s.segments.len())
        .sum();
    assert_eq!(segments_before, 6);

    assert_eq!(table.compact().unwrap(), 1);
    let segments_after: usize = table
        .snapshots()
        .unwrap()
        .iter()
        .map(|s| s.segments.len())
        .sum();
    assert!(
        segments_after < segments_before,
        "{segments_after} should be fewer than {segments_before}"
    );
    assert_eq!(
        visible_rows(&table),
        before,
        "compaction must not change what is visible"
    );

    drop(table);
    let table = open(&dir, schema, StorageConfig::default());
    assert_eq!(visible_rows(&table), before);
}

#[test]
fn compaction_reclaims_suppressed_rows_and_keeps_the_key_index_correct() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    let table = open(
        &dir,
        schema.clone(),
        StorageConfig {
            auto_compact: false,
            ..Default::default()
        },
    );
    let rows: Vec<_> = (1..=10)
        .map(|i| json!({"id": i, "country": "uae", "revenue": i as f64}))
        .collect();
    table.insert(batch(&schema, json!(rows))).unwrap();
    table.flush().unwrap();

    // Suppress half the segment, then flush so the tombstones are checkpointed.
    let keys: Vec<Vec<Value>> = (1..=5).map(|i| vec![Value::Int(i)]).collect();
    assert_eq!(table.delete(&keys).unwrap(), 5);
    table.flush().unwrap();
    let bytes_before = table.stored_bytes().unwrap();

    assert_eq!(table.compact().unwrap(), 1);
    assert_eq!(table.row_count().unwrap(), 5);
    assert_eq!(ids(&table), vec![6, 7, 8, 9, 10]);
    assert!(table.stored_bytes().unwrap() < bytes_before);

    // The key index must still resolve relocated rows, and deleted keys must
    // stay deleted.
    let found = rows::batch_to_json_rows(&table.get(&[vec![Value::Int(7)]]).unwrap()).unwrap();
    assert_eq!(found[0]["revenue"], json!(7.0));
    assert_eq!(table.get(&[vec![Value::Int(3)]]).unwrap().num_rows(), 0);

    // And an upsert after compaction still replaces rather than duplicates.
    table
        .upsert(batch(
            &schema,
            json!([{"id": 7, "country": "uk", "revenue": 70.0}]),
        ))
        .unwrap();
    assert_eq!(table.row_count().unwrap(), 5);
    assert_eq!(
        rows::batch_to_json_rows(&table.get(&[vec![Value::Int(7)]]).unwrap()).unwrap()[0]
            ["country"],
        json!("uk")
    );
}

#[test]
fn an_orphan_segment_from_an_interrupted_flush_is_cleaned_up_on_open() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    {
        let table = open(&dir, schema.clone(), StorageConfig::default());
        table
            .insert(batch(
                &schema,
                json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
            ))
            .unwrap();
        table.flush().unwrap();
    }
    // Simulate a crash after the Parquet file was written but before the
    // manifest named it.
    let store = LocalFsStore::new(dir.path()).unwrap();
    let orphan = "tenants/tenant_1/databases/crm/tables/customers/p0/segments/seg-00000099.parquet";
    store.put(orphan, b"not really parquet").unwrap();
    assert!(store.exists(orphan).unwrap());

    let table = open(&dir, schema, StorageConfig::default());
    assert!(
        !store.exists(orphan).unwrap(),
        "unreferenced segment should be deleted"
    );
    assert_eq!(table.row_count().unwrap(), 1);
}

#[test]
fn schema_evolution_backfills_older_segments() {
    let dir = TempDir::new().unwrap();
    let schema = customers(1);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
        ))
        .unwrap();
    table.flush().unwrap();

    let mut evolved = (*schema).clone();
    evolved
        .columns
        .push(ColumnSchema::new("segment", DataType::Utf8).described("sales segment"));
    evolved.version = 2;
    let evolved = Arc::new(evolved);
    table.set_schema(evolved.clone()).unwrap();

    table
        .insert(batch(
            &evolved,
            json!([{"id": 2, "country": "usa", "revenue": 2.0, "segment": "enterprise"}]),
        ))
        .unwrap();

    let rows = visible_rows(&table);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["segment"],
        json!(null),
        "the old row backfills as null"
    );
    assert_eq!(rows[1]["segment"], json!("enterprise"));

    drop(table);
    let table = open(&dir, evolved, StorageConfig::default());
    assert_eq!(visible_rows(&table).len(), 2);
}

#[test]
fn destroy_removes_every_file_for_the_table() {
    let dir = TempDir::new().unwrap();
    let schema = customers(2);
    let table = open(&dir, schema.clone(), StorageConfig::default());
    table
        .insert(batch(
            &schema,
            json!([{"id": 1, "country": "uae", "revenue": 1.0}]),
        ))
        .unwrap();
    table.flush().unwrap();
    let store = LocalFsStore::new(dir.path()).unwrap();
    assert!(!store
        .list("tenants/tenant_1/databases/crm/tables/customers")
        .unwrap()
        .is_empty());
    table.destroy().unwrap();
    assert!(store
        .list("tenants/tenant_1/databases/crm/tables/customers")
        .unwrap()
        .is_empty());
}
