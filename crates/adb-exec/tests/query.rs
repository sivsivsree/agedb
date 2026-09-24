//! End-to-end execution tests over real storage.
//!
//! The core of this file is a *correctness oracle*: the same questions are
//! answered by the vectorized engine and by a naive row-at-a-time
//! implementation over the same generated rows, and the answers must match. A
//! columnar engine has many places to be subtly wrong (null handling, group
//! merging across partitions, suppressed rows, pruning) and an oracle catches
//! all of them at once.

use std::sync::Arc;

use adb_core::{
    Aggregation, ColumnSchema, DataType, DatabaseName, QueryLimits, SemanticType, TableName,
    TableSchema, TenantId, Value,
};
use adb_exec::{execute, execute_with_budget, Budget, QueryResult};
use adb_planner::{physical, validate, AggregateExpr, AggregateFunc, Expr, Query, SortExpr};
use adb_storage::object_store::LocalFsStore;
use adb_storage::rows;
use adb_storage::table::{StorageConfig, TableStore};
use serde_json::{json, Map, Value as Json};
use tempfile::TempDir;

const PARTITIONS: u32 = 4;
const ROWS: usize = 4_000;
const COUNTRIES: [&str; 5] = ["uae", "usa", "uk", "de", "sg"];

fn schema() -> Arc<TableSchema> {
    Arc::new(
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
                ColumnSchema::new("qty", DataType::Int64),
                ColumnSchema::new("created_at", DataType::Timestamp)
                    .semantic(SemanticType::Timestamp),
                ColumnSchema::new("note", DataType::Utf8),
            ],
        )
        .with_partitions(PARTITIONS),
    )
}

/// Deterministic pseudo-random generator: the dataset must be identical on every
/// run so a failure is reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const DAY_MICROS: i64 = 86_400_000_000;
const START: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z

/// The rows every test in this file works from.
fn dataset() -> Vec<Map<String, Json>> {
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    (0..ROWS)
        .map(|i| {
            let country = COUNTRIES[rng.below(COUNTRIES.len() as u64) as usize];
            // Every eleventh row has a null amount and note, so null handling is
            // exercised everywhere.
            let nulls = i % 11 == 0;
            let amount = if nulls {
                Json::Null
            } else {
                json!((rng.below(100_000) as f64) / 100.0)
            };
            let mut row = Map::new();
            row.insert("id".into(), json!(i as i64));
            row.insert("country".into(), json!(country));
            row.insert("amount".into(), amount);
            row.insert("qty".into(), json!(rng.below(10) as i64 + 1));
            row.insert(
                "created_at".into(),
                json!(START + (i as i64 % 365) * DAY_MICROS),
            );
            row.insert(
                "note".into(),
                if nulls {
                    Json::Null
                } else {
                    json!(format!("order {i}"))
                },
            );
            row
        })
        .collect()
}

struct Fixture {
    _dir: TempDir,
    table: TableStore,
    rows: Vec<Map<String, Json>>,
}

impl Fixture {
    /// Build a table whose rows are split between several segments and the
    /// memtable, so every read path is covered.
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFsStore::new(dir.path()).unwrap());
        let schema = schema();
        let table = TableStore::open(
            store,
            &TenantId::new("t1").unwrap(),
            &DatabaseName::new("crm").unwrap(),
            schema.clone(),
            StorageConfig {
                auto_compact: false,
                ..Default::default()
            },
        )
        .unwrap();

        let rows_data = dataset();
        // Three flushed segments per partition plus an unflushed tail.
        for chunk in rows_data.chunks(1_000) {
            table
                .insert(rows::batch_from_json_rows(&schema, chunk).unwrap())
                .unwrap();
            if chunk.len() == 1_000 {
                table.flush().unwrap();
            }
        }
        Self {
            _dir: dir,
            table,
            rows: rows_data,
        }
    }

    fn run(&self, query: Query) -> QueryResult {
        self.run_with(query, QueryLimits::unlimited())
    }

    fn run_with(&self, query: Query, limits: QueryLimits) -> QueryResult {
        let schema = self.table.schema();
        let validated = validate(&query, &schema, &limits).expect("plan should validate");
        let plan = physical::build(&validated).expect("plan should lower");
        execute(&plan, &self.table.snapshots().unwrap(), limits).expect("query should run")
    }

    fn json(&self, query: Query) -> Vec<Map<String, Json>> {
        self.run(query).to_json_rows().unwrap()
    }
}

fn scan() -> Query {
    Query::scan(TableName::new("orders").unwrap())
}

fn f64_of(v: &Json) -> Option<f64> {
    v.as_f64()
}

// --- The oracle -----------------------------------------------------------

/// Naive reference: filter rows with a closure, then aggregate in plain Rust.
fn reference_sum_by_country(
    rows: &[Map<String, Json>],
    keep: impl Fn(&Map<String, Json>) -> bool,
) -> Vec<(String, f64, i64)> {
    let mut groups: std::collections::BTreeMap<String, (f64, i64)> = Default::default();
    for row in rows.iter().filter(|r| keep(r)) {
        let country = row["country"].as_str().unwrap().to_string();
        let entry = groups.entry(country).or_insert((0.0, 0));
        if let Some(amount) = f64_of(&row["amount"]) {
            entry.0 += amount;
        }
        entry.1 += 1;
    }
    groups
        .into_iter()
        .map(|(k, (sum, count))| (k, sum, count))
        .collect()
}

fn as_groups(result: &[Map<String, Json>]) -> Vec<(String, f64, i64)> {
    let mut out: Vec<(String, f64, i64)> = result
        .iter()
        .map(|row| {
            (
                row["country"].as_str().unwrap().to_string(),
                row["revenue"].as_f64().unwrap_or(0.0),
                row["orders"].as_i64().unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn assert_close(left: &[(String, f64, i64)], right: &[(String, f64, i64)]) {
    assert_eq!(
        left.len(),
        right.len(),
        "group counts differ:\n{left:?}\n{right:?}"
    );
    for (a, b) in left.iter().zip(right) {
        assert_eq!(a.0, b.0);
        assert!(
            (a.1 - b.1).abs() < 0.01,
            "revenue for {} differs: {} vs {}",
            a.0,
            a.1,
            b.1
        );
        assert_eq!(a.2, b.2, "order count for {} differs", a.0);
    }
}

#[test]
fn group_by_matches_the_reference_implementation() {
    let fixture = Fixture::new();
    let query = scan().aggregate(
        vec!["country".to_string()],
        vec![
            AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
            AggregateExpr::count_star("orders"),
        ],
    );
    let engine = as_groups(&fixture.json(query));
    let reference = reference_sum_by_country(&fixture.rows, |_| true);
    assert_close(&engine, &reference);
    assert_eq!(reference.iter().map(|g| g.2).sum::<i64>(), ROWS as i64);
}

#[test]
fn filtered_group_by_matches_the_reference_implementation() {
    let fixture = Fixture::new();
    let cutoff = START + 100 * DAY_MICROS;
    let query = scan()
        .filter(
            Expr::col("created_at")
                .gt(Expr::lit(Value::Timestamp(cutoff)))
                .and(Expr::col("qty").gt(Expr::lit(Value::Int(3)))),
        )
        .aggregate(
            vec!["country".to_string()],
            vec![
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
                AggregateExpr::count_star("orders"),
            ],
        );
    let engine = as_groups(&fixture.json(query));
    let reference = reference_sum_by_country(&fixture.rows, |row| {
        row["created_at"].as_i64().unwrap() > cutoff && row["qty"].as_i64().unwrap() > 3
    });
    assert_close(&engine, &reference);
    assert!(
        !reference.is_empty(),
        "the fixture should produce matching rows"
    );
}

#[test]
fn null_aware_filters_match_the_reference_implementation() {
    let fixture = Fixture::new();
    let query = scan()
        .filter(Expr::IsNull(Box::new(Expr::col("amount"))))
        .aggregate(vec![], vec![AggregateExpr::count_star("n")]);
    let engine = fixture.json(query)[0]["n"].as_i64().unwrap();
    let reference = fixture
        .rows
        .iter()
        .filter(|r| r["amount"].is_null())
        .count() as i64;
    assert_eq!(engine, reference);
    assert!(reference > 0);

    let query = scan()
        .filter(Expr::Not(Box::new(
            Expr::col("country").eq(Expr::lit(Value::Str("uae".into()))),
        )))
        .aggregate(vec![], vec![AggregateExpr::count_star("n")]);
    let engine = fixture.json(query)[0]["n"].as_i64().unwrap();
    let reference = fixture
        .rows
        .iter()
        .filter(|r| r["country"].as_str() != Some("uae"))
        .count() as i64;
    assert_eq!(engine, reference);
}

#[test]
fn scalar_aggregates_match_the_reference_implementation() {
    let fixture = Fixture::new();
    let query = scan().aggregate(
        vec![],
        vec![
            AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "total"),
            AggregateExpr::new(AggregateFunc::Avg, Some("amount".into()), "mean"),
            AggregateExpr::new(AggregateFunc::Min, Some("amount".into()), "low"),
            AggregateExpr::new(AggregateFunc::Max, Some("amount".into()), "high"),
            AggregateExpr::new(AggregateFunc::Count, Some("amount".into()), "with_amount"),
            AggregateExpr::count_star("rows"),
        ],
    );
    let out = &fixture.json(query)[0];

    let amounts: Vec<f64> = fixture
        .rows
        .iter()
        .filter_map(|r| f64_of(&r["amount"]))
        .collect();
    let total: f64 = amounts.iter().sum();
    let mean = total / amounts.len() as f64;
    let low = amounts.iter().cloned().fold(f64::INFINITY, f64::min);
    let high = amounts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    assert!((out["total"].as_f64().unwrap() - total).abs() < 0.01);
    assert!((out["mean"].as_f64().unwrap() - mean).abs() < 0.0001);
    assert!((out["low"].as_f64().unwrap() - low).abs() < 1e-9);
    assert!((out["high"].as_f64().unwrap() - high).abs() < 1e-9);
    assert_eq!(out["with_amount"].as_i64().unwrap(), amounts.len() as i64);
    assert_eq!(out["rows"].as_i64().unwrap(), ROWS as i64);
}

#[test]
fn top_n_matches_the_reference_implementation() {
    let fixture = Fixture::new();
    let query = scan()
        .aggregate(
            vec!["country".to_string()],
            vec![
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
                AggregateExpr::count_star("orders"),
            ],
        )
        .sort(vec![SortExpr::desc("revenue")])
        .limit(3);
    let engine = fixture.json(query);
    assert_eq!(engine.len(), 3);

    let mut reference = reference_sum_by_country(&fixture.rows, |_| true);
    reference.sort_by(|a, b| b.1.total_cmp(&a.1));
    for (row, expected) in engine.iter().zip(&reference[..3]) {
        assert_eq!(row["country"].as_str().unwrap(), expected.0);
    }
}

#[test]
fn row_queries_return_projected_renamed_columns_in_order() {
    let fixture = Fixture::new();
    let query = scan()
        .filter(Expr::col("id").eq(Expr::lit(Value::Int(7))))
        .project(vec![
            (Expr::col("amount"), "revenue".to_string()),
            (Expr::col("country"), "market".to_string()),
        ]);
    let result = fixture.run(query);
    let batch_schema = result.batch.schema();
    let names: Vec<&str> = batch_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["revenue", "market"]);
    let out = result.to_json_rows().unwrap();
    assert_eq!(out.len(), 1);
    let expected = &fixture.rows[7];
    assert_eq!(out[0]["market"], expected["country"]);
}

#[test]
fn a_selective_time_filter_prunes_most_segments() {
    let fixture = Fixture::new();

    // Everything: nothing can be skipped.
    let all = fixture.run(scan().filter(Expr::col("qty").gt(Expr::lit(Value::Int(0)))));
    assert_eq!(all.stats.segments_pruned, 0);
    assert!(all.stats.segments_read > 0);

    // A predicate outside every segment's id range: nothing should be read.
    let none = fixture.run(
        scan()
            .filter(Expr::col("id").gt(Expr::lit(Value::Int(1_000_000))))
            .aggregate(vec![], vec![AggregateExpr::count_star("n")]),
    );
    assert_eq!(
        none.stats.segments_read, 0,
        "a provably empty range must read nothing"
    );
    assert!(none.stats.segments_pruned > 0);
    assert_eq!(none.batch.num_rows(), 1);
    assert_eq!(none.to_json_rows().unwrap()[0]["n"], json!(0));

    // A selective id range should skip strictly more segments than the
    // unselective one while still returning the right rows.
    let selective = fixture.run(
        scan()
            .filter(Expr::col("id").lt(Expr::lit(Value::Int(50))))
            .aggregate(vec![], vec![AggregateExpr::count_star("n")]),
    );
    assert!(
        selective.stats.segments_pruned > 0,
        "expected pruning, stats: {:?}",
        selective.stats
    );
    assert_eq!(selective.to_json_rows().unwrap()[0]["n"], json!(50));
}

#[test]
fn equality_on_an_indexed_column_uses_bloom_filters() {
    let fixture = Fixture::new();
    // `id` is a semantic id, so segments carry a bloom filter for it.
    let hit = fixture.run(scan().filter(Expr::col("id").eq(Expr::lit(Value::Int(2_500)))));
    assert_eq!(hit.batch.num_rows(), 1);
    let miss = fixture.run(scan().filter(Expr::col("id").eq(Expr::lit(Value::Int(999_999)))));
    assert_eq!(miss.batch.num_rows(), 0);
    assert_eq!(miss.stats.segments_read, 0);
}

#[test]
fn count_star_is_answered_from_metadata_without_reading_anything() {
    let fixture = Fixture::new();
    let result = fixture.run(scan().aggregate(vec![], vec![AggregateExpr::count_star("n")]));
    assert_eq!(result.to_json_rows().unwrap()[0]["n"], json!(ROWS));
    assert_eq!(
        result.stats.bytes_scanned, 0,
        "count(*) should not read data"
    );
    assert_eq!(result.stats.segments_read, 0);
}

/// A small table *with* a primary key, for mutation tests. The main fixture is
/// append-only because that is the shape bulk analytical ingest uses (and it
/// keeps ids contiguous per segment, which is what makes pruning observable).
fn keyed_fixture() -> (TempDir, TableStore, Arc<TableSchema>) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFsStore::new(dir.path()).unwrap());
    let mut keyed = (*schema()).clone();
    keyed.primary_key = vec!["id".to_string()];
    keyed.partitions = 2;
    let keyed = Arc::new(keyed);
    let table = TableStore::open(
        store,
        &TenantId::new("t1").unwrap(),
        &DatabaseName::new("crm").unwrap(),
        keyed.clone(),
        StorageConfig {
            auto_compact: false,
            ..Default::default()
        },
    )
    .unwrap();
    let rows_data: Vec<Map<String, Json>> = dataset().into_iter().take(200).collect();
    table
        .insert(rows::batch_from_json_rows(&keyed, &rows_data).unwrap())
        .unwrap();
    table.flush().unwrap();
    (dir, table, keyed)
}

#[test]
fn updates_and_deletes_are_reflected_in_query_results() {
    let (_dir, table, schema) = keyed_fixture();
    let run = |query: Query| {
        let validated = validate(&query, &schema, &QueryLimits::unlimited()).unwrap();
        let plan = physical::build(&validated).unwrap();
        execute(&plan, &table.snapshots().unwrap(), QueryLimits::unlimited()).unwrap()
    };

    // Update row 7 by upsert, delete row 8.
    let updated = json!([{
        "id": 7, "country": "sg", "amount": 999.0, "qty": 1,
        "created_at": START, "note": "updated"
    }]);
    let rows_vec: Vec<_> = updated
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
    table
        .upsert(rows::batch_from_json_rows(&schema, &rows_vec).unwrap())
        .unwrap();
    table.delete(&[vec![Value::Int(8)]]).unwrap();

    let count = run(scan().aggregate(vec![], vec![AggregateExpr::count_star("n")]));
    assert_eq!(count.to_json_rows().unwrap()[0]["n"], json!(199));

    let row = run(scan().filter(Expr::col("id").eq(Expr::lit(Value::Int(7)))))
        .to_json_rows()
        .unwrap();
    assert_eq!(
        row.len(),
        1,
        "exactly one version of an updated row is visible"
    );
    assert_eq!(row[0]["amount"], json!(999.0));
    assert_eq!(row[0]["country"], json!("sg"));

    let deleted = run(scan().filter(Expr::col("id").eq(Expr::lit(Value::Int(8)))))
        .to_json_rows()
        .unwrap();
    assert!(deleted.is_empty());
}

#[test]
fn compaction_does_not_change_answers() {
    let fixture = Fixture::new();
    let query = || {
        scan().aggregate(
            vec!["country".to_string()],
            vec![
                AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
                AggregateExpr::count_star("orders"),
            ],
        )
    };
    let before = as_groups(&fixture.json(query()));
    fixture.table.flush().unwrap();
    fixture.table.compact().unwrap();
    let after = as_groups(&fixture.json(query()));
    assert_close(&before, &after);
}

#[test]
fn limits_are_enforced_inside_execution() {
    let fixture = Fixture::new();

    // Byte budget: a full scan must be refused rather than run.
    let limits = QueryLimits {
        max_bytes_scanned: 1,
        ..QueryLimits::unlimited()
    };
    let schema = fixture.table.schema();
    let validated = validate(&scan(), &schema, &limits).unwrap();
    let plan = physical::build(&validated).unwrap();
    let err = execute(&plan, &fixture.table.snapshots().unwrap(), limits).unwrap_err();
    assert_eq!(err.code(), "limit_exceeded");

    // Row budget: the result is cut short *and says so*.
    let limits = QueryLimits {
        max_rows: 10,
        ..QueryLimits::unlimited()
    };
    let result = fixture.run_with(scan(), limits);
    assert_eq!(result.rows(), 10);
    assert!(result.stats.truncated);
    assert!(
        result.warnings.iter().any(|w| w.contains("truncated")),
        "{:?}",
        result.warnings
    );

    // An explicit limit is not a truncation.
    let result = fixture.run_with(
        scan().limit(5),
        QueryLimits {
            max_rows: 10,
            ..QueryLimits::unlimited()
        },
    );
    assert_eq!(result.rows(), 5);
    assert!(!result.stats.truncated);
    assert!(result.warnings.is_empty());
}

#[test]
fn the_deadline_is_checked_after_the_scan_not_only_inside_it() {
    // An empty table gives the scan nothing to read, so its per-batch deadline
    // check never runs. Merge, finish and sort must still refuse to start once
    // the budget is spent, or the time limit is only a scan limit.
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFsStore::new(dir.path()).unwrap());
    let schema = schema();
    let table = TableStore::open(
        store,
        &TenantId::new("t1").unwrap(),
        &DatabaseName::new("crm").unwrap(),
        schema.clone(),
        StorageConfig::default(),
    )
    .unwrap();

    let limits = QueryLimits {
        max_execution_time_ms: 1_000,
        ..QueryLimits::unlimited()
    };
    let query = scan()
        .aggregate(
            vec!["country".to_string()],
            vec![AggregateExpr::new(
                AggregateFunc::Sum,
                Some("amount".into()),
                "revenue",
            )],
        )
        .sort(vec![SortExpr::desc("revenue")]);
    let validated = validate(&query, &schema, &limits).unwrap();
    let plan = physical::build(&validated).unwrap();
    let snapshots = table.snapshots().unwrap();

    // Well inside the budget, the empty answer comes back.
    let fresh = execute_with_budget(&plan, &snapshots, Budget::new(limits)).unwrap();
    assert_eq!(fresh.rows(), 0);

    // Started a minute ago, with a one second budget: no stage may run.
    let started = std::time::Instant::now() - std::time::Duration::from_secs(60);
    let err =
        execute_with_budget(&plan, &snapshots, Budget::started_at(limits, started)).unwrap_err();
    assert_eq!(err.code(), "limit_exceeded");
    assert!(err.to_string().contains("max_execution_time"), "{err}");
}

#[test]
fn offset_and_limit_page_through_sorted_results() {
    let fixture = Fixture::new();
    let page = |offset: usize| {
        fixture.json(
            scan()
                .project(vec![(Expr::col("id"), "id".to_string())])
                .sort(vec![SortExpr::asc("id")])
                .limit_offset(3, offset),
        )
    };
    let first = page(0);
    let second = page(3);
    assert_eq!(first.len(), 3);
    assert_eq!(second.len(), 3);
    assert_eq!(first[0]["id"], json!(0));
    assert_eq!(second[0]["id"], json!(3));
}

#[test]
fn parallel_partition_scans_produce_the_same_rows_as_a_single_partition() {
    // Same data, one partition: results must be identical, which is the check
    // that partition-parallel scanning and merging is not losing or duplicating
    // rows.
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFsStore::new(dir.path()).unwrap());
    let mut single = (*schema()).clone();
    single.partitions = 1;
    let single = Arc::new(single);
    let table = TableStore::open(
        store,
        &TenantId::new("t1").unwrap(),
        &DatabaseName::new("crm").unwrap(),
        single.clone(),
        StorageConfig {
            auto_compact: false,
            ..Default::default()
        },
    )
    .unwrap();
    let rows_data = dataset();
    table
        .insert(rows::batch_from_json_rows(&single, &rows_data).unwrap())
        .unwrap();
    table.flush().unwrap();

    let query = scan().aggregate(
        vec!["country".to_string()],
        vec![
            AggregateExpr::new(AggregateFunc::Sum, Some("amount".into()), "revenue"),
            AggregateExpr::count_star("orders"),
        ],
    );
    let validated = validate(&query, &single, &QueryLimits::unlimited()).unwrap();
    let plan = physical::build(&validated).unwrap();
    let one = execute(&plan, &table.snapshots().unwrap(), QueryLimits::unlimited()).unwrap();

    let many = Fixture::new();
    assert_close(
        &as_groups(&one.to_json_rows().unwrap()),
        &as_groups(&many.json(query)),
    );
}
