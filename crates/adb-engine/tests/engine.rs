//! Engine-level tests: the guarantees an agent actually depends on.

use adb_core::{
    Aggregation, ColumnSchema, DataType, DatabaseName, QueryLimits, RequestContext, Scope,
    SemanticType, TableName, TableSchema, TenantId, UserId,
};
use adb_engine::{Engine, EngineConfig, QuerySource};
use adb_planner::AggregateFunc;
use adb_query::{FilterOp, FilterSpec, MetricSpec, OrderSpec, PlanRequest};
use serde_json::{json, Map, Value as Json};
use tempfile::TempDir;

fn leads() -> TableSchema {
    TableSchema::new(
        TableName::new("leads").unwrap(),
        vec![
            ColumnSchema::new("id", DataType::Int64)
                .required()
                .semantic(SemanticType::Id),
            ColumnSchema::new("company", DataType::Utf8).semantic(SemanticType::Category),
            ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
            ColumnSchema::new("score", DataType::Float64)
                .semantic(SemanticType::Score)
                .aggregated_by(Aggregation::Avg),
            ColumnSchema::new("value", DataType::Float64)
                .semantic(SemanticType::Currency)
                .aggregated_by(Aggregation::Sum),
            ColumnSchema::new("created_at", DataType::Timestamp).semantic(SemanticType::Timestamp),
            ColumnSchema::new("email", DataType::Utf8)
                .semantic(SemanticType::Email)
                .sensitive(),
        ],
    )
    .with_primary_key(vec!["id".to_string()])
    .with_partitions(2)
}

fn ctx() -> RequestContext {
    RequestContext::root("acme").with_database(DatabaseName::new("crm").unwrap())
}

fn rows(count: i64) -> Vec<Map<String, Json>> {
    const COUNTRIES: [&str; 3] = ["uae", "usa", "uk"];
    (0..count)
        .map(|i| {
            let country = COUNTRIES[(i % 3) as usize];
            json!({
                "id": i,
                "company": format!("company {}", i % 10),
                "country": country,
                "score": (i % 100) as f64 / 100.0,
                "value": (i * 10) as f64,
                "created_at": 1_767_225_600_000_000i64 + i * 3_600_000_000,
                "email": format!("lead{i}@example.com")
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect()
}

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig::new(dir.path())).unwrap()
}

fn seeded(dir: &TempDir, count: i64) -> Engine {
    let engine = open(dir);
    let ctx = ctx();
    engine
        .create_database(&ctx, &DatabaseName::new("crm").unwrap())
        .unwrap();
    engine.create_table(&ctx, leads()).unwrap();
    engine
        .insert(&ctx, &TableName::new("leads").unwrap(), &rows(count))
        .unwrap();
    engine
}

#[test]
fn a_second_process_cannot_open_the_same_data_directory() {
    let dir = TempDir::new().unwrap();
    let first = open(&dir);
    let error = Engine::open(EngineConfig::new(dir.path())).unwrap_err();
    assert!(error.to_string().contains("already open"), "{error}");
    // Once the first is gone, the directory is available again.
    drop(first);
    let reopened = Engine::open(EngineConfig::new(dir.path())).unwrap();
    assert!(reopened.list_databases(&ctx()).unwrap().is_empty());
}

#[test]
fn the_full_agent_story_works_end_to_end() {
    // see "Design rationale" in ARCHITECTURE.md: create a table, store rows, then ask a question.
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 500);
    let ctx = ctx();

    assert_eq!(engine.list_databases(&ctx).unwrap(), vec!["crm"]);
    assert_eq!(engine.list_tables(&ctx).unwrap().len(), 1);

    let plan = PlanRequest::aggregate("leads")
        .grouped_by("country")
        .with_metric(MetricSpec::new(
            AggregateFunc::Sum,
            Some("value"),
            Some("pipeline"),
        ))
        .with_metric(MetricSpec::count())
        .ordered_by(OrderSpec::desc("pipeline"))
        .with_limit(3);
    let outcome = engine.query(&ctx, QuerySource::Plan(plan)).unwrap();
    assert_eq!(outcome.rows.len(), 3);
    assert_eq!(outcome.schema.names(), vec!["country", "pipeline", "count"]);
    assert_eq!(outcome.stats.rows_returned, 3);
    assert!(outcome.explain.contains("aggregate"), "{}", outcome.explain);

    let total: f64 = outcome
        .rows
        .iter()
        .map(|r| r["pipeline"].as_f64().unwrap())
        .sum();
    let expected: f64 = (0..500).map(|i| (i * 10) as f64).sum();
    assert!((total - expected).abs() < 0.001);
}

#[test]
fn natural_language_queries_run_and_report_their_interpretation() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 100);
    let ctx = ctx();

    let outcome = engine
        .query(
            &ctx,
            QuerySource::Request("how many leads are there".into()),
        )
        .unwrap();
    assert_eq!(outcome.rows.len(), 1);
    assert_eq!(outcome.rows[0]["count"], json!(100));
    assert!(outcome.interpretation.is_some());
    assert_eq!(outcome.plan.table, "leads");

    let outcome = engine
        .query(
            &ctx,
            QuerySource::Request("total value by country in leads".into()),
        )
        .unwrap();
    assert_eq!(outcome.rows.len(), 3);
    assert!(outcome.schema.names().contains(&"sum_value".to_string()));

    // A request the rules cannot handle is an error, not a wrong answer.
    let err = engine
        .query(&ctx, QuerySource::Request("frobnicate the widgets".into()))
        .unwrap_err();
    assert_eq!(err.code(), "bad_request");
}

#[test]
fn everything_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    {
        let engine = seeded(&dir, 300);
        let ctx = ctx();
        // Deliberately no flush: recovery must come from the WAL.
        engine
            .upsert(
                &ctx,
                &TableName::new("leads").unwrap(),
                &[
                    json!({"id": 0, "company": "updated", "country": "sg", "score": 1.0,
                         "value": 999.0, "created_at": 1_767_225_600_000_000i64,
                         "email": "x@example.com"})
                    .as_object()
                    .unwrap()
                    .clone(),
                ],
            )
            .unwrap();
        engine
            .delete(&ctx, &TableName::new("leads").unwrap(), &[json!(1)])
            .unwrap();
    }

    let engine = open(&dir);
    let ctx = ctx();
    // The catalog came back from the DDL log.
    assert_eq!(engine.list_databases(&ctx).unwrap(), vec!["crm"]);
    let schema = engine
        .describe_table(&ctx, &TableName::new("leads").unwrap())
        .unwrap();
    assert_eq!(schema.primary_key, vec!["id".to_string()]);

    let count = engine
        .query(&ctx, QuerySource::Request("how many leads".into()))
        .unwrap();
    assert_eq!(
        count.rows[0]["count"],
        json!(299),
        "the deleted row must stay deleted"
    );

    let updated = engine
        .get(&ctx, &TableName::new("leads").unwrap(), &[json!(0)])
        .unwrap();
    assert_eq!(updated[0]["company"], json!("updated"));
    assert!(engine
        .get(&ctx, &TableName::new("leads").unwrap(), &[json!(1)])
        .unwrap()
        .is_empty());
}

#[test]
fn permissions_are_enforced_per_operation() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 10);
    let reader = RequestContext::new(
        TenantId::new("acme").unwrap(),
        UserId("read-only-agent".into()),
        Scope::read_only(),
    )
    .with_database(DatabaseName::new("crm").unwrap());

    // Reads are allowed.
    engine.list_tables(&reader).unwrap();
    engine
        .query(
            &reader,
            QuerySource::Plan(PlanRequest::select("leads").with_limit(1)),
        )
        .unwrap();

    // Writes are not.
    let table = TableName::new("leads").unwrap();
    assert_eq!(
        engine.insert(&reader, &table, &rows(1)).unwrap_err().code(),
        "permission_denied"
    );
    assert_eq!(
        engine.upsert(&reader, &table, &rows(1)).unwrap_err().code(),
        "permission_denied"
    );
    assert_eq!(
        engine
            .delete(&reader, &table, &[json!(1)])
            .unwrap_err()
            .code(),
        "permission_denied"
    );
    assert_eq!(
        engine.drop_table(&reader, &table).unwrap_err().code(),
        "permission_denied"
    );
    assert_eq!(
        engine
            .create_database(&reader, &DatabaseName::new("other").unwrap())
            .unwrap_err()
            .code(),
        "permission_denied"
    );
}

#[test]
fn tenants_cannot_see_each_other() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 10);
    let intruder =
        RequestContext::root("evilcorp").with_database(DatabaseName::new("crm").unwrap());

    assert!(engine.list_databases(&intruder).unwrap().is_empty());
    assert_eq!(
        engine.list_tables(&intruder).unwrap_err().code(),
        "not_found"
    );
    assert_eq!(
        engine
            .query(&intruder, QuerySource::Plan(PlanRequest::select("leads")))
            .unwrap_err()
            .code(),
        "not_found"
    );

    // And a second tenant can use the same names without collision.
    engine
        .create_database(&intruder, &DatabaseName::new("crm").unwrap())
        .unwrap();
    engine.create_table(&intruder, leads()).unwrap();
    engine
        .insert(&intruder, &TableName::new("leads").unwrap(), &rows(5))
        .unwrap();
    let mine = engine
        .query(&ctx(), QuerySource::Request("how many leads".into()))
        .unwrap();
    let theirs = engine
        .query(&intruder, QuerySource::Request("how many leads".into()))
        .unwrap();
    assert_eq!(mine.rows[0]["count"], json!(10));
    assert_eq!(theirs.rows[0]["count"], json!(5));
}

#[test]
fn query_limits_come_from_the_request_context() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 1_000);
    let ctx = ctx().with_limits(QueryLimits {
        max_rows: 5,
        ..QueryLimits::unlimited()
    });

    let outcome = engine
        .query(&ctx, QuerySource::Plan(PlanRequest::select("leads")))
        .unwrap();
    assert_eq!(outcome.rows.len(), 5);
    assert!(outcome.stats.truncated);
    assert!(!outcome.warnings.is_empty());

    // A caller-supplied limit above the budget is clamped, with a warning.
    let outcome = engine
        .query(
            &ctx,
            QuerySource::Plan(PlanRequest::select("leads").with_limit(100)),
        )
        .unwrap();
    assert_eq!(outcome.rows.len(), 5);
    assert!(
        outcome.warnings.iter().any(|w| w.contains("row budget")),
        "{:?}",
        outcome.warnings
    );

    // Write size is capped too.
    let ctx = ctx.with_limits(QueryLimits {
        max_write_rows: 2,
        ..QueryLimits::unlimited()
    });
    let err = engine
        .insert(&ctx, &TableName::new("leads").unwrap(), &rows(3))
        .unwrap_err();
    assert_eq!(err.code(), "limit_exceeded");
}

#[test]
fn sensitive_columns_are_hidden_unless_named() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 5);
    let ctx = ctx();

    let outcome = engine
        .query(
            &ctx,
            QuerySource::Plan(PlanRequest::select("leads").with_limit(1)),
        )
        .unwrap();
    assert!(!outcome.schema.names().contains(&"email".to_string()));
    assert!(!engine.schema_context(&ctx).unwrap().contains("email"));

    let mut explicit = PlanRequest::select("leads").with_limit(1);
    explicit.columns = Some(vec!["email".to_string()]);
    let outcome = engine.query(&ctx, QuerySource::Plan(explicit)).unwrap();
    assert_eq!(outcome.schema.names(), vec!["email"]);
}

#[test]
fn ddl_is_validated_before_it_is_logged() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 1);
    let ctx = ctx();

    // Duplicates and unknown objects.
    assert_eq!(
        engine.create_table(&ctx, leads()).unwrap_err().code(),
        "already_exists"
    );
    assert_eq!(
        engine
            .create_database(&ctx, &DatabaseName::new("crm").unwrap())
            .unwrap_err()
            .code(),
        "already_exists"
    );
    assert_eq!(
        engine
            .drop_table(&ctx, &TableName::new("ghost").unwrap())
            .unwrap_err()
            .code(),
        "not_found"
    );

    // An invalid schema.
    let mut broken = leads();
    broken.name = TableName::new("broken").unwrap();
    broken.columns.clear();
    assert_eq!(
        engine.create_table(&ctx, broken).unwrap_err().code(),
        "invalid_schema"
    );

    // None of that corrupted the catalog, and it survives a reopen.
    assert_eq!(engine.list_tables(&ctx).unwrap().len(), 1);
    drop(engine);
    let engine = open(&dir);
    assert_eq!(engine.list_tables(&ctx).unwrap().len(), 1);
}

#[test]
fn schema_evolution_is_visible_to_queries_and_survives_restart() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 20);
    let ctx = ctx();

    let mut evolved = leads();
    evolved
        .columns
        .push(ColumnSchema::new("source", DataType::Utf8).described("where the lead came from"));
    let updated = engine.update_schema(&ctx, evolved).unwrap();
    assert_eq!(updated.version, 2);

    engine
        .insert(
            &ctx,
            &TableName::new("leads").unwrap(),
            &[
                json!({"id": 999, "company": "new", "country": "de", "score": 0.5, "value": 1.0,
                     "created_at": 1_767_225_600_000_000i64, "email": "n@example.com",
                     "source": "webinar"})
                .as_object()
                .unwrap()
                .clone(),
            ],
        )
        .unwrap();

    let outcome = engine
        .query(
            &ctx,
            QuerySource::Plan(
                PlanRequest::aggregate("leads")
                    .grouped_by("source")
                    .with_metric(MetricSpec::count()),
            ),
        )
        .unwrap();
    // Old rows read back as NULL for the new column.
    assert_eq!(outcome.rows.len(), 2);

    drop(engine);
    let engine = open(&dir);
    let schema = engine
        .describe_table(&ctx, &TableName::new("leads").unwrap())
        .unwrap();
    assert_eq!(schema.version, 2);
    assert!(schema.column("source").is_some());
}

#[test]
fn dropping_a_table_removes_its_data_for_good() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 50);
    let ctx = ctx();
    let table = TableName::new("leads").unwrap();
    engine.flush(&ctx, &table).unwrap();
    assert!(engine.table_stats(&ctx, &table).unwrap().bytes > 0);

    engine.drop_table(&ctx, &table).unwrap();
    assert_eq!(engine.list_tables(&ctx).unwrap().len(), 0);
    assert_eq!(
        engine.table_stats(&ctx, &table).unwrap_err().code(),
        "not_found"
    );

    // Recreating it gives an empty table, not the old rows.
    engine.create_table(&ctx, leads()).unwrap();
    let count = engine
        .query(&ctx, QuerySource::Request("how many leads".into()))
        .unwrap();
    assert_eq!(count.rows[0]["count"], json!(0));
}

#[test]
fn dropping_a_populated_database_needs_cascade() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 10);
    let ctx = ctx();
    let db = DatabaseName::new("crm").unwrap();

    let err = engine.drop_database(&ctx, &db, false).unwrap_err();
    assert!(err.to_string().contains("cascade"), "{err}");
    assert_eq!(engine.drop_database(&ctx, &db, true).unwrap(), 1);
    assert!(engine.list_databases(&ctx).unwrap().is_empty());
}

#[test]
fn filters_and_point_lookups_agree() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 100);
    let ctx = ctx();
    let table = TableName::new("leads").unwrap();

    let looked_up = engine
        .get(&ctx, &table, &[json!(42), json!({"id": 43})])
        .unwrap();
    assert_eq!(looked_up.len(), 2);
    assert_eq!(looked_up[0]["id"], json!(42));

    let queried = engine
        .query(
            &ctx,
            QuerySource::Plan(PlanRequest::select("leads").with_filter(FilterSpec::new(
                "id",
                FilterOp::Eq,
                json!(42),
            ))),
        )
        .unwrap();
    assert_eq!(queried.rows.len(), 1);
    assert_eq!(queried.rows[0]["value"], looked_up[0]["value"]);
}

#[test]
fn malformed_keys_are_rejected_with_guidance() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 5);
    let ctx = ctx();
    let table = TableName::new("leads").unwrap();

    let err = engine
        .get(&ctx, &table, &[json!({"wrong": 1})])
        .unwrap_err();
    assert!(
        err.to_string().contains("missing primary key column"),
        "{err}"
    );

    let err = engine.get(&ctx, &table, &[json!([1, 2])]).unwrap_err();
    assert!(err.to_string().contains("primary key has 1"), "{err}");

    let err = engine
        .get(&ctx, &table, &[json!("not an int")])
        .unwrap_err();
    assert_eq!(err.code(), "type_mismatch");
}

#[test]
fn stats_report_what_storage_is_doing() {
    let dir = TempDir::new().unwrap();
    let engine = seeded(&dir, 200);
    let ctx = ctx();
    let table = TableName::new("leads").unwrap();

    let before = engine.table_stats(&ctx, &table).unwrap();
    assert_eq!(before.rows, 200);
    assert_eq!(before.partitions, 2);
    assert_eq!(before.segments, 0, "nothing flushed yet");

    engine.flush(&ctx, &table).unwrap();
    let after = engine.table_stats(&ctx, &table).unwrap();
    assert_eq!(after.rows, 200);
    assert!(after.segments > 0);
    assert!(after.bytes > 0);
}

/// Durability under a hard kill.
///
/// The child process writes rows and then calls `abort()`, so nothing gets a
/// chance to run destructors or flush. The parent reopens the same directory and
/// checks that every acknowledged row is still there. This is the test that
/// makes "the WAL is the source of truth" more than a claim.
#[test]
fn acknowledged_writes_survive_a_hard_kill() {
    const CHILD_ENV: &str = "ADB_CRASH_TEST_DIR";

    if let Ok(dir) = std::env::var(CHILD_ENV) {
        let engine = Engine::open(EngineConfig::new(&dir)).unwrap();
        let ctx = ctx();
        engine
            .create_database(&ctx, &DatabaseName::new("crm").unwrap())
            .unwrap();
        engine.create_table(&ctx, leads()).unwrap();
        engine
            .insert(&ctx, &TableName::new("leads").unwrap(), &rows(250))
            .unwrap();
        // Acknowledged. Now die as violently as possible.
        std::process::abort();
    }

    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "acknowledged_writes_survive_a_hard_kill",
            "--nocapture",
        ])
        .env(CHILD_ENV, dir.path())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "the child was supposed to abort, not exit cleanly ({status:?})"
    );

    let engine = open(&dir);
    let ctx = ctx();
    assert_eq!(
        engine.list_databases(&ctx).unwrap(),
        vec!["crm"],
        "the DDL log should have been replayed"
    );
    let count = engine
        .query(&ctx, QuerySource::Request("how many leads".into()))
        .unwrap();
    assert_eq!(
        count.rows[0]["count"],
        json!(250),
        "every acknowledged row must be queryable after a crash"
    );

    // And the recovered table is still writable.
    engine
        .insert(
            &ctx,
            &TableName::new("leads").unwrap(),
            &[json!({
                "id": 10_000, "company": "post-crash", "country": "uae", "score": 0.1,
                "value": 1.0, "created_at": 1_767_225_600_000_000i64, "email": "a@example.com"
            })
            .as_object()
            .unwrap()
            .clone()],
        )
        .unwrap();
    let count = engine
        .query(&ctx, QuerySource::Request("how many leads".into()))
        .unwrap();
    assert_eq!(count.rows[0]["count"], json!(251));
}
