//! Ingest and query benchmark harness (see "Baseline numbers" in ARCHITECTURE.md).
//!
//! Measures what the readme asks for: ingest rows/sec, and query latency p50 /
//! p95 / p99 over a fixed query set, plus how much pruning actually happened.
//!
//! ```text
//! cargo run --release -p bench -- --rows 10000000
//! ```
//!
//! Two deliberate choices:
//!
//! * The table is **append-only**. That is the shape bulk analytical ingest
//!   should use: no primary key means no key index, so ingest is not paying for
//!   mutation support it does not need.
//! * Rows are generated deterministically, so two runs measure the same work.
//!
//! These numbers are a baseline for this engine, not a comparison with anything
//! else. Comparing to ClickHouse means running the same queries on ClickHouse
//! and saying which version and hardware; nothing here claims that.

use std::time::{Duration, Instant};

use adb_core::{
    Aggregation, ColumnSchema, DataType, DatabaseName, QueryLimits, RequestContext, SemanticType,
    TableName, TableSchema,
};
use adb_engine::{Engine, EngineConfig, QuerySource};
use adb_planner::AggregateFunc;
use adb_query::{FilterOp, FilterSpec, MetricSpec, OrderSpec, PlanRequest};
use adb_storage::table::StorageConfig;
use clap::Parser;
use serde_json::{json, Map, Value as Json};

#[derive(Debug, Parser)]
#[command(name = "adb-bench", about = "AgenticDB ingest and query benchmarks")]
struct Args {
    /// Rows to ingest.
    #[arg(long, default_value_t = 1_000_000)]
    rows: usize,

    /// Rows per insert call.
    #[arg(long, default_value_t = 50_000)]
    batch: usize,

    /// Partitions, i.e. query parallelism.
    #[arg(long, default_value_t = 8)]
    partitions: u32,

    /// Repetitions per query, for percentiles.
    #[arg(long, default_value_t = 9)]
    repeats: usize,

    /// Rows a memtable holds before it becomes a segment.
    ///
    /// This is what decides how many segments a table has, and therefore how
    /// much a time filter can prune. The default produces several segments per
    /// partition, which is what a real deployment looks like; raising it past
    /// the row count would leave one segment per partition and nothing to skip.
    #[arg(long, default_value_t = 32_768)]
    memtable_rows: usize,

    /// Where to write data. A temporary directory by default.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,

    /// fsync every WAL append. Off by default here: this measures the engine,
    /// not the disk's flush latency.
    #[arg(long)]
    fsync: bool,

    /// Keep the data directory afterwards.
    #[arg(long)]
    keep: bool,
}

const COUNTRIES: [&str; 8] = ["uae", "usa", "uk", "de", "sg", "in", "br", "jp"];
const START: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z
const HOUR: i64 = 3_600_000_000;

fn schema(partitions: u32) -> TableSchema {
    TableSchema::new(
        TableName::new("events").unwrap(),
        vec![
            ColumnSchema::new("event_id", DataType::Int64)
                .required()
                .semantic(SemanticType::Id),
            ColumnSchema::new("customer", DataType::Utf8)
                .required()
                .semantic(SemanticType::Category)
                .described("Customer that generated the event"),
            ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
            ColumnSchema::new("amount", DataType::Float64)
                .semantic(SemanticType::Currency)
                .aggregated_by(Aggregation::Sum),
            ColumnSchema::new("quantity", DataType::Int64).semantic(SemanticType::Quantity),
            ColumnSchema::new("timestamp", DataType::Timestamp)
                .required()
                .semantic(SemanticType::Timestamp),
        ],
    )
    .with_partitions(partitions)
}

/// Deterministic generator, so runs are comparable.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn batch(rng: &mut Rng, first_id: usize, count: usize) -> Vec<Map<String, Json>> {
    (0..count)
        .map(|offset| {
            let id = (first_id + offset) as i64;
            let noise = rng.next();
            let mut row = Map::with_capacity(6);
            row.insert("event_id".into(), json!(id));
            // ~100k distinct customers: high-cardinality grouping.
            row.insert(
                "customer".into(),
                json!(format!("cust-{}", noise % 100_000)),
            );
            row.insert(
                "country".into(),
                json!(COUNTRIES[(noise >> 20) as usize % COUNTRIES.len()]),
            );
            row.insert("amount".into(), json!((noise % 1_000_000) as f64 / 100.0));
            row.insert("quantity".into(), json!((noise % 10) as i64 + 1));
            // One event per hour, so time filters select contiguous segments.
            row.insert("timestamp".into(), json!(START + id * HOUR));
            row
        })
        .collect()
}

struct Case {
    name: &'static str,
    plan: PlanRequest,
}

fn cases(rows: usize) -> Vec<Case> {
    // Halfway through the time range: a filter that should prune about half the
    // segments.
    let midpoint = START + (rows as i64 / 2) * HOUR;
    vec![
        Case {
            name: "count(*)",
            plan: PlanRequest::aggregate("events").with_metric(MetricSpec::count()),
        },
        Case {
            name: "sum(amount)",
            plan: PlanRequest::aggregate("events").with_metric(MetricSpec::new(
                AggregateFunc::Sum,
                Some("amount"),
                Some("total"),
            )),
        },
        Case {
            name: "scan where timestamp > mid (limit 1000)",
            plan: PlanRequest::select("events")
                .with_filter(FilterSpec::new("timestamp", FilterOp::Gt, json!(midpoint)))
                .with_limit(1_000),
        },
        Case {
            name: "count(*) where timestamp > mid",
            plan: PlanRequest::aggregate("events")
                .with_filter(FilterSpec::new("timestamp", FilterOp::Gt, json!(midpoint)))
                .with_metric(MetricSpec::count()),
        },
        Case {
            name: "group by country",
            plan: PlanRequest::aggregate("events")
                .grouped_by("country")
                .with_metric(MetricSpec::new(
                    AggregateFunc::Sum,
                    Some("amount"),
                    Some("total"),
                ))
                .with_metric(MetricSpec::count()),
        },
        Case {
            name: "group by country, quantity",
            plan: PlanRequest::aggregate("events")
                .grouped_by("country")
                .grouped_by("quantity")
                .with_metric(MetricSpec::new(
                    AggregateFunc::Sum,
                    Some("amount"),
                    Some("total"),
                )),
        },
        Case {
            name: "group by customer (high cardinality)",
            plan: PlanRequest::aggregate("events")
                .grouped_by("customer")
                .with_metric(MetricSpec::new(
                    AggregateFunc::Sum,
                    Some("amount"),
                    Some("total"),
                )),
        },
        Case {
            name: "top 20 customers by revenue",
            plan: PlanRequest::aggregate("events")
                .grouped_by("customer")
                .with_metric(MetricSpec::new(
                    AggregateFunc::Sum,
                    Some("amount"),
                    Some("revenue"),
                ))
                .ordered_by(OrderSpec::desc("revenue"))
                .with_limit(20),
        },
        Case {
            name: "min/max/avg(amount) by country",
            plan: PlanRequest::aggregate("events")
                .grouped_by("country")
                .with_metric(MetricSpec::new(
                    AggregateFunc::Min,
                    Some("amount"),
                    Some("low"),
                ))
                .with_metric(MetricSpec::new(
                    AggregateFunc::Max,
                    Some("amount"),
                    Some("high"),
                ))
                .with_metric(MetricSpec::new(
                    AggregateFunc::Avg,
                    Some("amount"),
                    Some("mean"),
                )),
        },
    ]
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn main() -> adb_core::Result<()> {
    let args = Args::parse();
    let temp = if args.data_dir.is_none() {
        Some(std::env::temp_dir().join(format!("adb-bench-{}", std::process::id())))
    } else {
        None
    };
    let data_dir = args
        .data_dir
        .clone()
        .or_else(|| temp.clone())
        .expect("a data directory");
    std::fs::create_dir_all(&data_dir)?;

    let engine = Engine::open(EngineConfig {
        data_dir: data_dir.clone(),
        storage: StorageConfig {
            fsync: args.fsync,
            memtable_max_rows: args.memtable_rows,
            ..StorageConfig::default()
        },
        clock: None,
    })?;

    let ctx = RequestContext::root("bench").with_database(DatabaseName::new("bench").unwrap());
    let database = DatabaseName::new("bench").unwrap();
    if !engine.snapshot().has_database(&ctx.tenant, &database) {
        engine.create_database(&ctx, &database)?;
    }
    let table = TableName::new("events").unwrap();
    if engine.describe_table(&ctx, &table).is_err() {
        engine.create_table(&ctx, schema(args.partitions))?;
    }

    println!("AgenticDB benchmark");
    println!("  rows         {}", args.rows);
    println!("  batch        {}", args.batch);
    println!("  partitions   {}", args.partitions);
    println!("  fsync        {}", args.fsync);
    println!("  memtable     {} rows", args.memtable_rows);
    println!("  data_dir     {}", data_dir.display());
    println!();

    // --- ingest ---
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let started = Instant::now();
    let mut written = 0usize;
    while written < args.rows {
        let count = args.batch.min(args.rows - written);
        let rows = batch(&mut rng, written, count);
        engine.insert(&ctx, &table, &rows)?;
        written += count;
        if written.is_multiple_of(args.batch * 20) {
            let rate = written as f64 / started.elapsed().as_secs_f64();
            eprintln!("  ingested {written} rows ({rate:.0} rows/sec)");
        }
    }
    let ingest_elapsed = started.elapsed();

    let flush_started = Instant::now();
    engine.flush(&ctx, &table)?;
    let flush_elapsed = flush_started.elapsed();

    let stats = engine.table_stats(&ctx, &table)?;
    println!("ingest");
    println!(
        "  {:>12.0} rows/sec  ({} rows in {:.2}s)",
        args.rows as f64 / ingest_elapsed.as_secs_f64(),
        args.rows,
        ingest_elapsed.as_secs_f64()
    );
    println!("  {:>12.2}s final flush", flush_elapsed.as_secs_f64());
    println!(
        "  {:>12} segments, {:.1} MiB on disk ({:.1} bytes/row)",
        stats.segments,
        stats.bytes as f64 / (1024.0 * 1024.0),
        stats.bytes as f64 / args.rows as f64
    );
    println!("  {:>12} rows visible", stats.rows);
    println!();

    // --- queries ---
    println!(
        "{:<40} {:>9} {:>9} {:>9} {:>10} {:>8} {:>8}",
        "query", "p50 ms", "p95 ms", "p99 ms", "rows", "read", "pruned"
    );
    let limits = QueryLimits::unlimited();
    let ctx = ctx.with_limits(limits);
    for case in cases(args.rows) {
        let mut timings = Vec::with_capacity(args.repeats);
        let mut last = None;
        for _ in 0..args.repeats {
            let started = Instant::now();
            let outcome = engine.query(&ctx, QuerySource::Plan(case.plan.clone()))?;
            timings.push(started.elapsed());
            last = Some(outcome);
        }
        timings.sort();
        let outcome = last.expect("at least one repetition");
        println!(
            "{:<40} {:>9.2} {:>9.2} {:>9.2} {:>10} {:>8} {:>8}",
            case.name,
            ms(percentile(&timings, 0.50)),
            ms(percentile(&timings, 0.95)),
            ms(percentile(&timings, 0.99)),
            outcome.rows.len(),
            outcome.stats.segments_read,
            outcome.stats.segments_pruned,
        );
    }
    println!();
    println!("`read`/`pruned` are segments: pruned segments were skipped using statistics alone.");

    if let Some(temp) = temp {
        if args.keep {
            println!("data kept at {}", temp.display());
        } else {
            let _ = std::fs::remove_dir_all(&temp);
        }
    }
    Ok(())
}
