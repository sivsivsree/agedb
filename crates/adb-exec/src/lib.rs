//! Execution engine.
//!
//! Runs the linear physical pipeline from `adb-planner` over the partition
//! snapshots from `adb-storage`:
//!
//! ```text
//! per partition (in parallel): scan -> prune -> filter -> partial aggregate
//! coordinator:                 merge -> rename -> sort -> offset/limit
//! ```
//!
//! Partitions are scanned on separate threads (see "Concurrency" in ARCHITECTURE.md). The layer is
//! synchronous (callers wrap it in `spawn_blocking`) because a scan is CPU and
//! file-I/O bound and gains nothing from being async.

pub mod aggregate;
pub mod budget;
pub mod eval;
pub mod prune;
pub mod scan;
pub mod sort;

use std::sync::Arc;

use adb_core::{AdbError, QueryLimits, Result};
use adb_planner::{AggregateFunc, OutputSchema, PhysicalPlan};
use adb_storage::rows;
use adb_storage::table::PartitionSnapshot;
use arrow::array::RecordBatch;

pub use aggregate::Aggregator;
pub use budget::{Budget, ExecStats};

/// The result of one query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub batch: RecordBatch,
    pub stats: ExecStats,
    /// Things the caller should know: a truncated result, a clamped limit.
    pub warnings: Vec<String>,
}

impl QueryResult {
    pub fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// JSON rows for an API response.
    pub fn to_json_rows(&self) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        rows::batch_to_json_rows(&self.batch)
    }
}

/// Execute `plan` against `snapshots`.
pub fn execute(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
    limits: QueryLimits,
) -> Result<QueryResult> {
    execute_with_budget(plan, snapshots, Budget::new(limits))
}

/// Execute `plan` under an existing budget.
///
/// The deadline is checked inside the scan and again at every stage boundary
/// on the coordinator (after the scans, per merged partial, and before
/// finishing and sorting), so no expensive stage starts once time is up.
pub fn execute_with_budget(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
    budget: Budget,
) -> Result<QueryResult> {
    let mut warnings = Vec::new();

    // `count(*)` with no filter is answerable from segment and memtable
    // metadata, which is exact because suppression counts are exact.
    if let Some(batch) = try_metadata_count(plan, snapshots)? {
        let mut stats = budget.stats();
        stats.rows_returned = batch.num_rows() as u64;
        return Ok(QueryResult {
            batch,
            stats,
            warnings,
        });
    }

    let combined = if plan.aggregate.is_some() {
        run_aggregate(plan, snapshots, &budget)?
    } else {
        run_scan(plan, snapshots, &budget, &mut warnings)?
    };

    let renamed = apply_rename(combined, plan)?;
    budget.check_deadline()?;
    // No check after the sort: what remains is cheap, and failing a finished
    // answer would only make the caller rerun the expensive part.
    let sorted = sort::sort_batch(&renamed, &plan.sort, plan.fetch_rows())?;
    let limited = apply_offset_limit(&sorted, plan, &budget, &mut warnings);
    let aligned = align_output(&limited, &plan.output)?;

    let mut stats = budget.stats();
    stats.rows_returned = aligned.num_rows() as u64;
    if stats.truncated {
        warnings.push(format!(
            "result truncated to the {} row budget; add a limit or a filter",
            budget.limits().max_rows
        ));
    }
    Ok(QueryResult {
        batch: aligned,
        stats,
        warnings,
    })
}

/// Scan partitions in parallel and concatenate.
fn run_scan(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
    budget: &Budget,
    _warnings: &mut Vec<String>,
) -> Result<RecordBatch> {
    // Without a sort, a limit lets each partition stop early.
    let per_partition_cap = if plan.is_streaming_limit() {
        plan.fetch_rows()
    } else {
        None
    };

    let collected: Vec<Vec<RecordBatch>> = run_per_partition(snapshots, |snapshot| {
        let mut batches = Vec::new();
        let mut rows = 0usize;
        scan::scan_partition(snapshot, plan, budget, |batch| {
            rows += batch.num_rows();
            batches.push(batch);
            Ok(match per_partition_cap {
                Some(cap) => rows < cap,
                None => true,
            })
        })?;
        Ok(batches)
    })?;

    budget.check_deadline()?;
    let batches: Vec<RecordBatch> = collected.into_iter().flatten().collect();
    concat_or_empty(batches, plan)
}

/// Build a partial aggregate per partition, then merge.
fn run_aggregate(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
    budget: &Budget,
) -> Result<RecordBatch> {
    let spec = plan.aggregate.as_ref().expect("caller checked");
    let input_schema = scan_output_schema(plan, snapshots)?;

    let partials: Vec<Option<Aggregator>> = run_per_partition(snapshots, |snapshot| {
        let mut aggregator = Aggregator::new(spec, &input_schema, &plan.output);
        let mut saw_rows = false;
        scan::scan_partition(snapshot, plan, budget, |batch| {
            saw_rows = true;
            aggregator.update(&batch)?;
            Ok(true)
        })?;
        Ok(if saw_rows || !aggregator.is_grouped() {
            Some(aggregator)
        } else {
            None
        })
    })?;

    let mut merged: Option<Aggregator> = None;
    for partial in partials.into_iter().flatten() {
        budget.check_deadline()?;
        merged = Some(match merged {
            None => partial,
            Some(mut acc) => {
                acc.merge(partial)?;
                acc
            }
        });
    }
    let aggregator = match merged {
        Some(aggregator) => aggregator,
        None => Aggregator::new(spec, &input_schema, &plan.output),
    };
    budget.check_deadline()?;
    aggregator.finish()
}

/// Run `work` on every partition, on its own thread.
fn run_per_partition<T, F>(snapshots: &[Arc<PartitionSnapshot>], work: F) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(&PartitionSnapshot) -> Result<T> + Send + Sync,
{
    if snapshots.len() <= 1 {
        return snapshots.iter().map(|s| work(s)).collect();
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = snapshots
            .iter()
            .map(|snapshot| {
                let work = &work;
                scope.spawn(move || work(snapshot))
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| AdbError::internal("a scan thread panicked"))?
            })
            .collect()
    })
}

/// Answer `count(*)` from metadata when nothing needs to be read.
fn try_metadata_count(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
) -> Result<Option<RecordBatch>> {
    let Some(spec) = &plan.aggregate else {
        return Ok(None);
    };
    if plan.filter.is_some() || !spec.group_by.is_empty() || spec.aggregates.len() != 1 {
        return Ok(None);
    }
    let agg = &spec.aggregates[0];
    if agg.func != AggregateFunc::Count || agg.column.is_some() {
        return Ok(None);
    }
    let total: u64 = snapshots.iter().map(|s| s.visible_rows()).sum();
    let array = aggregate::build_array(
        &[adb_core::Value::Int(total as i64)],
        adb_core::DataType::Int64,
    )?;
    let field = arrow::datatypes::Field::new(&agg.alias, arrow::datatypes::DataType::Int64, false);
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![field])),
        vec![array],
    )?;
    Ok(Some(batch))
}

/// The shape a scan produces, needed to type aggregate arguments.
fn scan_output_schema(
    plan: &PhysicalPlan,
    snapshots: &[Arc<PartitionSnapshot>],
) -> Result<OutputSchema> {
    let schema = snapshots
        .first()
        .map(|s| s.schema.clone())
        .ok_or_else(|| AdbError::internal("table has no partitions"))?;
    Ok(OutputSchema::new(
        plan.projection
            .iter()
            .map(|name| {
                let col = schema.require_column(name)?;
                Ok(adb_planner::ColumnMeta {
                    name: col.name.clone(),
                    data_type: col.data_type,
                    nullable: col.nullable,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn concat_or_empty(batches: Vec<RecordBatch>, plan: &PhysicalPlan) -> Result<RecordBatch> {
    match batches.len() {
        0 => {
            // Nothing matched: return the right shape with no rows.
            let fields = plan
                .output
                .columns
                .iter()
                .map(|meta| {
                    arrow::datatypes::Field::new(
                        &meta.name,
                        meta.data_type.arrow_type(),
                        meta.nullable,
                    )
                })
                .collect::<Vec<_>>();
            Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::new(fields),
            )))
        }
        1 => Ok(batches.into_iter().next().expect("length checked")),
        _ => {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches).map_err(Into::into)
        }
    }
}

fn apply_rename(batch: RecordBatch, plan: &PhysicalPlan) -> Result<RecordBatch> {
    let Some(map) = &plan.rename else {
        return Ok(batch);
    };
    if batch.num_rows() == 0 && batch.schema().fields().len() == plan.output.columns.len() {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(map.len());
    let mut arrays = Vec::with_capacity(map.len());
    for (source, alias) in map {
        let index = batch
            .schema()
            .index_of(source)
            .map_err(|_| AdbError::not_found("column", source))?;
        let field = batch.schema().field(index).clone().with_name(alias);
        fields.push(field);
        arrays.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)
        .map_err(Into::into)
}

fn apply_offset_limit(
    batch: &RecordBatch,
    plan: &PhysicalPlan,
    budget: &Budget,
    _warnings: &mut Vec<String>,
) -> RecordBatch {
    let rows = batch.num_rows();
    let offset = plan.offset.min(rows);
    let remaining = rows - offset;
    // An explicit limit wins; otherwise the caller's row budget applies and the
    // truncation is reported rather than hidden.
    let take = match plan.limit {
        Some(limit) => limit.min(remaining),
        None => {
            let budgeted = budget.limits().max_rows.min(remaining);
            if budgeted < remaining {
                budget.note_truncated();
            }
            budgeted
        }
    };
    batch.slice(offset, take)
}

/// Final safety net: the returned batch must have exactly the columns the
/// validated plan promised, in order.
fn align_output(batch: &RecordBatch, output: &OutputSchema) -> Result<RecordBatch> {
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let wanted = output.names();
    if names == wanted {
        return Ok(batch.clone());
    }
    let indices = wanted
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
