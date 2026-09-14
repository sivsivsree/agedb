//! The scan operator: memtable plus pruned segments, with suppressed rows
//! removed and the residual filter applied.
//!
//! Sources are visited in arrival order (segments oldest-first, then the
//! memtable) so unsorted results are deterministic. Suppressed rows, those
//! superseded by a later update or deleted (see "Writes, updates and deletes" in
//! ARCHITECTURE.md), are dropped
//! here, which is what makes "the query layer resolves the latest version" true.

use adb_core::Result;
use adb_planner::PhysicalPlan;
use adb_storage::segment;
use adb_storage::table::PartitionSnapshot;
use arrow::array::{RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::budget::Budget;
use crate::eval;
use crate::prune;

/// Feed every visible, matching batch of one partition to `sink`.
///
/// `sink` returns `false` to stop early, which is how an unsorted `LIMIT` avoids
/// reading the rest of the table.
pub fn scan_partition<F>(
    snapshot: &PartitionSnapshot,
    plan: &PhysicalPlan,
    budget: &Budget,
    mut sink: F,
) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<bool>,
{
    let schema = &snapshot.schema;

    for entry in &snapshot.segments {
        budget.check_deadline()?;
        if prune::can_skip(&entry.meta, &plan.pruning) {
            budget.note_segment_pruned();
            continue;
        }
        budget.charge_bytes(entry.meta.bytes)?;
        budget.note_segment_read();
        let batch = segment::read_segment(
            snapshot.store.as_ref(),
            &entry.meta,
            schema,
            Some(&plan.projection),
        )?;
        budget.charge_rows(batch.num_rows() as u64);
        let batch = drop_suppressed(&batch, &entry.suppressed, 0)?;
        if !emit(batch, plan, &mut sink)? {
            return Ok(());
        }
    }

    for (offset, batch) in snapshot.memtable_with_offsets() {
        budget.check_deadline()?;
        budget.charge_bytes(batch.get_array_memory_size() as u64)?;
        budget.charge_rows(batch.num_rows() as u64);
        let projected = segment::project_with_backfill(batch, schema, &plan.projection)?;
        let live = drop_suppressed(&projected, &snapshot.memtable_suppressed, offset)?;
        if !emit(live, plan, &mut sink)? {
            return Ok(());
        }
    }
    Ok(())
}

fn emit<F>(batch: RecordBatch, plan: &PhysicalPlan, sink: &mut F) -> Result<bool>
where
    F: FnMut(RecordBatch) -> Result<bool>,
{
    if batch.num_rows() == 0 {
        return Ok(true);
    }
    let batch = match &plan.filter {
        None => batch,
        Some(predicate) => eval::filter_batch(predicate, &batch)?,
    };
    if batch.num_rows() == 0 {
        return Ok(true);
    }
    sink(batch)
}

/// Remove rows superseded by a later write.
///
/// `offset` shifts memtable ordinals, which are global across the memtable's
/// batches, into the local row numbering of this batch.
fn drop_suppressed(
    batch: &RecordBatch,
    suppressed: &RoaringBitmap,
    offset: u32,
) -> Result<RecordBatch> {
    if suppressed.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let rows = batch.num_rows() as u32;
    // Cheap exit when nothing in this batch's ordinal range is suppressed.
    if suppressed.range_cardinality(offset..offset.saturating_add(rows)) == 0 {
        return Ok(batch.clone());
    }
    let keep: Vec<u32> = (0..rows)
        .filter(|row| !suppressed.contains(offset + row))
        .collect();
    if keep.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let indices = UInt32Array::from(keep);
    arrow::compute::take_record_batch(batch, &indices).map_err(Into::into)
}
