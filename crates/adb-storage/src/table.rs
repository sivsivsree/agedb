//! Partitioned table storage: the piece that ties log, memtable, segments and
//! manifest together.
//!
//! # Model
//!
//! A table is `partitions` independent partitions (see "Concurrency" in ARCHITECTURE.md). Each owns a
//! WAL, a memtable, a set of immutable segments, and, for tables with a primary
//! key, a key index. Reads take a cheap `Arc` snapshot per partition and run in
//! parallel; nothing blocks on writers.
//!
//! # Row visibility
//!
//! At most one row per primary key is visible. Writes suppress the previous
//! location of a key (via a Roaring bitmap per source) instead of rewriting the
//! segment, so an `UPDATE` is O(changed rows), not O(table) (see "Writes, updates and deletes" in ARCHITECTURE.md).
//! Append-only tables (no primary key) keep no index and suppress nothing, which
//! is the shape bulk analytical ingest should use.
//!
//! # Write serialization
//!
//! Writes to one table take a table-level lock. Primary-key uniqueness and
//! all-or-nothing multi-partition writes both need it, and bulk ingest sends
//! large batches, so the lock is not the bottleneck. Different tables write
//! concurrently, and reads never take it. Lifting this to per-partition locking
//! for append-only tables is a known follow-up.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use adb_core::{AdbError, DatabaseName, Result, TableName, TableSchema, TenantId};
use arrow::array::{RecordBatch, UInt32Array};
use roaring::RoaringBitmap;

use crate::compaction::{self, CompactionPolicy, SegmentFacts};
use crate::hash::hash_key;
use crate::keyindex::{KeyIndex, Locator, RowKey, Source};
use crate::manifest::Manifest;
use crate::memtable::Memtable;
use crate::object_store::ObjectStore;
use crate::segment::{self, SegmentMeta};
use crate::wal::{FileLogStore, LogEntry, LogStore, Mutation};
use crate::{paths, rows};

#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// fsync the WAL on every append. Off is for benchmarks only.
    pub fsync: bool,
    pub memtable_max_rows: usize,
    pub memtable_max_bytes: usize,
    pub compaction: CompactionPolicy,
    /// Run compaction inline after a flush when the policy asks for it.
    pub auto_compact: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            fsync: true,
            memtable_max_rows: 512 * 1024,
            memtable_max_bytes: 128 << 20,
            compaction: CompactionPolicy::default(),
            auto_compact: true,
        }
    }
}

/// One segment plus the rows in it that later writes have superseded.
#[derive(Debug, Clone)]
pub struct SegmentEntry {
    pub meta: Arc<SegmentMeta>,
    pub suppressed: Arc<RoaringBitmap>,
}

impl SegmentEntry {
    pub fn visible_rows(&self) -> u64 {
        self.meta.row_count.saturating_sub(self.suppressed.len())
    }
}

/// Immutable read view of one partition. Everything the executor needs.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    pub partition: u32,
    pub schema: Arc<TableSchema>,
    pub store: Arc<dyn ObjectStore>,
    /// Memtable batches in arrival order.
    pub memtable: Vec<RecordBatch>,
    /// Suppressed memtable row ordinals (global across `memtable`).
    pub memtable_suppressed: Arc<RoaringBitmap>,
    pub segments: Vec<SegmentEntry>,
    /// LSN materialized into segments at the time of the snapshot.
    pub applied_lsn: u64,
}

impl PartitionSnapshot {
    /// Visible rows, without reading any segment.
    pub fn visible_rows(&self) -> u64 {
        let memtable_rows: u64 = self.memtable.iter().map(|b| b.num_rows() as u64).sum();
        let live_memtable = memtable_rows.saturating_sub(self.memtable_suppressed.len());
        live_memtable
            + self
                .segments
                .iter()
                .map(SegmentEntry::visible_rows)
                .sum::<u64>()
    }

    /// Bytes a full scan would read, for limit accounting.
    pub fn scan_bytes(&self) -> u64 {
        let memtable: u64 = self
            .memtable
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        memtable + self.segments.iter().map(|s| s.meta.bytes).sum::<u64>()
    }

    /// Memtable batches paired with their starting ordinal.
    pub fn memtable_with_offsets(&self) -> Vec<(u32, &RecordBatch)> {
        let mut out = Vec::with_capacity(self.memtable.len());
        let mut offset = 0u32;
        for batch in &self.memtable {
            out.push((offset, batch));
            offset += batch.num_rows() as u32;
        }
        out
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriteOutcome {
    pub rows: usize,
    /// Segments created by flushes triggered by this write.
    pub segments_flushed: usize,
}

#[derive(Debug)]
struct PartitionState {
    schema: Arc<TableSchema>,
    memtable: Memtable,
    memtable_suppressed: Arc<RoaringBitmap>,
    segments: Vec<SegmentEntry>,
    key_index: KeyIndex,
    /// Checkpointed into the manifest.
    applied_lsn: u64,
    /// Highest LSN reflected in memory.
    last_lsn: u64,
    next_segment_id: u64,
}

impl PartitionState {
    fn suppress(&mut self, locator: Locator) {
        match locator.source {
            Source::Memtable => {
                Arc::make_mut(&mut self.memtable_suppressed).insert(locator.ordinal);
            }
            Source::Segment(id) => {
                if let Some(entry) = self.segments.iter_mut().find(|e| e.meta.id == id) {
                    Arc::make_mut(&mut entry.suppressed).insert(locator.ordinal);
                }
            }
        }
    }

    /// Apply already-decoded rows. Insert and upsert behave identically here:
    /// uniqueness is enforced *before* the record is logged, so replay must
    /// never fail on a duplicate.
    fn apply_rows(&mut self, batch: RecordBatch, lsn: u64) -> Result<()> {
        if batch.num_rows() > 0 && !self.schema.is_append_only() {
            let pk = self.schema.pk_indices();
            let start = self.memtable.rows() as u32;
            for row in 0..batch.num_rows() {
                let key = rows::key_at(&batch, &pk, row)?;
                let locator = Locator::memtable(start + row as u32);
                if let Some(previous) = self.key_index.insert(key, locator) {
                    self.suppress(previous);
                }
            }
        }
        self.memtable.append(batch);
        self.last_lsn = self.last_lsn.max(lsn);
        Ok(())
    }

    fn apply_deletes(&mut self, keys: &[RowKey], lsn: u64) {
        for key in keys {
            if let Some(previous) = self.key_index.remove(key) {
                self.suppress(previous);
            }
        }
        self.last_lsn = self.last_lsn.max(lsn);
    }

    /// Replay path: decode and apply a logged entry.
    fn apply_entry(&mut self, entry: &LogEntry) -> Result<()> {
        match &entry.mutation {
            Mutation::Ddl { .. } => Err(AdbError::Corruption(
                "found a DDL record in a table WAL".to_string(),
            )),
            Mutation::Insert { ipc } | Mutation::Upsert { ipc } => {
                let batch = rows::decode_ipc(ipc)?;
                self.apply_rows(batch, entry.lsn)
            }
            Mutation::Delete { keys } => {
                self.apply_deletes(keys, entry.lsn);
                Ok(())
            }
        }
    }

    fn segment_facts(&self) -> Vec<SegmentFacts> {
        self.segments
            .iter()
            .map(|e| SegmentFacts {
                id: e.meta.id,
                rows: e.meta.row_count,
                bytes: e.meta.bytes,
                suppressed: e.suppressed.len(),
            })
            .collect()
    }
}

#[derive(Debug)]
struct Partition {
    id: u32,
    tenant: TenantId,
    database: DatabaseName,
    table: TableName,
    manifest_key: String,
    store: Arc<dyn ObjectStore>,
    log: FileLogStore,
    state: RwLock<PartitionState>,
    config: StorageConfig,
}

impl Partition {
    fn open(
        store: Arc<dyn ObjectStore>,
        tenant: &TenantId,
        database: &DatabaseName,
        schema: Arc<TableSchema>,
        id: u32,
        config: StorageConfig,
    ) -> Result<Self> {
        let table = schema.name.clone();
        let manifest_key = paths::manifest(tenant, database, &table, id);
        let wal_key = paths::wal(tenant, database, &table, id);
        let wal_path = store.local_path(&wal_key).ok_or_else(|| {
            AdbError::Unsupported(
                "the WAL needs a filesystem-backed object store in v0.1".to_string(),
            )
        })?;

        let manifest = Manifest::load(store.as_ref(), &manifest_key)?
            .unwrap_or_else(|| Manifest::new(id, schema.version));

        // A crash between writing a segment and committing the manifest leaves an
        // unreferenced Parquet file. Nothing points at it, so delete it now
        // rather than leaking storage.
        let referenced: HashSet<&str> = manifest.segment_keys().into_iter().collect();
        let segments_prefix = format!(
            "{}/segments",
            paths::partition_prefix(tenant, database, &table, id)
        );
        for key in store.list(&segments_prefix)? {
            if !referenced.contains(key.as_str()) {
                tracing::warn!(key = %key, "removing orphaned segment left by an interrupted flush");
                store.delete(&key)?;
            }
        }

        let mut segments = Vec::with_capacity(manifest.segments.len());
        for meta in &manifest.segments {
            segments.push(SegmentEntry {
                suppressed: Arc::new(manifest.suppressed_bitmap(meta.id)?),
                meta: Arc::new(meta.clone()),
            });
        }
        segments.sort_by_key(|e| e.meta.id);

        let mut state = PartitionState {
            schema: schema.clone(),
            memtable: Memtable::new(schema.clone()),
            memtable_suppressed: Arc::new(RoaringBitmap::new()),
            segments,
            key_index: KeyIndex::new(),
            applied_lsn: manifest.applied_lsn,
            last_lsn: manifest.applied_lsn,
            next_segment_id: manifest.next_segment_id,
        };

        // The key index is in-memory only, so it is rebuilt from the primary-key
        // columns of each segment. Costly at startup for large PK tables; the
        // alternative (persisting it) is a follow-up.
        if !schema.is_append_only() {
            Self::rebuild_key_index(store.as_ref(), &schema, &mut state)?;
        }

        let recovered = FileLogStore::open(&wal_path, config.fsync)?;
        recovered.store.advance_to(manifest.applied_lsn);
        let mut replayed = 0usize;
        for entry in recovered
            .entries
            .iter()
            .filter(|e| e.lsn > manifest.applied_lsn)
        {
            state.apply_entry(entry)?;
            replayed += 1;
        }
        if replayed > 0 {
            tracing::info!(
                table = %table,
                partition = id,
                entries = replayed,
                from_lsn = manifest.applied_lsn,
                "replayed WAL records into the memtable"
            );
        }

        Ok(Self {
            id,
            tenant: tenant.clone(),
            database: database.clone(),
            table,
            manifest_key,
            store,
            log: recovered.store,
            state: RwLock::new(state),
            config,
        })
    }

    fn rebuild_key_index(
        store: &dyn ObjectStore,
        schema: &TableSchema,
        state: &mut PartitionState,
    ) -> Result<()> {
        let pk_columns = schema.primary_key.clone();
        let pk_positions: Vec<usize> = (0..pk_columns.len()).collect();
        for entry in &state.segments {
            let batch = segment::read_segment(store, &entry.meta, schema, Some(&pk_columns))?;
            for row in 0..batch.num_rows() {
                let ordinal = row as u32;
                if entry.suppressed.contains(ordinal) {
                    continue;
                }
                let key = rows::key_at(&batch, &pk_positions, row)?;
                // Segments are visited in ascending id order, so a later
                // segment legitimately wins, though suppression should already
                // guarantee there is no contest.
                state
                    .key_index
                    .insert(key, Locator::segment(entry.meta.id, ordinal));
            }
        }
        Ok(())
    }

    fn read_state(&self) -> Result<std::sync::RwLockReadGuard<'_, PartitionState>> {
        self.state
            .read()
            .map_err(|_| AdbError::internal("partition state lock poisoned"))
    }

    fn write_state(&self) -> Result<std::sync::RwLockWriteGuard<'_, PartitionState>> {
        self.state
            .write()
            .map_err(|_| AdbError::internal("partition state lock poisoned"))
    }

    fn snapshot(&self) -> Result<Arc<PartitionSnapshot>> {
        let state = self.read_state()?;
        Ok(Arc::new(PartitionSnapshot {
            partition: self.id,
            schema: state.schema.clone(),
            store: self.store.clone(),
            memtable: state.memtable.batches().to_vec(),
            memtable_suppressed: state.memtable_suppressed.clone(),
            segments: state.segments.clone(),
            applied_lsn: state.applied_lsn,
        }))
    }

    fn should_flush(&self, state: &PartitionState) -> bool {
        state.memtable.rows() >= self.config.memtable_max_rows
            || state.memtable.bytes() >= self.config.memtable_max_bytes
    }

    /// Log rows, apply them, and flush if the memtable is full.
    fn append_rows(&self, batch: RecordBatch, upsert: bool) -> Result<usize> {
        let ipc = rows::encode_ipc(&batch)?;
        let mutation = if upsert {
            Mutation::Upsert { ipc }
        } else {
            Mutation::Insert { ipc }
        };
        let mut state = self.write_state()?;
        let lsn = self.log.append(&mutation)?;
        state.apply_rows(batch, lsn)?;
        if self.should_flush(&state) {
            let flushed = self.flush_locked(&mut state)?;
            return Ok(usize::from(flushed.is_some()));
        }
        Ok(0)
    }

    /// Tombstone keys that exist here; returns how many were actually present.
    fn delete_keys(&self, keys: &[RowKey]) -> Result<usize> {
        let mut state = self.write_state()?;
        let present: Vec<RowKey> = keys
            .iter()
            .filter(|k| state.key_index.contains(k))
            .cloned()
            .collect();
        if present.is_empty() {
            return Ok(0);
        }
        let mutation = Mutation::Delete {
            keys: present.clone(),
        };
        let lsn = self.log.append(&mutation)?;
        state.apply_deletes(&present, lsn);
        Ok(present.len())
    }

    fn flush(&self) -> Result<Option<u64>> {
        let mut state = self.write_state()?;
        self.flush_locked(&mut state)
    }

    /// Materialize the memtable into a segment and checkpoint.
    ///
    /// Ordering matters: the segment file is written first, then the manifest is
    /// atomically replaced, then the WAL prefix is dropped. A crash at any point
    /// leaves either the old checkpoint plus a replayable WAL, or the new
    /// checkpoint, never a gap.
    fn flush_locked(&self, state: &mut PartitionState) -> Result<Option<u64>> {
        let schema = state.schema.clone();
        let buffered = state.memtable.concat()?;
        let mut created = None;

        if let Some(batch) = buffered {
            let suppressed = state.memtable_suppressed.clone();
            let keep: Vec<u32> = (0..batch.num_rows() as u32)
                .filter(|ordinal| !suppressed.contains(*ordinal))
                .collect();

            if !keep.is_empty() {
                let segment_id = state.next_segment_id;
                let seg_batch = if keep.len() == batch.num_rows() {
                    batch.clone()
                } else {
                    take_rows(&batch, &keep)?
                };
                let key = paths::segment(
                    &self.tenant,
                    &self.database,
                    &self.table,
                    self.id,
                    segment_id,
                );
                let meta = segment::write_segment(
                    self.store.as_ref(),
                    &key,
                    segment_id,
                    &schema,
                    &seg_batch,
                )?;

                if !schema.is_append_only() {
                    let pk = schema.pk_indices();
                    for (new_ordinal, &old_ordinal) in keep.iter().enumerate() {
                        let row_key = rows::key_at(&batch, &pk, old_ordinal as usize)?;
                        state.key_index.relocate(
                            &row_key,
                            Locator::memtable(old_ordinal),
                            Locator::segment(segment_id, new_ordinal as u32),
                        );
                    }
                }

                state.segments.push(SegmentEntry {
                    meta: Arc::new(meta),
                    suppressed: Arc::new(RoaringBitmap::new()),
                });
                state.next_segment_id += 1;
                created = Some(segment_id);
            }

            state.memtable.clear();
            state.memtable_suppressed = Arc::new(RoaringBitmap::new());
        } else if state.last_lsn == state.applied_lsn {
            return Ok(None); // nothing buffered, nothing new to checkpoint
        }

        state.applied_lsn = state.last_lsn;
        self.commit_manifest(state)?;
        self.log.truncate_prefix(state.applied_lsn)?;

        if self.config.auto_compact {
            let ids = compaction::select(&state.segment_facts(), &self.config.compaction);
            if !ids.is_empty() {
                self.compact_locked(state, &ids)?;
            }
        }
        Ok(created)
    }

    fn commit_manifest(&self, state: &PartitionState) -> Result<()> {
        let mut manifest = Manifest::new(self.id, state.schema.version);
        manifest.applied_lsn = state.applied_lsn;
        manifest.next_segment_id = state.next_segment_id;
        for entry in &state.segments {
            manifest.segments.push((*entry.meta).clone());
            manifest.set_suppressed(entry.meta.id, &entry.suppressed)?;
        }
        manifest.save(self.store.as_ref(), &self.manifest_key)
    }

    fn compact(&self) -> Result<Option<u64>> {
        let mut state = self.write_state()?;
        let ids = compaction::select(&state.segment_facts(), &self.config.compaction);
        if ids.is_empty() {
            return Ok(None);
        }
        self.compact_locked(&mut state, &ids)
    }

    /// Merge `ids` into a single new segment, dropping suppressed rows.
    ///
    /// Only surviving rows are carried over, and every key in them is a winner,
    /// so giving the output a fresh (highest) id cannot reorder any key's
    /// history.
    fn compact_locked(&self, state: &mut PartitionState, ids: &[u64]) -> Result<Option<u64>> {
        let schema = state.schema.clone();
        let columns = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let mut inputs = Vec::new();
        for id in ids {
            let Some(entry) = state.segments.iter().find(|e| e.meta.id == *id) else {
                continue;
            };
            inputs.push(entry.clone());
        }
        if inputs.is_empty() {
            return Ok(None);
        }
        inputs.sort_by_key(|e| e.meta.id);

        // (old segment id, surviving rows, their original ordinals)
        let mut kept: Vec<(u64, RecordBatch, Vec<u32>)> = Vec::new();
        for entry in &inputs {
            let batch =
                segment::read_segment(self.store.as_ref(), &entry.meta, &schema, Some(&columns))?;
            let keep: Vec<u32> = (0..batch.num_rows() as u32)
                .filter(|ordinal| !entry.suppressed.contains(*ordinal))
                .collect();
            if keep.is_empty() {
                continue;
            }
            let survivors = if keep.len() == batch.num_rows() {
                batch.clone()
            } else {
                take_rows(&batch, &keep)?
            };
            kept.push((entry.meta.id, survivors, keep));
        }

        let new_id = state.next_segment_id;
        let mut new_entry = None;
        if !kept.is_empty() {
            let batches: Vec<RecordBatch> = kept.iter().map(|(_, b, _)| b.clone()).collect();
            let arrow_schema = batches[0].schema();
            let merged = if batches.len() == 1 {
                batches[0].clone()
            } else {
                arrow::compute::concat_batches(&arrow_schema, &batches)?
            };
            let key = paths::segment(&self.tenant, &self.database, &self.table, self.id, new_id);
            let meta = segment::write_segment(self.store.as_ref(), &key, new_id, &schema, &merged)?;

            if !schema.is_append_only() {
                let pk = schema.pk_indices();
                let mut new_ordinal = 0u32;
                for (old_id, survivors, old_ordinals) in &kept {
                    for (row, &old_ordinal) in old_ordinals.iter().enumerate() {
                        let row_key = rows::key_at(survivors, &pk, row)?;
                        state.key_index.relocate(
                            &row_key,
                            Locator::segment(*old_id, old_ordinal),
                            Locator::segment(new_id, new_ordinal),
                        );
                        new_ordinal += 1;
                    }
                }
            }
            new_entry = Some(SegmentEntry {
                meta: Arc::new(meta),
                suppressed: Arc::new(RoaringBitmap::new()),
            });
        }

        let merged_ids: HashSet<u64> = inputs.iter().map(|e| e.meta.id).collect();
        state.segments.retain(|e| !merged_ids.contains(&e.meta.id));
        if let Some(entry) = new_entry {
            state.segments.push(entry);
        }
        state.segments.sort_by_key(|e| e.meta.id);
        state.next_segment_id = new_id + 1;
        self.commit_manifest(state)?;

        // Old files are only removed once the manifest no longer references
        // them; a crash before this point just leaves orphans for `open`.
        for entry in &inputs {
            self.store.delete(&entry.meta.key)?;
        }
        tracing::info!(
            table = %self.table,
            partition = self.id,
            merged = inputs.len(),
            into = new_id,
            "compacted segments"
        );
        Ok(Some(new_id))
    }

    fn set_schema(&self, schema: Arc<TableSchema>) -> Result<()> {
        let mut state = self.write_state()?;
        state.schema = schema.clone();
        state.memtable.set_schema(schema);
        self.commit_manifest(&state)
    }
}

/// Take rows by ordinal, preserving order.
fn take_rows(batch: &RecordBatch, ordinals: &[u32]) -> Result<RecordBatch> {
    let indices = UInt32Array::from(ordinals.to_vec());
    arrow::compute::take_record_batch(batch, &indices).map_err(Into::into)
}

/// Storage for one table: routing, write serialization and read snapshots.
#[derive(Debug)]
pub struct TableStore {
    tenant: TenantId,
    database: DatabaseName,
    schema: RwLock<Arc<TableSchema>>,
    store: Arc<dyn ObjectStore>,
    partitions: Vec<Partition>,
    write_lock: Mutex<()>,
    config: StorageConfig,
}

impl TableStore {
    pub fn open(
        store: Arc<dyn ObjectStore>,
        tenant: &TenantId,
        database: &DatabaseName,
        schema: Arc<TableSchema>,
        config: StorageConfig,
    ) -> Result<Self> {
        schema.validate()?;
        let mut partitions = Vec::with_capacity(schema.partitions as usize);
        for id in 0..schema.partitions {
            partitions.push(Partition::open(
                store.clone(),
                tenant,
                database,
                schema.clone(),
                id,
                config.clone(),
            )?);
        }
        Ok(Self {
            tenant: tenant.clone(),
            database: database.clone(),
            schema: RwLock::new(schema),
            store,
            partitions,
            write_lock: Mutex::new(()),
            config,
        })
    }

    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.read().expect("schema lock poisoned").clone()
    }

    pub fn name(&self) -> TableName {
        self.schema().name.clone()
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Read views of every partition, for the executor.
    pub fn snapshots(&self) -> Result<Vec<Arc<PartitionSnapshot>>> {
        self.partitions.iter().map(Partition::snapshot).collect()
    }

    /// Visible row count, computed from metadata only.
    pub fn row_count(&self) -> Result<u64> {
        Ok(self.snapshots()?.iter().map(|s| s.visible_rows()).sum())
    }

    /// Bytes held in segments.
    pub fn stored_bytes(&self) -> Result<u64> {
        Ok(self
            .snapshots()?
            .iter()
            .flat_map(|s| s.segments.clone())
            .map(|e| e.meta.bytes)
            .sum())
    }

    fn guard(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.write_lock
            .lock()
            .map_err(|_| AdbError::internal("table write lock poisoned"))
    }

    fn check_batch_schema(&self, schema: &TableSchema, batch: &RecordBatch) -> Result<()> {
        let expected = schema.arrow_schema();
        if batch.schema().fields() != expected.fields() {
            return Err(AdbError::InvalidSchema(format!(
                "batch does not match table {}: expected {:?}, got {:?}",
                schema.name,
                expected
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>(),
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )));
        }
        Ok(())
    }

    pub fn insert(&self, batch: RecordBatch) -> Result<WriteOutcome> {
        self.write(batch, false)
    }

    pub fn upsert(&self, batch: RecordBatch) -> Result<WriteOutcome> {
        self.write(batch, true)
    }

    fn write(&self, batch: RecordBatch, upsert: bool) -> Result<WriteOutcome> {
        let schema = self.schema();
        self.check_batch_schema(&schema, &batch)?;
        if batch.num_rows() == 0 {
            return Ok(WriteOutcome::default());
        }
        if upsert && schema.is_append_only() {
            return Err(AdbError::bad_request(format!(
                "table {} has no primary key, so upsert is not available; use insert",
                schema.name
            )));
        }

        let _guard = self.guard()?;
        let mut outcome = WriteOutcome {
            rows: batch.num_rows(),
            segments_flushed: 0,
        };

        if schema.is_append_only() {
            // Spread the batch across partitions so scans can parallelize, using
            // zero-copy slices.
            let total = batch.num_rows();
            let per = total.div_ceil(self.partitions.len());
            for (idx, partition) in self.partitions.iter().enumerate() {
                let offset = idx * per;
                if offset >= total {
                    break;
                }
                let len = per.min(total - offset);
                outcome.segments_flushed +=
                    partition.append_rows(batch.slice(offset, len), false)?;
            }
            return Ok(outcome);
        }

        let pk = schema.pk_indices();
        let mut keys = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            keys.push(rows::key_at(&batch, &pk, row)?);
        }

        let mut seen: HashSet<&RowKey> = HashSet::with_capacity(keys.len());
        for (row, key) in keys.iter().enumerate() {
            if !seen.insert(key) && !upsert {
                return Err(AdbError::bad_request(format!(
                    "row {row} repeats primary key ({}) already present in this batch",
                    format_key(key)
                )));
            }
        }

        let mut groups: Vec<Vec<u32>> = vec![Vec::new(); self.partitions.len()];
        for (row, key) in keys.iter().enumerate() {
            let target = (hash_key(key) % self.partitions.len() as u64) as usize;
            groups[target].push(row as u32);
        }

        // Uniqueness is checked across every partition before anything is
        // logged, so a duplicate cannot leave a half-applied write behind.
        if !upsert {
            for (idx, group) in groups.iter().enumerate() {
                if group.is_empty() {
                    continue;
                }
                let state = self.partitions[idx].read_state()?;
                for &row in group {
                    let key = &keys[row as usize];
                    if state.key_index.contains(key) {
                        return Err(AdbError::already_exists(
                            "row",
                            format!("{} ({})", schema.name, format_key(key)),
                        ));
                    }
                }
            }
        }

        for (idx, group) in groups.iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let sub = if group.len() == batch.num_rows() {
                batch.clone()
            } else {
                take_rows(&batch, group)?
            };
            outcome.segments_flushed += self.partitions[idx].append_rows(sub, upsert)?;
        }
        Ok(outcome)
    }

    /// Tombstone rows by primary key; returns how many keys existed.
    pub fn delete(&self, keys: &[RowKey]) -> Result<usize> {
        let schema = self.schema();
        if schema.is_append_only() {
            return Err(AdbError::bad_request(format!(
                "table {} has no primary key, so rows cannot be deleted individually",
                schema.name
            )));
        }
        for key in keys {
            if key.len() != schema.primary_key.len() {
                return Err(AdbError::bad_request(format!(
                    "primary key of {} has {} column(s), got {}",
                    schema.name,
                    schema.primary_key.len(),
                    key.len()
                )));
            }
        }
        let _guard = self.guard()?;
        let mut groups: Vec<Vec<RowKey>> = vec![Vec::new(); self.partitions.len()];
        for key in keys {
            let target = (hash_key(key) % self.partitions.len() as u64) as usize;
            groups[target].push(key.clone());
        }
        let mut deleted = 0;
        for (idx, group) in groups.iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            deleted += self.partitions[idx].delete_keys(group)?;
        }
        Ok(deleted)
    }

    /// Point lookup by primary key. Returns found rows in request order.
    pub fn get(&self, keys: &[RowKey]) -> Result<RecordBatch> {
        let schema = self.schema();
        let columns = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        if schema.is_append_only() {
            return Err(AdbError::bad_request(format!(
                "table {} has no primary key, so point lookups are not available; use a query",
                schema.name
            )));
        }

        // request index -> (partition, locator)
        let mut located: Vec<(usize, usize, Locator)> = Vec::new();
        for (request, key) in keys.iter().enumerate() {
            if key.len() != schema.primary_key.len() {
                return Err(AdbError::bad_request(format!(
                    "primary key of {} has {} column(s), got {}",
                    schema.name,
                    schema.primary_key.len(),
                    key.len()
                )));
            }
            let idx = (hash_key(key) % self.partitions.len() as u64) as usize;
            let state = self.partitions[idx].read_state()?;
            if let Some(locator) = state.key_index.get(key) {
                located.push((request, idx, locator));
            }
        }
        if located.is_empty() {
            return rows::empty_batch(&schema, &columns);
        }

        // Group by source so each segment is opened at most once.
        let mut by_source: HashMap<(usize, Source), Vec<(usize, u32)>> = HashMap::new();
        for (request, partition, locator) in located {
            by_source
                .entry((partition, locator.source))
                .or_default()
                .push((request, locator.ordinal));
        }

        let mut pieces = Vec::new();
        let mut request_order = Vec::new();
        for ((partition, source), mut wanted) in by_source {
            wanted.sort_by_key(|(_, ordinal)| *ordinal);
            let ordinals: Vec<u32> = wanted.iter().map(|(_, o)| *o).collect();
            let batch = match source {
                Source::Memtable => {
                    let state = self.partitions[partition].read_state()?;
                    let Some(all) = state.memtable.concat()? else {
                        continue;
                    };
                    let all = segment::project_with_backfill(&all, &schema, &columns)?;
                    take_rows(&all, &ordinals)?
                }
                Source::Segment(id) => {
                    let state = self.partitions[partition].read_state()?;
                    let Some(entry) = state.segments.iter().find(|e| e.meta.id == id).cloned()
                    else {
                        continue;
                    };
                    drop(state);
                    let all = segment::read_segment(
                        self.store.as_ref(),
                        &entry.meta,
                        &schema,
                        Some(&columns),
                    )?;
                    take_rows(&all, &ordinals)?
                }
            };
            request_order.extend(wanted.iter().map(|(request, _)| *request));
            pieces.push(batch);
        }
        if pieces.is_empty() {
            return rows::empty_batch(&schema, &columns);
        }

        let arrow_schema = pieces[0].schema();
        let combined = if pieces.len() == 1 {
            pieces.into_iter().next().expect("length checked")
        } else {
            arrow::compute::concat_batches(&arrow_schema, &pieces)?
        };
        // Restore the caller's key order.
        let mut permutation: Vec<(usize, u32)> = request_order
            .into_iter()
            .enumerate()
            .map(|(position, request)| (request, position as u32))
            .collect();
        permutation.sort_by_key(|(request, _)| *request);
        let ordinals: Vec<u32> = permutation
            .into_iter()
            .map(|(_, position)| position)
            .collect();
        take_rows(&combined, &ordinals)
    }

    /// Flush every partition's memtable and checkpoint.
    pub fn flush(&self) -> Result<usize> {
        let _guard = self.guard()?;
        let mut created = 0;
        for partition in &self.partitions {
            if partition.flush()?.is_some() {
                created += 1;
            }
        }
        Ok(created)
    }

    /// Run compaction where the policy calls for it.
    pub fn compact(&self) -> Result<usize> {
        let _guard = self.guard()?;
        let mut merged = 0;
        for partition in &self.partitions {
            if partition.compact()?.is_some() {
                merged += 1;
            }
        }
        Ok(merged)
    }

    /// Adopt an evolved schema. The memtable is flushed first so no batch
    /// straddles two schema versions.
    pub fn set_schema(&self, schema: Arc<TableSchema>) -> Result<()> {
        let current = self.schema();
        current.check_evolution(&schema)?;
        let _guard = self.guard()?;
        for partition in &self.partitions {
            partition.flush()?;
        }
        for partition in &self.partitions {
            partition.set_schema(schema.clone())?;
        }
        *self
            .schema
            .write()
            .map_err(|_| AdbError::internal("schema lock poisoned"))? = schema;
        Ok(())
    }

    /// Delete every file belonging to this table. Irreversible.
    pub fn destroy(self) -> Result<()> {
        let schema = self.schema();
        let prefix = paths::table_prefix(&self.tenant, &self.database, &schema.name);
        drop(self.partitions);
        self.store.delete_prefix(&prefix)
    }
}

fn format_key(key: &RowKey) -> String {
    key.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}
