//! Per-partition manifest: the checkpoint that makes the WAL truncatable.
//!
//! It records which segments exist, which of their rows are suppressed by later
//! updates/deletes, and the LSN up to which all of that is already materialized.
//! Recovery is therefore: load manifest, replay WAL entries above `applied_lsn`.
//!
//! Commits are `put_atomic` (write temp, rename), so a crash mid-commit leaves
//! the previous manifest intact and at worst orphans a segment file, which
//! `TableStore::open` deletes.

use std::collections::BTreeMap;

use adb_core::{AdbError, Result};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::b64;
use crate::object_store::ObjectStore;
use crate::segment::SegmentMeta;

/// On-disk manifest version. Bump when the layout changes incompatibly.
pub const MANIFEST_FORMAT: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub partition: u32,
    /// Schema version the partition was last checkpointed with.
    pub schema_version: u32,
    /// Every mutation with `lsn <= applied_lsn` is reflected in `segments` and
    /// `suppressed`; anything above it must be replayed from the WAL.
    pub applied_lsn: u64,
    pub next_segment_id: u64,
    pub segments: Vec<SegmentMeta>,
    /// segment id -> base64 Roaring bitmap of suppressed row ordinals.
    #[serde(default)]
    pub suppressed: BTreeMap<u64, String>,
}

impl Manifest {
    pub fn new(partition: u32, schema_version: u32) -> Self {
        Self {
            format: MANIFEST_FORMAT,
            partition,
            schema_version,
            applied_lsn: 0,
            next_segment_id: 1,
            segments: Vec::new(),
            suppressed: BTreeMap::new(),
        }
    }

    /// Load a manifest, or `None` if the partition has never checkpointed.
    pub fn load(store: &dyn ObjectStore, key: &str) -> Result<Option<Self>> {
        if !store.exists(key)? {
            return Ok(None);
        }
        let bytes = store.get(key)?;
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|e| AdbError::Corruption(format!("{key}: manifest is not valid JSON: {e}")))?;
        if manifest.format != MANIFEST_FORMAT {
            return Err(AdbError::Corruption(format!(
                "{key}: manifest format {} is not supported (expected {MANIFEST_FORMAT})",
                manifest.format
            )));
        }
        Ok(Some(manifest))
    }

    pub fn save(&self, store: &dyn ObjectStore, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(self).map_err(AdbError::from)?;
        store.put_atomic(key, &bytes)
    }

    pub fn suppressed_bitmap(&self, segment_id: u64) -> Result<RoaringBitmap> {
        match self.suppressed.get(&segment_id) {
            None => Ok(RoaringBitmap::new()),
            Some(encoded) => {
                let raw = b64::decode(encoded)?;
                RoaringBitmap::deserialize_from(&raw[..]).map_err(|e| {
                    AdbError::Corruption(format!(
                        "segment {segment_id}: suppressed bitmap is unreadable: {e}"
                    ))
                })
            }
        }
    }

    pub fn set_suppressed(&mut self, segment_id: u64, bitmap: &RoaringBitmap) -> Result<()> {
        if bitmap.is_empty() {
            self.suppressed.remove(&segment_id);
            return Ok(());
        }
        let mut raw = Vec::with_capacity(bitmap.serialized_size());
        bitmap
            .serialize_into(&mut raw)
            .map_err(|e| AdbError::internal(format!("bitmap encode: {e}")))?;
        self.suppressed.insert(segment_id, b64::encode(&raw));
        Ok(())
    }

    pub fn total_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.row_count).sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    /// Segment object keys, used to spot orphans left by a crash.
    pub fn segment_keys(&self) -> Vec<&str> {
        self.segments.iter().map(|s| s.key.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::LocalFsStore;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn segment(id: u64, rows: u64) -> SegmentMeta {
        SegmentMeta {
            id,
            key: format!("t/p0/segments/seg-{id:08}.parquet"),
            row_count: rows,
            bytes: rows * 16,
            schema_version: 1,
            columns: BTreeMap::new(),
        }
    }

    #[test]
    fn round_trips_through_the_object_store() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let key = "t/p0/manifest.json";
        assert!(Manifest::load(&store, key).unwrap().is_none());

        let mut manifest = Manifest::new(0, 3);
        manifest.applied_lsn = 42;
        manifest.next_segment_id = 5;
        manifest.segments.push(segment(4, 100));
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(99);
        manifest.set_suppressed(4, &bitmap).unwrap();
        manifest.save(&store, key).unwrap();

        let loaded = Manifest::load(&store, key).unwrap().unwrap();
        assert_eq!(loaded.applied_lsn, 42);
        assert_eq!(loaded.schema_version, 3);
        assert_eq!(loaded.next_segment_id, 5);
        assert_eq!(loaded.total_rows(), 100);
        assert_eq!(loaded.suppressed_bitmap(4).unwrap(), bitmap);
        assert!(loaded.suppressed_bitmap(9).unwrap().is_empty());
    }

    #[test]
    fn empty_bitmaps_are_not_stored() {
        let mut manifest = Manifest::new(0, 1);
        manifest.set_suppressed(1, &RoaringBitmap::new()).unwrap();
        assert!(manifest.suppressed.is_empty());
    }

    #[test]
    fn commits_are_atomic_replacements() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        let key = "t/p0/manifest.json";
        Manifest::new(0, 1).save(&store, key).unwrap();
        let mut second = Manifest::new(0, 1);
        second.applied_lsn = 7;
        second.save(&store, key).unwrap();
        assert_eq!(Manifest::load(&store, key).unwrap().unwrap().applied_lsn, 7);
        // No leftover temp files that a later `list` would mistake for data.
        assert_eq!(store.list("t/p0").unwrap(), vec![key.to_string()]);
    }

    #[test]
    fn unknown_format_and_garbage_are_refused() {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        store.put("m.json", b"{not json").unwrap();
        assert_eq!(
            Manifest::load(&store, "m.json").unwrap_err().code(),
            "corruption"
        );

        let mut future = Manifest::new(0, 1);
        future.format = 99;
        future.save(&store, "f.json").unwrap();
        let err = Manifest::load(&store, "f.json").unwrap_err();
        assert!(err.to_string().contains("format 99"), "{err}");
    }

    #[test]
    fn corrupt_bitmaps_are_reported() {
        let mut manifest = Manifest::new(0, 1);
        manifest.suppressed.insert(1, "AAAA".to_string());
        assert_eq!(
            manifest.suppressed_bitmap(1).unwrap_err().code(),
            "corruption"
        );
    }
}
