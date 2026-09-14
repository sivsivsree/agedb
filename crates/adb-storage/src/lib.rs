//! Storage layer: the write-ahead log, memtables, immutable Parquet segments,
//! per-partition manifests and compaction.
//!
//! Two design rules run through this crate:
//!
//! 1. **The log is the only way state changes.** Every mutation is appended to a
//!    [`wal::LogStore`] and then applied to in-memory state by a deterministic
//!    `apply`. Segments and manifests are checkpoints of that log, never an
//!    independent source of truth. Swapping the `LogStore` for a Raft-backed one
//!    is therefore the whole job of making this HA.
//! 2. **The layer is synchronous.** Callers wrap it in `spawn_blocking`. This
//!    keeps `RwLock`-guarded partition state honest instead of scattering
//!    `.await` points across critical sections.

pub mod b64;
pub mod bloom;
pub mod compaction;
pub mod hash;
pub mod keyindex;
pub mod manifest;
pub mod memtable;
pub mod object_store;
pub mod paths;
pub mod rows;
pub mod segment;
pub mod table;
pub mod wal;

pub use manifest::Manifest;
pub use memtable::Memtable;
pub use object_store::{LocalFsStore, ObjectStore};
pub use rows::{decode_ipc, encode_ipc, RowBatchBuilder};
pub use segment::{ColumnStats, SegmentMeta};
pub use table::{PartitionSnapshot, SegmentEntry, StorageConfig, TableStore, WriteOutcome};
pub use wal::{FileLogStore, LogEntry, LogStore, Mutation};
