//! Write-ahead log.
//!
//! # Why the log is shaped this way
//!
//! The WAL is the replication unit. Everything that changes state is appended
//! here first, then applied by a *deterministic* `apply` (see
//! [`table::Partition`](crate::table)). That means:
//!
//! * No clocks, UUIDs or defaults may be resolved during apply. They are
//!   resolved when the record is built and baked into the payload. Two nodes
//!   replaying the same log must reach byte-identical state.
//! * Applying a record twice must be equivalent to applying it once for
//!   everything already covered by `applied_lsn` in the manifest.
//!
//! Given that, adding HA later is a matter of implementing [`LogStore`] on top of
//! a Raft log and driving apply from committed entries; nothing above this trait
//! changes.
//!
//! # Frame format
//!
//! ```text
//! file: "ADBWAL01" then a sequence of records
//! record: [u32 payload_len LE][u32 crc32(payload) LE][payload]
//! payload: bincode(LogEntry { lsn, mutation })
//! ```
//!
//! A short or CRC-failing frame at the *end* of the file is a torn write from a
//! crash: it is truncated away and the LSN is reused. The same failure anywhere
//! earlier is real corruption and is reported, never silently skipped.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use adb_core::{AdbError, DdlMutation, Result, Value};
use serde::{Deserialize, Serialize};

pub const WAL_MAGIC: &[u8; 8] = b"ADBWAL01";
const HEADER_LEN: u64 = 8;
const FRAME_HEADER_LEN: u64 = 8;
/// Refuse to allocate for an implausible frame length from a corrupt file.
const MAX_RECORD_BYTES: u32 = 1 << 30;

/// A logged state change.
///
/// Data-bearing variants carry Arrow IPC bytes rather than rows: it keeps bulk
/// ingest cheap (one encode, one write) and makes the payload self-describing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mutation {
    /// Catalog change, carried as a JSON string. Only ever appears in the
    /// system log.
    ///
    /// Why JSON inside a bincode frame: `TableSchema` uses
    /// `skip_serializing_if`, and bincode is not self-describing, so a skipped
    /// field is simply absent on read, so the record becomes undecodable. That
    /// failure only shows up on restart, which is the worst possible time to
    /// find it. Schemas are small and DDL is rare, so JSON costs nothing here,
    /// while row data stays in the compact Arrow IPC form.
    Ddl { json: String },
    /// Append rows. On a table with a primary key, duplicate keys are rejected
    /// before the record is built.
    Insert { ipc: Vec<u8> },
    /// Insert-or-replace by primary key.
    Upsert { ipc: Vec<u8> },
    /// Tombstone rows by primary key.
    Delete { keys: Vec<Vec<Value>> },
}

impl Mutation {
    /// Wrap a catalog change for the log.
    pub fn ddl(mutation: &DdlMutation) -> Result<Self> {
        let json = serde_json::to_string(mutation)
            .map_err(|e| AdbError::internal(format!("DDL encode failed: {e}")))?;
        Ok(Self::Ddl { json })
    }

    /// Unwrap a logged catalog change.
    pub fn as_ddl(&self) -> Result<DdlMutation> {
        match self {
            Self::Ddl { json } => serde_json::from_str(json)
                .map_err(|e| AdbError::Corruption(format!("undecodable DDL record: {e}: {json}"))),
            other => Err(AdbError::Internal(format!(
                "expected a DDL record, found {}",
                other.kind()
            ))),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Ddl { .. } => "ddl",
            Self::Insert { .. } => "insert",
            Self::Upsert { .. } => "upsert",
            Self::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub lsn: u64,
    pub mutation: Mutation,
}

pub trait LogStore: Send + Sync + fmt::Debug {
    /// Durably append and return the assigned LSN.
    fn append(&self, mutation: &Mutation) -> Result<u64>;
    /// Entries with `lsn >= from`, in log order.
    fn entries_from(&self, from: u64) -> Result<Vec<LogEntry>>;
    /// Drop entries with `lsn <= up_to_inclusive`; called after a checkpoint.
    fn truncate_prefix(&self, up_to_inclusive: u64) -> Result<()>;
    /// Highest assigned LSN (0 when empty).
    fn last_lsn(&self) -> u64;
    fn size_bytes(&self) -> Result<u64>;
}

/// Outcome of opening a log: the store plus everything it still holds.
#[derive(Debug)]
pub struct RecoveredLog {
    pub store: FileLogStore,
    pub entries: Vec<LogEntry>,
    /// Bytes discarded as a torn tail. Non-zero means we crashed mid-append.
    pub truncated_bytes: u64,
}

#[derive(Debug)]
pub struct FileLogStore {
    path: PathBuf,
    file: Mutex<File>,
    next_lsn: AtomicU64,
    last_lsn: AtomicU64,
    sync: bool,
}

impl FileLogStore {
    /// Open (creating if absent), recovering any torn tail.
    ///
    /// `sync` = fsync on every append. Turning it off trades durability for
    /// ingest throughput and is only for benchmarks.
    pub fn open(path: impl AsRef<Path>, sync: bool) -> Result<RecoveredLog> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Never truncate: an existing log is the state we are recovering.
            .truncate(false)
            .open(&path)?;

        if !existed || file.metadata()?.len() == 0 {
            file.write_all(WAL_MAGIC)?;
            file.sync_all()?;
        }

        let (entries, good_end) = Self::scan(&mut file, &path)?;
        let file_len = file.metadata()?.len();
        let truncated_bytes = file_len.saturating_sub(good_end);
        if truncated_bytes > 0 {
            tracing::warn!(
                path = %path.display(),
                bytes = truncated_bytes,
                "discarding torn WAL tail from an interrupted append"
            );
            file.set_len(good_end)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;

        let last = entries.last().map(|e| e.lsn).unwrap_or(0);
        Ok(RecoveredLog {
            store: Self {
                path,
                file: Mutex::new(file),
                next_lsn: AtomicU64::new(last + 1),
                last_lsn: AtomicU64::new(last),
                sync,
            },
            entries,
            truncated_bytes,
        })
    }

    /// Ensure the next appended LSN is at least `lsn + 1`.
    ///
    /// Needed after loading a manifest whose `applied_lsn` is ahead of the
    /// (already truncated) log.
    pub fn advance_to(&self, lsn: u64) {
        self.next_lsn.fetch_max(lsn + 1, Ordering::SeqCst);
        self.last_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read every intact record, returning them plus the offset at which the
    /// first unusable byte begins.
    fn scan(file: &mut File, path: &Path) -> Result<(Vec<LogEntry>, u64)> {
        let len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 8];
        if len < HEADER_LEN {
            return Err(AdbError::Corruption(format!(
                "{}: WAL is shorter than its header",
                path.display()
            )));
        }
        file.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(AdbError::Corruption(format!(
                "{}: not an AgenticDB WAL (bad magic)",
                path.display()
            )));
        }

        let mut entries = Vec::new();
        let mut offset = HEADER_LEN;
        loop {
            if offset == len {
                return Ok((entries, offset));
            }
            let remaining = len - offset;
            if remaining < FRAME_HEADER_LEN {
                return Ok((entries, offset)); // torn tail
            }
            let mut header = [0u8; 8];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut header)?;
            let payload_len = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
            let crc = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
            if payload_len == 0 || payload_len > MAX_RECORD_BYTES {
                return Ok((entries, offset));
            }
            let frame_end = offset + FRAME_HEADER_LEN + payload_len as u64;
            if frame_end > len {
                return Ok((entries, offset)); // torn tail
            }
            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)?;

            let is_last_frame = frame_end == len;
            if crc32fast::hash(&payload) != crc {
                if is_last_frame {
                    return Ok((entries, offset)); // torn tail
                }
                return Err(AdbError::Corruption(format!(
                    "{}: CRC mismatch at offset {offset} with {} bytes of records after it",
                    path.display(),
                    len - frame_end
                )));
            }
            match bincode::deserialize::<LogEntry>(&payload) {
                Ok(entry) => entries.push(entry),
                Err(e) if is_last_frame => {
                    tracing::warn!(path = %path.display(), error = %e, "undecodable final WAL record");
                    return Ok((entries, offset));
                }
                Err(e) => {
                    return Err(AdbError::Corruption(format!(
                        "{}: undecodable record at offset {offset}: {e}",
                        path.display()
                    )))
                }
            }
            offset = frame_end;
        }
    }

    fn read_all(&self) -> Result<Vec<LogEntry>> {
        let mut file = File::open(&self.path)?;
        let (entries, _) = Self::scan(&mut file, &self.path)?;
        Ok(entries)
    }
}

impl LogStore for FileLogStore {
    fn append(&self, mutation: &Mutation) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let entry = LogEntry {
            lsn,
            mutation: mutation.clone(),
        };
        let payload = bincode::serialize(&entry)
            .map_err(|e| AdbError::internal(format!("WAL encode failed: {e}")))?;
        if payload.len() as u32 > MAX_RECORD_BYTES {
            return Err(AdbError::bad_request(format!(
                "WAL record of {} bytes exceeds the {MAX_RECORD_BYTES} byte limit",
                payload.len()
            )));
        }
        let mut frame = Vec::with_capacity(payload.len() + FRAME_HEADER_LEN as usize);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);

        let mut file = self
            .file
            .lock()
            .map_err(|_| AdbError::internal("WAL mutex poisoned"))?;
        // One write_all per record keeps a crash from interleaving two frames.
        file.write_all(&frame)?;
        if self.sync {
            file.sync_data()?;
        }
        self.last_lsn.fetch_max(lsn, Ordering::SeqCst);
        Ok(lsn)
    }

    fn entries_from(&self, from: u64) -> Result<Vec<LogEntry>> {
        Ok(self
            .read_all()?
            .into_iter()
            .filter(|e| e.lsn >= from)
            .collect())
    }

    fn truncate_prefix(&self, up_to_inclusive: u64) -> Result<()> {
        let survivors: Vec<LogEntry> = self
            .read_all()?
            .into_iter()
            .filter(|e| e.lsn > up_to_inclusive)
            .collect();

        let mut buf = Vec::with_capacity(HEADER_LEN as usize);
        buf.extend_from_slice(WAL_MAGIC);
        for entry in &survivors {
            let payload = bincode::serialize(entry)
                .map_err(|e| AdbError::internal(format!("WAL encode failed: {e}")))?;
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
            buf.extend_from_slice(&payload);
        }

        let mut file = self
            .file
            .lock()
            .map_err(|_| AdbError::internal("WAL mutex poisoned"))?;
        let tmp = self.path.with_extension("log.compacting");
        {
            let mut out = File::create(&tmp)?;
            out.write_all(&buf)?;
            out.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        // Re-point the writer at the replacement file.
        let mut reopened = OpenOptions::new().read(true).write(true).open(&self.path)?;
        reopened.seek(SeekFrom::End(0))?;
        *file = reopened;
        Ok(())
    }

    fn last_lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::SeqCst)
    }

    fn size_bytes(&self) -> Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn delete(n: i64) -> Mutation {
        Mutation::Delete {
            keys: vec![vec![Value::Int(n)]],
        }
    }

    fn key_of(entry: &LogEntry) -> i64 {
        match &entry.mutation {
            Mutation::Delete { keys } => keys[0][0].as_i64().unwrap(),
            other => panic!("unexpected mutation {}", other.kind()),
        }
    }

    #[test]
    fn append_then_replay_preserves_order_and_lsns() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal/current.log");
        let log = FileLogStore::open(&path, true).unwrap().store;
        for n in 1..=3 {
            assert_eq!(log.append(&delete(n)).unwrap(), n as u64);
        }
        let entries = log.entries_from(0).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries.iter().map(key_of).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(log.last_lsn(), 3);

        // Reopening continues where we left off.
        let recovered = FileLogStore::open(&path, true).unwrap();
        assert_eq!(recovered.entries.len(), 3);
        assert_eq!(recovered.truncated_bytes, 0);
        assert_eq!(recovered.store.append(&delete(4)).unwrap(), 4);
    }

    #[test]
    fn entries_from_filters_by_lsn() {
        let dir = TempDir::new().unwrap();
        let log = FileLogStore::open(dir.path().join("w.log"), true)
            .unwrap()
            .store;
        for n in 1..=5 {
            log.append(&delete(n)).unwrap();
        }
        let tail = log.entries_from(4).unwrap();
        assert_eq!(tail.iter().map(|e| e.lsn).collect::<Vec<_>>(), vec![4, 5]);
    }

    #[test]
    fn torn_tail_is_truncated_and_the_lsn_is_reused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        {
            let log = FileLogStore::open(&path, true).unwrap().store;
            for n in 1..=3 {
                log.append(&delete(n)).unwrap();
            }
        }
        // Simulate a crash partway through appending record 3.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 5).unwrap();
        drop(file);

        let recovered = FileLogStore::open(&path, true).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        assert!(recovered.truncated_bytes > 0);
        // LSN 3 was never acknowledged, so it is handed out again.
        assert_eq!(recovered.store.append(&delete(30)).unwrap(), 3);
        assert_eq!(recovered.store.entries_from(0).unwrap().len(), 3);
    }

    #[test]
    fn garbage_appended_after_a_crash_is_discarded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        {
            let log = FileLogStore::open(&path, true).unwrap().store;
            log.append(&delete(1)).unwrap();
        }
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xAA; 3]).unwrap(); // shorter than a frame header
        drop(file);
        let recovered = FileLogStore::open(&path, true).unwrap();
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.truncated_bytes, 3);
    }

    #[test]
    fn corruption_in_the_middle_is_reported_not_skipped() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        {
            let log = FileLogStore::open(&path, true).unwrap().store;
            for n in 1..=3 {
                log.append(&delete(n)).unwrap();
            }
        }
        // Flip a payload byte in the first record.
        let mut bytes = std::fs::read(&path).unwrap();
        let payload_start = (HEADER_LEN + FRAME_HEADER_LEN) as usize;
        bytes[payload_start] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = FileLogStore::open(&path, true).unwrap_err();
        assert_eq!(err.code(), "corruption");
        assert!(err.to_string().contains("CRC mismatch"), "{err}");
    }

    #[test]
    fn a_non_wal_file_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        std::fs::write(&path, b"this is not a WAL at all").unwrap();
        assert_eq!(
            FileLogStore::open(&path, true).unwrap_err().code(),
            "corruption"
        );
    }

    #[test]
    fn truncate_prefix_drops_checkpointed_records_and_keeps_appending() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        let log = FileLogStore::open(&path, true).unwrap().store;
        for n in 1..=5 {
            log.append(&delete(n)).unwrap();
        }
        let before = log.size_bytes().unwrap();
        log.truncate_prefix(3).unwrap();
        assert!(log.size_bytes().unwrap() < before);
        assert_eq!(
            log.entries_from(0)
                .unwrap()
                .iter()
                .map(|e| e.lsn)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );

        log.append(&delete(6)).unwrap();
        assert_eq!(
            log.entries_from(0)
                .unwrap()
                .iter()
                .map(|e| e.lsn)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        // And the survivors are still readable after a reopen.
        let recovered = FileLogStore::open(&path, true).unwrap();
        assert_eq!(recovered.entries.len(), 3);
        assert_eq!(recovered.truncated_bytes, 0);
    }

    #[test]
    fn ddl_records_round_trip_through_the_log() {
        // Regression guard for a real bug: `TableSchema` uses
        // `skip_serializing_if`, so encoding it with bincode produced records
        // that failed to decode on restart, silently losing every table.
        use adb_core::{ColumnSchema, DataType, DatabaseName, TableName, TableSchema, TenantId};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("system.log");
        let schema = TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64).required(),
                ColumnSchema::new("note", DataType::Utf8).described("free text"),
            ],
        )
        .with_primary_key(vec!["id".to_string()]);
        let ddl = DdlMutation::CreateTable {
            tenant: TenantId::new("t1").unwrap(),
            database: DatabaseName::new("crm").unwrap(),
            schema: schema.clone(),
        };

        {
            let log = FileLogStore::open(&path, true).unwrap().store;
            log.append(&Mutation::ddl(&ddl).unwrap()).unwrap();
        }
        let recovered = FileLogStore::open(&path, true).unwrap();
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.entries[0].mutation.as_ddl().unwrap(), ddl);
    }

    #[test]
    fn advance_to_skips_lsns_already_checkpointed() {
        let dir = TempDir::new().unwrap();
        let log = FileLogStore::open(dir.path().join("w.log"), true)
            .unwrap()
            .store;
        log.advance_to(100);
        assert_eq!(log.append(&delete(1)).unwrap(), 101);
    }

    #[test]
    fn replaying_the_same_log_twice_is_byte_identical() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("w.log");
        let log = FileLogStore::open(&path, true).unwrap().store;
        for n in 1..=4 {
            log.append(&delete(n)).unwrap();
        }
        let first = bincode::serialize(&log.entries_from(0).unwrap()).unwrap();
        let second = bincode::serialize(&FileLogStore::open(&path, true).unwrap().entries).unwrap();
        assert_eq!(first, second);
    }
}
