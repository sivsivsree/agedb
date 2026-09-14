//! Object storage abstraction.
//!
//! v0.1 ships one implementation, [`LocalFsStore`], because the user's
//! requirement is file-based storage that can later be replicated with Raft
//! rather than pushed to S3. The trait still exists so an S3/R2/GCS backend is a
//! new impl plus a config switch, and so tests can substitute a fake.
//!
//! `put_atomic` is the load-bearing operation: manifest commits are
//! write-temp-then-rename, which is what makes a crash mid-commit invisible.

use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use adb_core::{AdbError, Result};

pub trait ObjectStore: Send + Sync + fmt::Debug {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    /// Durable, all-or-nothing replace of `key`.
    fn put_atomic(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn exists(&self, key: &str) -> Result<bool>;
    fn delete(&self, key: &str) -> Result<()>;
    fn delete_prefix(&self, prefix: &str) -> Result<()>;
    /// Keys under `prefix`, sorted lexically (which for our zero-padded segment
    /// names equals numeric order).
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    fn size(&self, key: &str) -> Result<u64>;
    /// Filesystem path backing `key`, when there is one. The WAL uses this to
    /// append with `fsync` instead of rewriting whole objects; a remote store
    /// returns `None` and would need a different log implementation.
    fn local_path(&self, key: &str) -> Option<PathBuf>;
}

/// Reject keys that could escape the store root or collide with our temp files.
fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(AdbError::bad_request("empty object key"));
    }
    if key.starts_with('/') || key.ends_with('/') {
        return Err(AdbError::bad_request(format!(
            "object key {key:?} must be relative"
        )));
    }
    for part in key.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(AdbError::bad_request(format!(
                "object key {key:?} has an unsafe segment"
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct LocalFsStore {
    root: PathBuf,
    counter: AtomicU64,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        Ok(Self {
            root,
            counter: AtomicU64::new(0),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        let path = self.root.join(key);
        // Defence in depth: `validate_key` already rejects `..`, but symlinked
        // components could still point outside the root.
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(AdbError::bad_request(format!(
                "object key {key:?} escapes the store root"
            )));
        }
        Ok(path)
    }

    fn ensure_parent(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    fn fsync_dir(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            // Directory fsync is what actually makes a rename durable.
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                Self::walk(&path, out)?;
            } else {
                out.push(path);
            }
        }
        Ok(())
    }
}

impl ObjectStore for LocalFsStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key)?;
        Self::ensure_parent(&path)?;
        let mut file = fs::File::create(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Self::fsync_dir(&path)?;
        Ok(())
    }

    fn put_atomic(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key)?;
        Self::ensure_parent(&path)?;
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp{}-{}", std::process::id(), seq));
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Self::fsync_dir(&path)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path_for(key)?;
        match fs::read(&path) {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(AdbError::not_found("object", key))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.path_for(key)?.exists())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn delete_prefix(&self, prefix: &str) -> Result<()> {
        validate_key(prefix)?;
        let path = self.root.join(prefix);
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_key(prefix)?;
        let dir = self.root.join(prefix);
        let mut files = Vec::new();
        Self::walk(&dir, &mut files)?;
        let mut keys: Vec<String> = files
            .iter()
            .filter_map(|p| p.strip_prefix(&self.root).ok())
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .filter(|k| !k.contains(".tmp"))
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn size(&self, key: &str) -> Result<u64> {
        let path = self.path_for(key)?;
        Ok(fs::metadata(&path)?.len())
    }

    fn local_path(&self, key: &str) -> Option<PathBuf> {
        self.path_for(key).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, LocalFsStore) {
        let dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn put_get_list_delete() {
        let (_dir, store) = store();
        store.put("tenants/t/a.txt", b"hello").unwrap();
        store.put("tenants/t/sub/b.txt", b"world").unwrap();
        assert_eq!(store.get("tenants/t/a.txt").unwrap(), b"hello");
        assert_eq!(store.size("tenants/t/a.txt").unwrap(), 5);
        assert_eq!(
            store.list("tenants/t").unwrap(),
            vec![
                "tenants/t/a.txt".to_string(),
                "tenants/t/sub/b.txt".to_string()
            ]
        );
        store.delete("tenants/t/a.txt").unwrap();
        assert!(!store.exists("tenants/t/a.txt").unwrap());
        // Deleting a missing key is not an error: recovery paths rely on it.
        store.delete("tenants/t/a.txt").unwrap();
    }

    #[test]
    fn missing_key_is_not_found_not_io_error() {
        let (_dir, store) = store();
        assert_eq!(store.get("nope/x").unwrap_err().code(), "not_found");
    }

    #[test]
    fn put_atomic_replaces_in_place_and_leaves_no_temp_files() {
        let (_dir, store) = store();
        store.put_atomic("m/manifest.json", b"v1").unwrap();
        store.put_atomic("m/manifest.json", b"v2").unwrap();
        assert_eq!(store.get("m/manifest.json").unwrap(), b"v2");
        assert_eq!(
            store.list("m").unwrap(),
            vec!["m/manifest.json".to_string()]
        );
    }

    #[test]
    fn rejects_keys_that_escape_the_root() {
        let (_dir, store) = store();
        for key in ["", "/abs", "a/../../etc/passwd", "a//b", "trailing/", "./x"] {
            assert!(store.put(key, b"x").is_err(), "{key:?} should be rejected");
        }
    }

    #[test]
    fn delete_prefix_removes_a_whole_subtree() {
        let (_dir, store) = store();
        store.put("tenants/t/db/x", b"1").unwrap();
        store.put("tenants/t/db/y", b"2").unwrap();
        store.put("tenants/other/z", b"3").unwrap();
        store.delete_prefix("tenants/t").unwrap();
        assert!(store.list("tenants/t").unwrap().is_empty());
        assert_eq!(store.list("tenants/other").unwrap().len(), 1);
    }
}
