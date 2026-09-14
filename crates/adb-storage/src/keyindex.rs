//! Primary-key index.
//!
//! Updates and deletes never rewrite a segment (see "Writes, updates and deletes" in ARCHITECTURE.md). Instead the
//! newest row for a key wins and older copies are suppressed. Answering "where
//! does key K currently live?" in O(1) is what makes that cheap, so tables with
//! a primary key keep this index in memory; append-only tables keep nothing.
//!
//! Memory cost is roughly 60-100 bytes per live key, which is why `upsert`,
//! `update` and `delete` require a primary key and bulk analytical tables are
//! expected to be append-only.

use std::collections::HashMap;

use adb_core::Value;

/// A primary-key tuple, in key column order.
pub type RowKey = Vec<Value>;

/// Which store a row lives in. Ordering of `Segment` ids matches arrival order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// Currently buffered in the memtable.
    Memtable,
    /// Written to the segment with this id.
    Segment(u64),
}

/// Physical address of one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Locator {
    pub source: Source,
    /// Row ordinal within the source.
    pub ordinal: u32,
}

impl Locator {
    pub fn memtable(ordinal: u32) -> Self {
        Self {
            source: Source::Memtable,
            ordinal,
        }
    }

    pub fn segment(id: u64, ordinal: u32) -> Self {
        Self {
            source: Source::Segment(id),
            ordinal,
        }
    }
}

#[derive(Debug, Default)]
pub struct KeyIndex {
    map: HashMap<RowKey, Locator>,
}

impl KeyIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &RowKey) -> Option<Locator> {
        self.map.get(key).copied()
    }

    pub fn contains(&self, key: &RowKey) -> bool {
        self.map.contains_key(key)
    }

    /// Point `key` at `locator`, returning the address it displaced (which the
    /// caller must suppress).
    pub fn insert(&mut self, key: RowKey, locator: Locator) -> Option<Locator> {
        self.map.insert(key, locator)
    }

    /// Forget `key`, returning the address to suppress.
    pub fn remove(&mut self, key: &RowKey) -> Option<Locator> {
        self.map.remove(key)
    }

    /// Move a key from one address to another, but only if it still points at
    /// `from`. Used when a flush or compaction relocates rows: a key that has
    /// since been superseded must not be dragged back.
    pub fn relocate(&mut self, key: &RowKey, from: Locator, to: Locator) -> bool {
        match self.map.get_mut(key) {
            Some(current) if *current == from => {
                *current = to;
                true
            }
            _ => false,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RowKey, &Locator)> {
        self.map.iter()
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: i64) -> RowKey {
        vec![Value::Int(n)]
    }

    #[test]
    fn insert_reports_the_displaced_address() {
        let mut index = KeyIndex::new();
        assert_eq!(index.insert(key(1), Locator::segment(3, 7)), None);
        assert_eq!(
            index.insert(key(1), Locator::memtable(0)),
            Some(Locator::segment(3, 7))
        );
        assert_eq!(index.get(&key(1)), Some(Locator::memtable(0)));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn remove_reports_the_address_to_suppress() {
        let mut index = KeyIndex::new();
        index.insert(key(1), Locator::segment(2, 5));
        assert_eq!(index.remove(&key(1)), Some(Locator::segment(2, 5)));
        assert_eq!(index.remove(&key(1)), None);
        assert!(index.is_empty());
    }

    #[test]
    fn relocate_only_moves_a_key_that_still_lives_there() {
        let mut index = KeyIndex::new();
        index.insert(key(1), Locator::memtable(4));
        assert!(index.relocate(&key(1), Locator::memtable(4), Locator::segment(9, 0)));
        assert_eq!(index.get(&key(1)), Some(Locator::segment(9, 0)));

        // A stale relocation (the key moved on) is refused.
        assert!(!index.relocate(&key(1), Locator::memtable(4), Locator::segment(10, 0)));
        assert_eq!(index.get(&key(1)), Some(Locator::segment(9, 0)));
        assert!(!index.relocate(&key(2), Locator::memtable(0), Locator::segment(1, 1)));
    }

    #[test]
    fn composite_keys_are_distinguished_by_order() {
        let mut index = KeyIndex::new();
        let a = vec![Value::Int(1), Value::Str("x".into())];
        let b = vec![Value::Str("x".into()), Value::Int(1)];
        index.insert(a.clone(), Locator::memtable(0));
        index.insert(b.clone(), Locator::memtable(1));
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&a), Some(Locator::memtable(0)));
        assert_eq!(index.get(&b), Some(Locator::memtable(1)));
    }
}
