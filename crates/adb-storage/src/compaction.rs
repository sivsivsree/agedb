//! Compaction policy (see "Writes, updates and deletes" in ARCHITECTURE.md).
//!
//! Two reasons to rewrite segments:
//!
//! 1. **Dead weight.** Updates and deletes suppress rows rather than rewriting
//!    files, so a segment can end up mostly invisible. Rewriting reclaims the
//!    space and shortens scans.
//! 2. **Too many small files.** Every segment costs an object read and a set of
//!    statistics to evaluate; merging the small ones keeps scans cheap.
//!
//! This module is only the *decision*; `table.rs` performs the merge, because
//! that needs the partition lock and the key index.

#[derive(Debug, Clone, Copy)]
pub struct CompactionPolicy {
    /// Merge small segments once a partition has at least this many.
    pub min_segments: usize,
    /// Never feed more than this many bytes into one merge.
    pub max_input_bytes: u64,
    /// Rewrite a single segment when at least this fraction of its rows are
    /// suppressed.
    pub suppressed_ratio: f64,
    /// Upper bound on inputs per merge, to keep any one merge bounded.
    pub max_inputs: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            min_segments: 8,
            max_input_bytes: 512 << 20,
            suppressed_ratio: 0.25,
            max_inputs: 16,
        }
    }
}

/// What the policy needs to know about one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFacts {
    pub id: u64,
    pub rows: u64,
    pub bytes: u64,
    pub suppressed: u64,
}

impl SegmentFacts {
    fn dead_ratio(&self) -> f64 {
        if self.rows == 0 {
            return 1.0;
        }
        self.suppressed as f64 / self.rows as f64
    }
}

/// Segment ids to merge, in ascending (arrival) order. Empty means "nothing to
/// do". A single id is returned only when that segment is mostly dead rows.
pub fn select(facts: &[SegmentFacts], policy: &CompactionPolicy) -> Vec<u64> {
    let mut dead: Vec<&SegmentFacts> = facts
        .iter()
        .filter(|f| f.dead_ratio() >= policy.suppressed_ratio)
        .collect();
    if !dead.is_empty() {
        dead.sort_by_key(|f| f.id);
        let mut out = Vec::new();
        let mut budget = policy.max_input_bytes;
        for f in dead {
            if out.len() >= policy.max_inputs || f.bytes > budget {
                break;
            }
            budget -= f.bytes;
            out.push(f.id);
        }
        if !out.is_empty() {
            return out;
        }
    }

    if facts.len() < policy.min_segments {
        return Vec::new();
    }
    // Smallest-first keeps write amplification down: repeatedly merging the
    // small files approximates a size-tiered strategy without a level manifest.
    let mut by_size: Vec<&SegmentFacts> = facts.iter().collect();
    by_size.sort_by_key(|f| (f.bytes, f.id));
    let mut chosen = Vec::new();
    let mut budget = policy.max_input_bytes;
    for f in by_size {
        if chosen.len() >= policy.max_inputs || f.bytes > budget {
            break;
        }
        budget -= f.bytes;
        chosen.push(f.id);
    }
    if chosen.len() < 2 {
        return Vec::new();
    }
    chosen.sort_unstable();
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(spec: &[(u64, u64, u64, u64)]) -> Vec<SegmentFacts> {
        spec.iter()
            .map(|&(id, rows, bytes, suppressed)| SegmentFacts {
                id,
                rows,
                bytes,
                suppressed,
            })
            .collect()
    }

    #[test]
    fn does_nothing_when_there_is_little_to_gain() {
        let policy = CompactionPolicy::default();
        assert!(select(&[], &policy).is_empty());
        assert!(select(&facts(&[(1, 1000, 4096, 0), (2, 1000, 4096, 10)]), &policy).is_empty());
    }

    #[test]
    fn rewrites_a_single_mostly_dead_segment() {
        let policy = CompactionPolicy::default();
        let chosen = select(&facts(&[(1, 1000, 4096, 900), (2, 1000, 4096, 0)]), &policy);
        assert_eq!(chosen, vec![1]);
    }

    #[test]
    fn merges_small_segments_once_there_are_enough() {
        let policy = CompactionPolicy {
            min_segments: 4,
            ..Default::default()
        };
        let chosen = select(
            &facts(&[
                (1, 10, 100, 0),
                (2, 10, 100, 0),
                (3, 10, 5_000_000, 0),
                (4, 10, 100, 0),
            ]),
            &policy,
        );
        // Ascending id order, and the big segment comes last by size so it is
        // still included here only because the budget allows it.
        assert_eq!(chosen, vec![1, 2, 3, 4]);
    }

    #[test]
    fn respects_the_byte_budget_and_input_cap() {
        let policy = CompactionPolicy {
            min_segments: 2,
            max_input_bytes: 250,
            max_inputs: 3,
            ..Default::default()
        };
        let chosen = select(
            &facts(&[
                (1, 10, 100, 0),
                (2, 10, 100, 0),
                (3, 10, 100, 0),
                (4, 10, 100, 0),
            ]),
            &policy,
        );
        assert_eq!(chosen, vec![1, 2]);
    }

    #[test]
    fn output_is_always_in_arrival_order() {
        let policy = CompactionPolicy {
            min_segments: 2,
            ..Default::default()
        };
        let chosen = select(
            &facts(&[(9, 10, 900, 0), (3, 10, 100, 0), (7, 10, 500, 0)]),
            &policy,
        );
        assert_eq!(chosen, vec![3, 7, 9]);
    }

    #[test]
    fn an_empty_segment_counts_as_fully_dead() {
        let policy = CompactionPolicy::default();
        assert_eq!(select(&facts(&[(5, 0, 512, 0)]), &policy), vec![5]);
    }
}
