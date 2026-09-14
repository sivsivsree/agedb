//! Query budget and execution statistics.
//!
//! Limits are enforced *inside* execution, not at the edge (see "Guardrails" in ARCHITECTURE.md):
//! bytes are charged as segments are opened and the deadline is checked between
//! batches, so a hostile or careless plan is stopped partway rather than after it
//! has already cost the node everything.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use adb_core::{AdbError, QueryLimits, Result};
use serde::Serialize;

/// Shared, thread-safe accounting for one query.
#[derive(Debug)]
pub struct Budget {
    limits: QueryLimits,
    started: Instant,
    bytes_scanned: AtomicU64,
    rows_scanned: AtomicU64,
    segments_read: AtomicU64,
    segments_pruned: AtomicU64,
    truncated: AtomicBool,
}

impl Budget {
    pub fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            started: Instant::now(),
            bytes_scanned: AtomicU64::new(0),
            rows_scanned: AtomicU64::new(0),
            segments_read: AtomicU64::new(0),
            segments_pruned: AtomicU64::new(0),
            truncated: AtomicBool::new(false),
        }
    }

    pub fn limits(&self) -> &QueryLimits {
        &self.limits
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Charge bytes about to be read, failing if that would exceed the budget.
    pub fn charge_bytes(&self, bytes: u64) -> Result<()> {
        let total = self.bytes_scanned.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if total > self.limits.max_bytes_scanned {
            return Err(AdbError::LimitExceeded {
                limit: "query:max_bytes",
                detail: format!(
                    "would read {total} bytes, budget is {}",
                    self.limits.max_bytes_scanned
                ),
            });
        }
        Ok(())
    }

    pub fn charge_rows(&self, rows: u64) {
        self.rows_scanned.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn note_segment_read(&self) {
        self.segments_read.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_segment_pruned(&self) {
        self.segments_pruned.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the result was cut short by `max_rows` rather than by an
    /// explicit `limit`, so the caller can be told.
    pub fn note_truncated(&self) {
        self.truncated.store(true, Ordering::Relaxed);
    }

    pub fn was_truncated(&self) -> bool {
        self.truncated.load(Ordering::Relaxed)
    }

    /// Called between batches; turns a long-running query into an error.
    pub fn check_deadline(&self) -> Result<()> {
        if self.limits.max_execution_time_ms == u64::MAX {
            return Ok(());
        }
        let elapsed = self.started.elapsed().as_millis() as u64;
        if elapsed > self.limits.max_execution_time_ms {
            return Err(AdbError::LimitExceeded {
                limit: "query:max_execution_time",
                detail: format!(
                    "ran for {elapsed}ms, budget is {}ms",
                    self.limits.max_execution_time_ms
                ),
            });
        }
        Ok(())
    }

    pub fn stats(&self) -> ExecStats {
        ExecStats {
            bytes_scanned: self.bytes_scanned.load(Ordering::Relaxed),
            rows_scanned: self.rows_scanned.load(Ordering::Relaxed),
            segments_read: self.segments_read.load(Ordering::Relaxed),
            segments_pruned: self.segments_pruned.load(Ordering::Relaxed),
            rows_returned: 0,
            elapsed_ms: self.started.elapsed().as_millis() as u64,
            truncated: self.was_truncated(),
        }
    }
}

/// What executing a query actually cost. Returned to the caller because "how
/// much did you read" is exactly what an agent tuning a query needs to know.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ExecStats {
    pub bytes_scanned: u64,
    pub rows_scanned: u64,
    pub segments_read: u64,
    /// Segments skipped using statistics alone. High is good.
    pub segments_pruned: u64,
    pub rows_returned: u64,
    pub elapsed_ms: u64,
    /// The result hit the row budget and was cut short.
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_are_charged_until_the_budget_runs_out() {
        let budget = Budget::new(QueryLimits {
            max_bytes_scanned: 100,
            ..QueryLimits::default()
        });
        budget.charge_bytes(60).unwrap();
        let err = budget.charge_bytes(60).unwrap_err();
        assert_eq!(err.code(), "limit_exceeded");
        assert!(err.to_string().contains("query:max_bytes"), "{err}");
    }

    #[test]
    fn an_expired_deadline_stops_the_query() {
        let budget = Budget::new(QueryLimits {
            max_execution_time_ms: 0,
            ..QueryLimits::default()
        });
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(
            budget.check_deadline().unwrap_err().code(),
            "limit_exceeded"
        );
    }

    #[test]
    fn unlimited_budgets_never_trip() {
        let budget = Budget::new(QueryLimits::unlimited());
        budget.charge_bytes(u64::MAX / 2).unwrap();
        budget.check_deadline().unwrap();
    }

    #[test]
    fn stats_report_pruning_effectiveness() {
        let budget = Budget::new(QueryLimits::unlimited());
        budget.note_segment_read();
        budget.note_segment_pruned();
        budget.note_segment_pruned();
        budget.charge_rows(500);
        let stats = budget.stats();
        assert_eq!(stats.segments_read, 1);
        assert_eq!(stats.segments_pruned, 2);
        assert_eq!(stats.rows_scanned, 500);
        assert!(!stats.truncated);
    }
}
