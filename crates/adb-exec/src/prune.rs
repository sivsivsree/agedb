//! Segment pruning from statistics (see "Segments and pruning" in ARCHITECTURE.md).
//!
//! The rule is one-sided: `can_skip` may only return `true` when the segment
//! provably contains no matching row. Every row that *is* read still goes
//! through the residual filter, so a conservative answer costs time while a
//! wrong one would corrupt results.

use adb_core::Value;
use adb_planner::{PruneAtom, PruneOp};
use adb_storage::segment::{ColumnStats, SegmentMeta};

/// True when no row in `meta` can satisfy every atom.
pub fn can_skip(meta: &SegmentMeta, atoms: &[PruneAtom]) -> bool {
    if meta.row_count == 0 {
        return true;
    }
    atoms.iter().any(|atom| atom_excludes_segment(meta, atom))
}

fn atom_excludes_segment(meta: &SegmentMeta, atom: &PruneAtom) -> bool {
    // No statistics for the column (e.g. added by a later schema change): we
    // cannot conclude anything.
    let Some(stats) = meta.stats(&atom.column) else {
        return false;
    };

    match atom.op {
        PruneOp::IsNull => stats.null_count == 0,
        PruneOp::IsNotNull => stats.null_count == meta.row_count,
        _ => {
            // Only non-null values can satisfy a comparison, so a segment whose
            // column is entirely null cannot match.
            if stats.all_null() {
                return true;
            }
            let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
                return false;
            };
            match atom.op {
                PruneOp::Eq => atom
                    .values
                    .first()
                    .map(|v| v < min || v > max || !bloom_allows(stats, v))
                    .unwrap_or(false),
                PruneOp::Lt => atom.values.first().map(|v| min >= v).unwrap_or(false),
                PruneOp::LtEq => atom.values.first().map(|v| min > v).unwrap_or(false),
                PruneOp::Gt => atom.values.first().map(|v| max <= v).unwrap_or(false),
                PruneOp::GtEq => atom.values.first().map(|v| max < v).unwrap_or(false),
                PruneOp::In => {
                    !atom.values.is_empty()
                        && atom
                            .values
                            .iter()
                            .all(|v| v < min || v > max || !bloom_allows(stats, v))
                }
                PruneOp::IsNull | PruneOp::IsNotNull => unreachable!("handled above"),
            }
        }
    }
}

fn bloom_allows(stats: &ColumnStats, value: &Value) -> bool {
    match &stats.bloom {
        None => true,
        Some(filter) => filter.contains(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_storage::bloom;

    fn meta(rows: u64, columns: Vec<(&str, ColumnStats)>) -> SegmentMeta {
        SegmentMeta {
            id: 1,
            key: "seg.parquet".to_string(),
            row_count: rows,
            bytes: rows * 8,
            schema_version: 1,
            columns: columns
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    fn stats(min: i64, max: i64, nulls: u64) -> ColumnStats {
        ColumnStats {
            min: Some(Value::Int(min)),
            max: Some(Value::Int(max)),
            null_count: nulls,
            bloom: None,
        }
    }

    fn atom(column: &str, op: PruneOp, values: Vec<Value>) -> PruneAtom {
        PruneAtom {
            column: column.to_string(),
            op,
            values,
        }
    }

    #[test]
    fn range_predicates_skip_segments_outside_the_range() {
        let m = meta(100, vec![("at", stats(10, 20, 0))]);
        assert!(can_skip(
            &m,
            &[atom("at", PruneOp::Gt, vec![Value::Int(20)])]
        ));
        assert!(can_skip(
            &m,
            &[atom("at", PruneOp::GtEq, vec![Value::Int(21)])]
        ));
        assert!(can_skip(
            &m,
            &[atom("at", PruneOp::Lt, vec![Value::Int(10)])]
        ));
        assert!(can_skip(
            &m,
            &[atom("at", PruneOp::LtEq, vec![Value::Int(9)])]
        ));

        // Overlapping ranges must be read.
        assert!(!can_skip(
            &m,
            &[atom("at", PruneOp::Gt, vec![Value::Int(15)])]
        ));
        assert!(!can_skip(
            &m,
            &[atom("at", PruneOp::GtEq, vec![Value::Int(20)])]
        ));
        assert!(!can_skip(
            &m,
            &[atom("at", PruneOp::Lt, vec![Value::Int(11)])]
        ));
    }

    #[test]
    fn equality_uses_range_then_bloom() {
        let m = meta(100, vec![("id", stats(10, 20, 0))]);
        assert!(can_skip(
            &m,
            &[atom("id", PruneOp::Eq, vec![Value::Int(30)])]
        ));
        assert!(!can_skip(
            &m,
            &[atom("id", PruneOp::Eq, vec![Value::Int(15)])]
        ));

        // With a bloom filter, an in-range value that was never inserted is
        // also skippable.
        let filter = bloom::build(vec![Value::Int(10), Value::Int(20)].into_iter()).unwrap();
        let m = meta(
            2,
            vec![(
                "id",
                ColumnStats {
                    min: Some(Value::Int(10)),
                    max: Some(Value::Int(20)),
                    null_count: 0,
                    bloom: Some(filter),
                },
            )],
        );
        assert!(can_skip(
            &m,
            &[atom("id", PruneOp::Eq, vec![Value::Int(15)])]
        ));
        assert!(!can_skip(
            &m,
            &[atom("id", PruneOp::Eq, vec![Value::Int(20)])]
        ));
    }

    #[test]
    fn in_lists_are_skipped_only_when_every_value_is_impossible() {
        let m = meta(100, vec![("id", stats(10, 20, 0))]);
        assert!(can_skip(
            &m,
            &[atom(
                "id",
                PruneOp::In,
                vec![Value::Int(1), Value::Int(100)]
            )]
        ));
        assert!(!can_skip(
            &m,
            &[atom("id", PruneOp::In, vec![Value::Int(1), Value::Int(15)])]
        ));
    }

    #[test]
    fn null_checks_use_null_counts() {
        let no_nulls = meta(100, vec![("note", stats(1, 5, 0))]);
        assert!(can_skip(
            &no_nulls,
            &[atom("note", PruneOp::IsNull, vec![])]
        ));
        assert!(!can_skip(
            &no_nulls,
            &[atom("note", PruneOp::IsNotNull, vec![])]
        ));

        let all_nulls = meta(
            100,
            vec![(
                "note",
                ColumnStats {
                    min: None,
                    max: None,
                    null_count: 100,
                    bloom: None,
                },
            )],
        );
        assert!(!can_skip(
            &all_nulls,
            &[atom("note", PruneOp::IsNull, vec![])]
        ));
        assert!(can_skip(
            &all_nulls,
            &[atom("note", PruneOp::IsNotNull, vec![])]
        ));
        // A comparison cannot match an all-null column either.
        assert!(can_skip(
            &all_nulls,
            &[atom("note", PruneOp::Eq, vec![Value::Int(1)])]
        ));
    }

    #[test]
    fn unknown_columns_and_empty_atoms_never_prune() {
        let m = meta(100, vec![("id", stats(10, 20, 0))]);
        assert!(!can_skip(&m, &[]));
        assert!(!can_skip(
            &m,
            &[atom("added_later", PruneOp::Eq, vec![Value::Int(1)])]
        ));
    }

    #[test]
    fn atoms_are_conjunctive_so_any_impossible_one_skips() {
        let m = meta(
            100,
            vec![("id", stats(10, 20, 0)), ("at", stats(100, 200, 0))],
        );
        let atoms = vec![
            atom("id", PruneOp::Eq, vec![Value::Int(15)]), // possible
            atom("at", PruneOp::Gt, vec![Value::Int(500)]), // impossible
        ];
        assert!(can_skip(&m, &atoms));
    }

    #[test]
    fn an_empty_segment_is_always_skippable() {
        assert!(can_skip(&meta(0, vec![]), &[]));
    }
}
