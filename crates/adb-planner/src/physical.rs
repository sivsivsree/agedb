//! Physical plan: the linear pipeline the executor runs.
//!
//! ```text
//! scan(projection, pruning) -> filter -> aggregate -> rename -> sort -> limit
//! ```
//!
//! v0.1 executes exactly this shape, which covers the readme's section 18
//! feature set. Logical plans that do not fit, such as a filter above an aggregate
//! (`HAVING`), a limit below one, or two aggregations, are rejected here with a
//! specific message instead of being silently reinterpreted.

use std::collections::BTreeSet;

use adb_core::{AdbError, Result};

use crate::expr::Expr;
use crate::ir::{AggregateExpr, OutputSchema, Query, SortExpr, TableRef};
use crate::optimize::{extract_prune_atoms, PruneAtom};
use crate::validate::ValidatedQuery;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub table: TableRef,
    /// Base columns to read from storage, in table order.
    pub projection: Vec<String>,
    /// Constraints usable against segment statistics.
    pub pruning: Vec<PruneAtom>,
    /// Residual predicate, evaluated on every row that is read.
    pub filter: Option<Expr>,
    pub aggregate: Option<AggregateSpec>,
    /// `(source, output name)` applied after aggregation.
    pub rename: Option<Vec<(String, String)>>,
    pub sort: Vec<SortExpr>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub output: OutputSchema,
}

impl PhysicalPlan {
    /// True when the executor can stop as soon as it has `limit + offset` rows.
    pub fn is_streaming_limit(&self) -> bool {
        self.limit.is_some() && self.sort.is_empty() && self.aggregate.is_none()
    }

    /// Rows to materialize before applying `offset`.
    pub fn fetch_rows(&self) -> Option<usize> {
        self.limit.map(|n| n.saturating_add(self.offset))
    }

    pub fn explain(&self) -> String {
        let mut out = format!("scan {} [{}]", self.table, self.projection.join(", "));
        if !self.pruning.is_empty() {
            let atoms = self
                .pruning
                .iter()
                .map(|a| {
                    let values = a
                        .values
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{} {:?} [{}]", a.column, a.op, values)
                })
                .collect::<Vec<_>>()
                .join(" and ");
            out.push_str(&format!("\n  prune: {atoms}"));
        }
        if let Some(filter) = &self.filter {
            out.push_str(&format!("\n  filter: {filter}"));
        }
        if let Some(agg) = &self.aggregate {
            let aggs = agg
                .aggregates
                .iter()
                .map(|a| {
                    format!(
                        "{}({})",
                        a.func,
                        a.column.clone().unwrap_or_else(|| "*".to_string())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!(
                "\n  aggregate: group_by=[{}] {aggs}",
                agg.group_by.join(", ")
            ));
        }
        if !self.sort.is_empty() {
            let keys = self
                .sort
                .iter()
                .map(|s| format!("{} {}", s.column, if s.ascending { "asc" } else { "desc" }))
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("\n  sort: {keys}"));
        }
        if let Some(limit) = self.limit {
            out.push_str(&format!("\n  limit: {limit} offset {}", self.offset));
        }
        out
    }
}

/// Pipeline stages, in the order the executor applies them. A logical node may
/// never move the pipeline backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Scan,
    Filter,
    Aggregate,
    Rename,
    Sort,
    Limit,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Aggregate => "aggregate",
            Self::Rename => "projection",
            Self::Sort => "sort",
            Self::Limit => "limit",
        }
    }
}

/// Lower a validated logical plan into the physical pipeline.
pub fn build(validated: &ValidatedQuery) -> Result<PhysicalPlan> {
    // Bottom-up: the scan first.
    let mut nodes = Vec::new();
    let mut cursor = Some(&validated.query);
    while let Some(node) = cursor {
        nodes.push(node);
        cursor = node.input();
    }
    nodes.reverse();

    let mut table = None;
    let mut scan_columns = Vec::new();
    let mut filter: Option<Expr> = None;
    let mut aggregate: Option<AggregateSpec> = None;
    let mut rename: Option<Vec<(String, String)>> = None;
    let mut sort: Vec<SortExpr> = Vec::new();
    let mut limit: Option<usize> = None;
    let mut offset = 0usize;
    let mut stage = Stage::Scan;

    for node in nodes {
        let node_stage = match node {
            Query::Scan { .. } => Stage::Scan,
            Query::Filter { .. } => Stage::Filter,
            Query::Aggregate { .. } => Stage::Aggregate,
            Query::Project { .. } => Stage::Rename,
            Query::Sort { .. } => Stage::Sort,
            Query::Limit { .. } => Stage::Limit,
            Query::Join { .. } => {
                return Err(AdbError::Unsupported(
                    "joins are not available in v0.1".to_string(),
                ))
            }
        };
        if node_stage < stage {
            // Two orderings differ from the pipeline but are equivalent after a
            // rewrite: filtering on projected names (pushed back onto base
            // columns) and aggregating after a projection that only selects.
            let equivalent = match (stage, node_stage) {
                (Stage::Rename, Stage::Filter) => aggregate.is_none(),
                (Stage::Rename, Stage::Aggregate) => true,
                _ => false,
            };
            if !equivalent {
                return Err(AdbError::Unsupported(format!(
                    "{} after {} is not executable in v0.1",
                    node_stage.name(),
                    stage.name()
                )));
            }
        }

        match node {
            Query::Scan {
                table: table_ref,
                columns,
            } => {
                table = Some(table_ref.clone());
                scan_columns = columns.clone().unwrap_or_default();
            }
            Query::Filter { predicate, .. } => {
                // A filter above a projection refers to output names; push those
                // back onto base columns, since the pipeline renames last.
                let predicate = match &rename {
                    None => predicate.clone(),
                    Some(map) => predicate.rewrite_columns(&|name| {
                        map.iter()
                            .find(|(_, alias)| alias == name)
                            .map(|(src, _)| src.clone())
                    })?,
                };
                filter = Some(match filter {
                    None => predicate,
                    Some(existing) => existing.and(predicate),
                });
            }
            Query::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                if aggregate.is_some() {
                    return Err(AdbError::Unsupported(
                        "two aggregations in one plan".to_string(),
                    ));
                }
                if let Some(map) = &rename {
                    // A projection below an aggregate is only allowed when it
                    // just selects columns: renaming there would change the
                    // aggregate's output names too.
                    if map.iter().any(|(source, alias)| source != alias) {
                        return Err(AdbError::Unsupported(
                            "renaming columns before aggregating".to_string(),
                        ));
                    }
                    rename = None;
                }
                aggregate = Some(AggregateSpec {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                });
            }
            Query::Project { exprs, .. } => {
                let map: Vec<(String, String)> = exprs
                    .iter()
                    .map(|(expr, alias)| {
                        let source = expr.as_column().ok_or_else(|| {
                            AdbError::Unsupported(format!("computed projection ({expr})"))
                        })?;
                        Ok((source.to_string(), alias.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Compose with any projection already applied.
                rename = Some(match rename {
                    None => map,
                    Some(existing) => map
                        .into_iter()
                        .map(|(source, alias)| {
                            let base = existing
                                .iter()
                                .find(|(_, prior)| *prior == source)
                                .map(|(base, _)| base.clone())
                                .unwrap_or(source);
                            (base, alias)
                        })
                        .collect(),
                });
            }
            Query::Sort { exprs, .. } => {
                if !sort.is_empty() {
                    return Err(AdbError::Unsupported("two sorts in one plan".to_string()));
                }
                sort = exprs.clone();
            }
            Query::Limit {
                limit: n,
                offset: skip,
                ..
            } => {
                // Nested limits intersect: the tighter bound wins.
                limit = Some(match limit {
                    None => *n,
                    Some(existing) => existing.min(*n),
                });
                offset = offset.saturating_add(*skip);
            }
            Query::Join { .. } => unreachable!("rejected above"),
        }
        // The pipeline never moves backwards, even when a node was accepted
        // out of order above.
        stage = stage.max(node_stage);
    }

    let table = table.ok_or_else(|| AdbError::Internal("plan has no scan".to_string()))?;

    // Projection pushdown: read only what the pipeline touches.
    let mut needed: BTreeSet<String> = BTreeSet::new();
    if let Some(filter) = &filter {
        needed.extend(filter.referenced_columns());
    }
    match &aggregate {
        Some(spec) => {
            needed.extend(spec.group_by.iter().cloned());
            needed.extend(spec.aggregates.iter().filter_map(|a| a.column.clone()));
        }
        None => {
            match &rename {
                Some(map) => needed.extend(map.iter().map(|(source, _)| source.clone())),
                None => needed.extend(scan_columns.iter().cloned()),
            }
            // Without an aggregate, sort keys name output columns; map them back.
            for key in &sort {
                let source = match &rename {
                    Some(map) => map
                        .iter()
                        .find(|(_, alias)| *alias == key.column)
                        .map(|(source, _)| source.clone())
                        .ok_or_else(|| {
                            AdbError::Unsupported(format!(
                                "sorting by {:?}, which the projection removes",
                                key.column
                            ))
                        })?,
                    None => key.column.clone(),
                };
                needed.insert(source);
            }
        }
    }
    // Keep table order so the executor's column indices are stable.
    let mut projection: Vec<String> = if scan_columns.is_empty() {
        needed.iter().cloned().collect()
    } else {
        scan_columns
            .iter()
            .filter(|c| needed.contains(*c))
            .cloned()
            .collect()
    };
    if projection.is_empty() {
        // `count(*)` references no column at all. Read one anyway so the scan
        // has a shape to count rows of; the executor may skip the read entirely
        // and answer from metadata.
        let fallback = scan_columns
            .first()
            .cloned()
            .or_else(|| needed.iter().next().cloned())
            .ok_or_else(|| AdbError::Internal("plan reads no columns".to_string()))?;
        projection.push(fallback);
    }

    let pruning = filter.as_ref().map(extract_prune_atoms).unwrap_or_default();

    Ok(PhysicalPlan {
        table,
        projection,
        pruning,
        filter,
        aggregate,
        rename,
        sort,
        limit,
        offset,
        output: validated.schema.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{AggregateFunc, SortExpr};
    use crate::validate::validate;
    use adb_core::{
        Aggregation, ColumnSchema, DataType, QueryLimits, SemanticType, TableName, TableSchema,
        Value,
    };

    fn orders() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("amount", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum),
                ColumnSchema::new("created_at", DataType::Timestamp),
            ],
        )
    }

    fn plan(query: Query) -> Result<PhysicalPlan> {
        let limits = QueryLimits {
            max_rows: 1000,
            ..QueryLimits::default()
        };
        build(&validate(&query, &orders(), &limits)?)
    }

    fn scan() -> Query {
        Query::scan(TableName::new("orders").unwrap())
    }

    #[test]
    fn projection_pushdown_reads_only_what_is_used() {
        let p = plan(
            scan()
                .filter(Expr::col("created_at").gt(Expr::lit(Value::Str("2026-01-01".into()))))
                .aggregate(
                    vec!["country".to_string()],
                    vec![AggregateExpr::new(
                        AggregateFunc::Sum,
                        Some("amount".into()),
                        "revenue",
                    )],
                ),
        )
        .unwrap();
        assert_eq!(p.projection, vec!["country", "amount", "created_at"]);
        assert!(!p.projection.contains(&"id".to_string()));
    }

    #[test]
    fn a_plain_scan_reads_its_declared_columns() {
        let p = plan(scan()).unwrap();
        assert_eq!(p.projection, vec!["id", "country", "amount", "created_at"]);
        assert!(p.filter.is_none());
        assert!(p.pruning.is_empty());
    }

    #[test]
    fn pruning_atoms_are_derived_from_the_filter() {
        let p = plan(
            scan().filter(
                Expr::col("created_at")
                    .gt(Expr::lit(Value::Str("2026-01-01".into())))
                    .and(Expr::col("country").eq(Expr::lit(Value::Str("uae".into())))),
            ),
        )
        .unwrap();
        assert_eq!(p.pruning.len(), 2);
        // The literal was coerced during validation, so pruning compares
        // timestamps against timestamp statistics.
        assert_eq!(
            p.pruning[0].values[0],
            Value::Timestamp(1_767_225_600_000_000)
        );
        assert!(
            p.filter.is_some(),
            "the residual filter is still applied per row"
        );
    }

    #[test]
    fn multiple_filters_are_merged() {
        let p = plan(
            scan()
                .filter(Expr::col("amount").gt(Expr::lit(Value::Int(10))))
                .filter(Expr::col("country").eq(Expr::lit(Value::Str("uae".into())))),
        )
        .unwrap();
        assert_eq!(p.filter.as_ref().unwrap().conjuncts().len(), 2);
        assert_eq!(p.pruning.len(), 2);
    }

    #[test]
    fn sort_and_limit_become_a_bounded_fetch() {
        let p = plan(
            scan()
                .aggregate(
                    vec!["country".to_string()],
                    vec![AggregateExpr::new(
                        AggregateFunc::Sum,
                        Some("amount".into()),
                        "revenue",
                    )],
                )
                .sort(vec![SortExpr::desc("revenue")])
                .limit(20),
        )
        .unwrap();
        assert_eq!(p.limit, Some(20));
        assert_eq!(p.sort.len(), 1);
        assert!(
            !p.is_streaming_limit(),
            "a sorted plan must see every row first"
        );
        assert_eq!(p.fetch_rows(), Some(20));
    }

    #[test]
    fn an_unsorted_limit_can_stop_early() {
        let p = plan(scan().limit_offset(10, 5)).unwrap();
        assert!(p.is_streaming_limit());
        assert_eq!(p.fetch_rows(), Some(15));
    }

    #[test]
    fn nested_limits_take_the_tighter_bound() {
        let p = plan(scan().limit(50).limit(10)).unwrap();
        assert_eq!(p.limit, Some(10));
    }

    #[test]
    fn filters_above_a_projection_are_rewritten_to_base_columns() {
        let p = plan(
            scan()
                .project(vec![
                    (Expr::col("amount"), "revenue".to_string()),
                    (Expr::col("country"), "country".to_string()),
                ])
                .filter(Expr::col("revenue").gt(Expr::lit(Value::Int(100)))),
        )
        .unwrap();
        // The residual filter names the storage column, not the alias.
        assert_eq!(p.filter.as_ref().unwrap().to_string(), "(amount > 100)");
        assert_eq!(p.pruning[0].column, "amount");
        assert_eq!(p.output.names(), vec!["revenue", "country"]);
    }

    #[test]
    fn sorting_by_a_projected_away_column_is_refused() {
        let query = scan()
            .project(vec![(Expr::col("country"), "country".to_string())])
            .sort(vec![SortExpr::asc("country")]);
        plan(query).unwrap();

        // Validation rejects sorting by a column the projection dropped, since
        // it is not in the plan's output schema.
        let query = scan()
            .project(vec![(Expr::col("country"), "country".to_string())])
            .sort(vec![SortExpr::asc("amount")]);
        assert!(plan(query).is_err());
    }

    #[test]
    fn shapes_the_pipeline_cannot_express_are_rejected_clearly() {
        // HAVING: a filter after aggregation.
        let having = scan()
            .aggregate(vec![], vec![AggregateExpr::count_star("n")])
            .filter(Expr::col("n").gt(Expr::lit(Value::Int(1))));
        let err = plan(having).unwrap_err();
        assert_eq!(err.code(), "unsupported");
        assert!(err.to_string().contains("filter after aggregate"), "{err}");

        // Aggregating a limited subset.
        let limited = scan()
            .limit(10)
            .aggregate(vec![], vec![AggregateExpr::count_star("n")]);
        assert_eq!(plan(limited).unwrap_err().code(), "unsupported");

        // Renaming before aggregating.
        let renamed = scan()
            .project(vec![(Expr::col("amount"), "revenue".to_string())])
            .aggregate(
                vec![],
                vec![AggregateExpr::new(
                    AggregateFunc::Sum,
                    Some("revenue".into()),
                    "total",
                )],
            );
        assert_eq!(plan(renamed).unwrap_err().code(), "unsupported");
    }

    #[test]
    fn a_pure_column_selection_below_an_aggregate_is_allowed() {
        let query = scan()
            .project(vec![
                (Expr::col("country"), "country".to_string()),
                (Expr::col("amount"), "amount".to_string()),
            ])
            .aggregate(
                vec!["country".to_string()],
                vec![AggregateExpr::new(
                    AggregateFunc::Sum,
                    Some("amount".into()),
                    "revenue",
                )],
            );
        let p = plan(query).unwrap();
        assert!(p.rename.is_none());
        assert_eq!(p.projection, vec!["country", "amount"]);
    }

    #[test]
    fn explain_shows_the_pipeline() {
        let p = plan(
            scan()
                .filter(Expr::col("amount").gt(Expr::lit(Value::Int(10))))
                .aggregate(
                    vec!["country".to_string()],
                    vec![AggregateExpr::count_star("orders")],
                )
                .sort(vec![SortExpr::desc("orders")])
                .limit(5),
        )
        .unwrap();
        let text = p.explain();
        for expected in [
            "scan orders",
            "prune:",
            "filter:",
            "aggregate:",
            "sort:",
            "limit: 5",
        ] {
            assert!(text.contains(expected), "{expected} missing from:\n{text}");
        }
    }
}
