//! The logical Query IR. See "The Query IR is the centre" in ARCHITECTURE.md.
//!
//! Deliberately a small algebra: scan, filter, project, aggregate, sort, limit.
//! `Join` exists as a variant because it is part of the intended shape, but the
//! v0.1 validator rejects it rather than pretending.

use std::fmt;

use adb_core::{Aggregation, DataType, DatabaseName, TableName};
use serde::{Deserialize, Serialize};

use crate::expr::Expr;

/// The table a scan reads. `database` is normally `None`, because the request
/// context selects the database. It is here for cross-database plans later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseName>,
    pub table: TableName,
}

impl TableRef {
    pub fn new(table: TableName) -> Self {
        Self {
            database: None,
            table,
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.database {
            Some(db) => write!(f, "{db}.{}", self.table),
            None => write!(f, "{}", self.table),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunc {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    pub fn from_aggregation(agg: Aggregation) -> Self {
        match agg {
            Aggregation::Count => Self::Count,
            Aggregation::Sum => Self::Sum,
            Aggregation::Avg => Self::Avg,
            Aggregation::Min => Self::Min,
            Aggregation::Max => Self::Max,
        }
    }

    /// Result type given the argument type. `None` argument means `count(*)`.
    pub fn output_type(self, argument: Option<DataType>) -> DataType {
        match self {
            Self::Count => DataType::Int64,
            Self::Avg => DataType::Float64,
            Self::Sum => match argument {
                Some(DataType::Int64) => DataType::Int64,
                _ => DataType::Float64,
            },
            Self::Min | Self::Max => argument.unwrap_or(DataType::Float64),
        }
    }
}

impl fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    /// `None` only for `count(*)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    pub alias: String,
}

impl AggregateExpr {
    pub fn new(func: AggregateFunc, column: Option<String>, alias: impl Into<String>) -> Self {
        Self {
            func,
            column,
            alias: alias.into(),
        }
    }

    pub fn count_star(alias: impl Into<String>) -> Self {
        Self::new(AggregateFunc::Count, None, alias)
    }

    /// The alias an agent would expect if it did not supply one.
    pub fn default_alias(func: AggregateFunc, column: Option<&str>) -> String {
        match column {
            Some(c) => format!("{}_{}", func.as_str(), c),
            None => "count".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortExpr {
    pub column: String,
    #[serde(default = "default_ascending")]
    pub ascending: bool,
    /// SQL default: nulls sort last ascending, first descending.
    #[serde(default)]
    pub nulls_first: bool,
}

fn default_ascending() -> bool {
    true
}

impl SortExpr {
    pub fn asc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            ascending: true,
            nulls_first: false,
        }
    }

    pub fn desc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            ascending: false,
            nulls_first: false,
        }
    }
}

/// The logical plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    Scan {
        table: TableRef,
        /// `None` means every non-sensitive column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        columns: Option<Vec<String>>,
    },
    Filter {
        input: Box<Query>,
        predicate: Expr,
    },
    /// Column selection and renaming. v0.1 allows column references only, not
    /// computed expressions.
    Project {
        input: Box<Query>,
        /// (expression, output name)
        exprs: Vec<(Expr, String)>,
    },
    Aggregate {
        input: Box<Query>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    Sort {
        input: Box<Query>,
        exprs: Vec<SortExpr>,
    },
    Limit {
        input: Box<Query>,
        limit: usize,
        #[serde(default)]
        offset: usize,
    },
    /// Part of the intended IR shape; not executable in v0.1.
    Join {
        left: Box<Query>,
        right: Box<Query>,
        condition: Expr,
    },
}

impl Query {
    pub fn scan(table: TableName) -> Self {
        Self::Scan {
            table: TableRef::new(table),
            columns: None,
        }
    }

    pub fn scan_columns(table: TableName, columns: Vec<String>) -> Self {
        Self::Scan {
            table: TableRef::new(table),
            columns: Some(columns),
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        Self::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    pub fn project(self, exprs: Vec<(Expr, String)>) -> Self {
        Self::Project {
            input: Box::new(self),
            exprs,
        }
    }

    pub fn aggregate(self, group_by: Vec<String>, aggregates: Vec<AggregateExpr>) -> Self {
        Self::Aggregate {
            input: Box::new(self),
            group_by,
            aggregates,
        }
    }

    pub fn sort(self, exprs: Vec<SortExpr>) -> Self {
        Self::Sort {
            input: Box::new(self),
            exprs,
        }
    }

    pub fn limit(self, limit: usize) -> Self {
        Self::Limit {
            input: Box::new(self),
            limit,
            offset: 0,
        }
    }

    pub fn limit_offset(self, limit: usize, offset: usize) -> Self {
        Self::Limit {
            input: Box::new(self),
            limit,
            offset,
        }
    }

    pub fn input(&self) -> Option<&Query> {
        match self {
            Self::Scan { .. } => None,
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. } => Some(input),
            Self::Join { left, .. } => Some(left),
        }
    }

    /// The table this plan reads, if it is a single-table plan.
    pub fn table(&self) -> Option<&TableRef> {
        match self {
            Self::Scan { table, .. } => Some(table),
            other => other.input().and_then(Query::table),
        }
    }

    pub fn node_name(&self) -> &'static str {
        match self {
            Self::Scan { .. } => "scan",
            Self::Filter { .. } => "filter",
            Self::Project { .. } => "project",
            Self::Aggregate { .. } => "aggregate",
            Self::Sort { .. } => "sort",
            Self::Limit { .. } => "limit",
            Self::Join { .. } => "join",
        }
    }

    /// Human-readable plan tree, for `explain` output and error messages.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        self.explain_into(0, &mut out);
        out
    }

    fn explain_into(&self, depth: usize, out: &mut String) {
        let pad = "  ".repeat(depth);
        match self {
            Self::Scan { table, columns } => {
                let cols = match columns {
                    Some(c) => c.join(", "),
                    None => "*".to_string(),
                };
                out.push_str(&format!("{pad}scan {table} [{cols}]\n"));
            }
            Self::Filter { input, predicate } => {
                out.push_str(&format!("{pad}filter {predicate}\n"));
                input.explain_into(depth + 1, out);
            }
            Self::Project { input, exprs } => {
                let cols = exprs
                    .iter()
                    .map(|(e, alias)| {
                        if e.as_column() == Some(alias.as_str()) {
                            alias.clone()
                        } else {
                            format!("{e} as {alias}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!("{pad}project {cols}\n"));
                input.explain_into(depth + 1, out);
            }
            Self::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let aggs = aggregates
                    .iter()
                    .map(|a| {
                        format!(
                            "{}({}) as {}",
                            a.func,
                            a.column.clone().unwrap_or_else(|| "*".to_string()),
                            a.alias
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!(
                    "{pad}aggregate group_by=[{}] {aggs}\n",
                    group_by.join(", ")
                ));
                input.explain_into(depth + 1, out);
            }
            Self::Sort { input, exprs } => {
                let keys = exprs
                    .iter()
                    .map(|s| format!("{} {}", s.column, if s.ascending { "asc" } else { "desc" }))
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!("{pad}sort {keys}\n"));
                input.explain_into(depth + 1, out);
            }
            Self::Limit {
                input,
                limit,
                offset,
            } => {
                out.push_str(&format!("{pad}limit {limit} offset {offset}\n"));
                input.explain_into(depth + 1, out);
            }
            Self::Join {
                left,
                right,
                condition,
            } => {
                out.push_str(&format!("{pad}join on {condition}\n"));
                left.explain_into(depth + 1, out);
                right.explain_into(depth + 1, out);
            }
        }
    }
}

/// One output column of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// The shape a plan produces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSchema {
    pub columns: Vec<ColumnMeta>,
}

impl OutputSchema {
    pub fn new(columns: Vec<ColumnMeta>) -> Self {
        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::Value;

    fn table() -> TableName {
        TableName::new("orders").unwrap()
    }

    #[test]
    fn builders_nest_in_the_expected_order() {
        let q = Query::scan(table())
            .filter(Expr::col("amount").gt(Expr::lit(Value::Int(10))))
            .aggregate(
                vec!["country".to_string()],
                vec![AggregateExpr::new(
                    AggregateFunc::Sum,
                    Some("amount".into()),
                    "revenue",
                )],
            )
            .sort(vec![SortExpr::desc("revenue")])
            .limit(20);
        assert_eq!(q.node_name(), "limit");
        assert_eq!(q.table().unwrap().table, table());
        let explained = q.explain();
        assert!(explained.contains("limit 20"), "{explained}");
        assert!(explained.contains("sum(amount) as revenue"), "{explained}");
        assert!(explained.contains("scan orders [*]"), "{explained}");
    }

    #[test]
    fn aggregate_output_types_follow_the_argument() {
        assert_eq!(AggregateFunc::Count.output_type(None), DataType::Int64);
        assert_eq!(
            AggregateFunc::Sum.output_type(Some(DataType::Int64)),
            DataType::Int64
        );
        assert_eq!(
            AggregateFunc::Sum.output_type(Some(DataType::Float64)),
            DataType::Float64
        );
        assert_eq!(
            AggregateFunc::Avg.output_type(Some(DataType::Int64)),
            DataType::Float64
        );
        assert_eq!(
            AggregateFunc::Max.output_type(Some(DataType::Timestamp)),
            DataType::Timestamp
        );
    }

    #[test]
    fn default_aliases_are_predictable() {
        assert_eq!(
            AggregateExpr::default_alias(AggregateFunc::Sum, Some("amount")),
            "sum_amount"
        );
        assert_eq!(
            AggregateExpr::default_alias(AggregateFunc::Count, None),
            "count"
        );
    }

    #[test]
    fn plans_round_trip_through_json() {
        let q = Query::scan_columns(table(), vec!["id".into()])
            .filter(Expr::col("id").eq(Expr::lit(Value::Int(1))))
            .limit(5);
        let json = serde_json::to_string(&q).unwrap();
        assert_eq!(serde_json::from_str::<Query>(&json).unwrap(), q);
    }
}
