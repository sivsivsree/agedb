//! The structured plan: what an LLM or an MCP client is allowed to send.
//!
//! This is deliberately *not* the Query IR. It is a flat, JSON-schema-able
//! request shape (see "Natural language" in ARCHITECTURE.md) that maps onto the section 18 feature set
//! and nothing more, so the surface an LLM can hallucinate against is small.
//! [`PlanRequest::to_ir`] turns it into IR; the validator then decides whether it
//! is legal.

use adb_core::{AdbError, Result, TableName, Value};
use adb_planner::{AggregateExpr, AggregateFunc, BinaryOp, Expr, Query, SortExpr};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Return matching rows.
    #[default]
    Select,
    /// Return grouped metrics.
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
    Between,
    Like,
    IsNull,
    IsNotNull,
}

impl FilterOp {
    fn binary(self) -> Option<BinaryOp> {
        Some(match self {
            Self::Eq => BinaryOp::Eq,
            Self::Ne => BinaryOp::NotEq,
            Self::Lt => BinaryOp::Lt,
            Self::Lte => BinaryOp::LtEq,
            Self::Gt => BinaryOp::Gt,
            Self::Gte => BinaryOp::GtEq,
            Self::Like => BinaryOp::Like,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::In => "in",
            Self::NotIn => "not_in",
            Self::Between => "between",
            Self::Like => "like",
            Self::IsNull => "is_null",
            Self::IsNotNull => "is_not_null",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterSpec {
    pub column: String,
    pub op: FilterOp,
    /// Single operand, for the comparison operators.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Json>,
    /// Operand list, for `in`, `not_in` and `between` (exactly two).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Json>,
}

impl FilterSpec {
    pub fn new(column: impl Into<String>, op: FilterOp, value: Json) -> Self {
        Self {
            column: column.into(),
            op,
            value: Some(value),
            values: Vec::new(),
        }
    }

    pub fn unary(column: impl Into<String>, op: FilterOp) -> Self {
        Self {
            column: column.into(),
            op,
            value: None,
            values: Vec::new(),
        }
    }

    pub fn list(column: impl Into<String>, op: FilterOp, values: Vec<Json>) -> Self {
        Self {
            column: column.into(),
            op,
            value: None,
            values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSpec {
    pub function: AggregateFunc,
    /// Omitted only for `count`, which then counts rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl MetricSpec {
    pub fn new(function: AggregateFunc, column: Option<&str>, alias: Option<&str>) -> Self {
        Self {
            function,
            column: column.map(str::to_string),
            alias: alias.map(str::to_string),
        }
    }

    pub fn count() -> Self {
        Self {
            function: AggregateFunc::Count,
            column: None,
            alias: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSpec {
    pub column: String,
    #[serde(default)]
    pub direction: Direction,
}

impl OrderSpec {
    pub fn desc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: Direction::Desc,
        }
    }

    pub fn asc(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: Direction::Asc,
        }
    }
}

/// A complete query request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PlanRequest {
    #[serde(default)]
    pub operation: Operation,
    pub table: String,
    /// Columns to return for a `select`. Omitted means every non-sensitive one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<FilterSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<MetricSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

impl PlanRequest {
    pub fn select(table: impl Into<String>) -> Self {
        Self {
            operation: Operation::Select,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn aggregate(table: impl Into<String>) -> Self {
        Self {
            operation: Operation::Aggregate,
            table: table.into(),
            ..Default::default()
        }
    }

    pub fn with_filter(mut self, filter: FilterSpec) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn with_metric(mut self, metric: MetricSpec) -> Self {
        self.metrics.push(metric);
        self
    }

    pub fn grouped_by(mut self, column: impl Into<String>) -> Self {
        self.group_by.push(column.into());
        self
    }

    pub fn ordered_by(mut self, order: OrderSpec) -> Self {
        self.order_by.push(order);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Lower the request into the Query IR.
    ///
    /// Type coercion of filter values is *not* done here: values stay JSON and
    /// become untyped `Value`s, which the validator coerces against the real
    /// column type. That keeps a single place responsible for type decisions.
    pub fn to_ir(&self) -> Result<Query> {
        let table = TableName::new(self.table.clone())?;
        let mut query = match (&self.columns, self.operation) {
            (Some(columns), Operation::Select) if !columns.is_empty() => {
                Query::scan_columns(table, columns.clone())
            }
            _ => Query::scan(table),
        };

        if let Some(predicate) = self.predicate()? {
            query = query.filter(predicate);
        }

        match self.operation {
            Operation::Aggregate => {
                if self.metrics.is_empty() {
                    return Err(AdbError::bad_request(
                        "an aggregate needs at least one metric, e.g. {\"function\":\"count\"}",
                    ));
                }
                let aggregates = self
                    .metrics
                    .iter()
                    .map(|metric| {
                        let alias = metric.alias.clone().unwrap_or_else(|| {
                            AggregateExpr::default_alias(metric.function, metric.column.as_deref())
                        });
                        AggregateExpr::new(metric.function, metric.column.clone(), alias)
                    })
                    .collect();
                query = query.aggregate(self.group_by.clone(), aggregates);
            }
            Operation::Select => {
                if !self.metrics.is_empty() || !self.group_by.is_empty() {
                    return Err(AdbError::bad_request(
                        "metrics and group_by need \"operation\": \"aggregate\"",
                    ));
                }
            }
        }

        if !self.order_by.is_empty() {
            query = query.sort(
                self.order_by
                    .iter()
                    .map(|order| match order.direction {
                        Direction::Asc => SortExpr::asc(&order.column),
                        Direction::Desc => SortExpr::desc(&order.column),
                    })
                    .collect(),
            );
        }

        if let Some(limit) = self.limit {
            query = query.limit_offset(limit, self.offset.unwrap_or(0));
        } else if let Some(offset) = self.offset.filter(|o| *o > 0) {
            return Err(AdbError::bad_request(format!(
                "offset {offset} needs a limit as well"
            )));
        }
        Ok(query)
    }

    fn predicate(&self) -> Result<Option<Expr>> {
        let mut parts = Vec::with_capacity(self.filters.len());
        for filter in &self.filters {
            parts.push(filter_to_expr(filter)?);
        }
        Ok(Expr::all(parts))
    }

    /// JSON Schema for this shape, used for the MCP tool contract so a calling
    /// agent can emit a plan directly.
    pub fn json_schema() -> Json {
        json!({
            "type": "object",
            "required": ["table"],
            "additionalProperties": false,
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["select", "aggregate"],
                    "description": "select returns rows; aggregate returns grouped metrics",
                    "default": "select"
                },
                "table": { "type": "string", "description": "table to read" },
                "columns": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "columns to return for a select; omit for all non-sensitive columns"
                },
                "filters": {
                    "type": "array",
                    "description": "conditions, combined with AND",
                    "items": {
                        "type": "object",
                        "required": ["column", "op"],
                        "additionalProperties": false,
                        "properties": {
                            "column": { "type": "string" },
                            "op": {
                                "type": "string",
                                "enum": [
                                    "eq", "ne", "lt", "lte", "gt", "gte",
                                    "in", "not_in", "between", "like",
                                    "is_null", "is_not_null"
                                ]
                            },
                            "value": {
                                "description": "operand for the comparison operators; dates as ISO-8601 strings"
                            },
                            "values": {
                                "type": "array",
                                "description": "operands for in, not_in and between (exactly two)"
                            }
                        }
                    }
                },
                "group_by": { "type": "array", "items": { "type": "string" } },
                "metrics": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["function"],
                        "additionalProperties": false,
                        "properties": {
                            "function": {
                                "type": "string",
                                "enum": ["count", "sum", "avg", "min", "max"]
                            },
                            "column": {
                                "type": "string",
                                "description": "omit only for count, which counts rows"
                            },
                            "alias": { "type": "string" }
                        }
                    }
                },
                "order_by": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["column"],
                        "additionalProperties": false,
                        "properties": {
                            "column": { "type": "string" },
                            "direction": { "type": "string", "enum": ["asc", "desc"], "default": "asc" }
                        }
                    }
                },
                "limit": { "type": "integer", "minimum": 1 },
                "offset": { "type": "integer", "minimum": 0 }
            }
        })
    }
}

fn json_to_value(json: &Json) -> Result<Value> {
    Ok(match json {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Value::Int(i),
            None => Value::Float(
                n.as_f64()
                    .ok_or_else(|| AdbError::bad_request(format!("{n} is not a usable number")))?,
            ),
        },
        Json::String(s) => Value::Str(s.clone()),
        other => {
            return Err(AdbError::bad_request(format!(
                "filter values must be scalars, got {other}"
            )))
        }
    })
}

fn filter_to_expr(filter: &FilterSpec) -> Result<Expr> {
    let column = Expr::col(&filter.column);
    let single = |what: &str| -> Result<Value> {
        let value = filter.value.as_ref().ok_or_else(|| {
            AdbError::bad_request(format!(
                "filter on {:?} with op {what} needs a \"value\"",
                filter.column
            ))
        })?;
        json_to_value(value)
    };

    if let Some(op) = filter.op.binary() {
        return Ok(Expr::binary(
            op,
            column,
            Expr::lit(single(filter.op.as_str())?),
        ));
    }
    Ok(match filter.op {
        FilterOp::IsNull => Expr::IsNull(Box::new(column)),
        FilterOp::IsNotNull => Expr::IsNotNull(Box::new(column)),
        FilterOp::In | FilterOp::NotIn => {
            if filter.values.is_empty() {
                return Err(AdbError::bad_request(format!(
                    "filter on {:?} with op {} needs \"values\"",
                    filter.column,
                    filter.op.as_str()
                )));
            }
            Expr::InList {
                expr: Box::new(column),
                list: filter
                    .values
                    .iter()
                    .map(json_to_value)
                    .collect::<Result<Vec<_>>>()?,
                negated: filter.op == FilterOp::NotIn,
            }
        }
        FilterOp::Between => {
            if filter.values.len() != 2 {
                return Err(AdbError::bad_request(format!(
                    "filter on {:?} with op between needs exactly two \"values\"",
                    filter.column
                )));
            }
            Expr::Between {
                expr: Box::new(column),
                low: json_to_value(&filter.values[0])?,
                high: json_to_value(&filter.values[1])?,
                negated: false,
            }
        }
        other => {
            return Err(AdbError::Internal(format!(
                "filter op {} was not lowered",
                other.as_str()
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{ColumnSchema, DataType, QueryLimits, SemanticType, TableSchema};

    fn orders() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("amount", DataType::Float64).semantic(SemanticType::Currency),
                ColumnSchema::new("created_at", DataType::Timestamp),
            ],
        )
    }

    fn validated(request: &PlanRequest) -> adb_core::Result<adb_planner::ValidatedQuery> {
        let ir = request.to_ir()?;
        adb_planner::validate(&ir, &orders(), &QueryLimits::unlimited())
    }

    #[test]
    fn the_readme_example_plan_lowers_and_validates() {
        // The exact shape from see "Natural language" in ARCHITECTURE.md.
        let raw = json!({
            "operation": "aggregate",
            "table": "orders",
            "group_by": ["country"],
            "metrics": [{ "function": "sum", "column": "amount", "alias": "revenue" }],
            "order_by": [{ "column": "revenue", "direction": "desc" }],
            "limit": 20
        });
        let request: PlanRequest = serde_json::from_value(raw).unwrap();
        let v = validated(&request).unwrap();
        assert_eq!(v.schema.names(), vec!["country", "revenue"]);
        let explained = v.query.explain();
        assert!(explained.contains("limit 20"), "{explained}");
        assert!(explained.contains("sum(amount) as revenue"), "{explained}");
    }

    #[test]
    fn filter_operators_all_lower_to_ir() {
        let request = PlanRequest::select("orders")
            .with_filter(FilterSpec::new("amount", FilterOp::Gte, json!(100)))
            .with_filter(FilterSpec::new("country", FilterOp::Like, json!("u%")))
            .with_filter(FilterSpec::list(
                "id",
                FilterOp::In,
                vec![json!(1), json!(2)],
            ))
            .with_filter(FilterSpec::list(
                "created_at",
                FilterOp::Between,
                vec![json!("2026-01-01"), json!("2026-02-01")],
            ))
            .with_filter(FilterSpec::unary("country", FilterOp::IsNotNull));
        let v = validated(&request).unwrap();
        let adb_planner::Query::Filter { predicate, .. } = &v.query else {
            panic!("expected a filter, got {}", v.query.explain())
        };
        assert_eq!(predicate.conjuncts().len(), 5);
    }

    #[test]
    fn values_are_coerced_by_the_validator_not_here() {
        let request = PlanRequest::select("orders").with_filter(FilterSpec::new(
            "created_at",
            FilterOp::Gt,
            json!("2026-01-01"),
        ));
        // Untyped in the IR...
        let ir = request.to_ir().unwrap();
        assert!(ir.explain().contains("2026-01-01"));
        // ...and a real timestamp after validation.
        let v = validated(&request).unwrap();
        assert!(
            v.query.explain().contains("2026-01-01T00:00:00.000000Z"),
            "{}",
            v.query.explain()
        );
    }

    #[test]
    fn malformed_filters_are_rejected_with_actionable_messages() {
        let missing_value =
            PlanRequest::select("orders").with_filter(FilterSpec::unary("amount", FilterOp::Gt));
        let err = validated(&missing_value).unwrap_err();
        assert!(err.to_string().contains("needs a \"value\""), "{err}");

        let bad_between = PlanRequest::select("orders").with_filter(FilterSpec::list(
            "amount",
            FilterOp::Between,
            vec![json!(1)],
        ));
        assert!(validated(&bad_between)
            .unwrap_err()
            .to_string()
            .contains("exactly two"));

        let empty_in =
            PlanRequest::select("orders").with_filter(FilterSpec::list("id", FilterOp::In, vec![]));
        assert!(validated(&empty_in)
            .unwrap_err()
            .to_string()
            .contains("needs \"values\""));
    }

    #[test]
    fn hallucinated_columns_and_tables_become_errors() {
        let bad_column = PlanRequest::select("orders").with_filter(FilterSpec::new(
            "revenue_usd",
            FilterOp::Gt,
            json!(1),
        ));
        assert_eq!(validated(&bad_column).unwrap_err().code(), "not_found");

        let bad_table = PlanRequest::select("Orders");
        assert_eq!(
            validated(&bad_table).unwrap_err().code(),
            "invalid_identifier"
        );
    }

    #[test]
    fn metrics_without_aggregate_operation_are_refused() {
        let mut request = PlanRequest::select("orders");
        request.metrics.push(MetricSpec::count());
        let err = validated(&request).unwrap_err();
        assert!(err.to_string().contains("\"aggregate\""), "{err}");

        let empty = PlanRequest::aggregate("orders");
        assert!(validated(&empty)
            .unwrap_err()
            .to_string()
            .contains("at least one metric"));
    }

    #[test]
    fn offset_without_limit_is_refused() {
        let mut request = PlanRequest::select("orders");
        request.offset = Some(10);
        assert!(validated(&request)
            .unwrap_err()
            .to_string()
            .contains("needs a limit"));
    }

    #[test]
    fn default_metric_aliases_are_used_when_omitted() {
        let request = PlanRequest::aggregate("orders")
            .with_metric(MetricSpec::new(AggregateFunc::Sum, Some("amount"), None))
            .with_metric(MetricSpec::count());
        let v = validated(&request).unwrap();
        assert_eq!(v.schema.names(), vec!["sum_amount", "count"]);
    }

    #[test]
    fn the_json_schema_describes_the_whole_surface() {
        let schema = PlanRequest::json_schema();
        let properties = schema["properties"].as_object().unwrap();
        for key in [
            "operation",
            "table",
            "columns",
            "filters",
            "group_by",
            "metrics",
            "order_by",
            "limit",
            "offset",
        ] {
            assert!(
                properties.contains_key(key),
                "{key} missing from the plan schema"
            );
        }
        // Closed shape: an LLM inventing a field gets a validation error rather
        // than having it silently ignored.
        assert_eq!(schema["additionalProperties"], json!(false));
    }

    #[test]
    fn plans_round_trip_through_json() {
        let request = PlanRequest::aggregate("orders")
            .grouped_by("country")
            .with_metric(MetricSpec::new(
                AggregateFunc::Sum,
                Some("amount"),
                Some("revenue"),
            ))
            .ordered_by(OrderSpec::desc("revenue"))
            .with_limit(20);
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(
            serde_json::from_value::<PlanRequest>(json).unwrap(),
            request
        );
    }
}
