use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::error::{AdbError, Result};
use crate::ids::{validate_ident, TableName};
use crate::types::DataType;

/// Default number of partitions per table (see "Concurrency" in ARCHITECTURE.md). Each partition owns
/// an independent WAL + memtable and is the unit of query parallelism.
pub const DEFAULT_PARTITIONS: u32 = 4;
pub const MAX_PARTITIONS: u32 = 64;
pub const MAX_COLUMNS: usize = 512;

/// What a column *means*, not just how it is stored (see "Natural language" in ARCHITECTURE.md).
///
/// This is the metadata the natural-language layer retrieves to build prompt
/// context, and what lets the planner reject nonsense like `SUM(country)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticType {
    /// Primary or foreign key / opaque identifier. Never aggregated.
    Id,
    /// Monetary amount; pair with `currency`.
    Currency,
    /// Point in time.
    Timestamp,
    /// Low-cardinality label, a natural `GROUP BY` target.
    Category,
    Country,
    Email,
    Url,
    /// Counted or measured quantity; pair with `unit`.
    Quantity,
    /// Bounded numeric score / ratio.
    Score,
    /// Free-form prose.
    Text,
    Boolean,
    Json,
    Other(String),
}

impl SemanticType {
    /// Columns worth a bloom filter in segment metadata: identifiers and
    /// low-cardinality labels are what equality predicates target.
    pub fn wants_bloom_filter(&self) -> bool {
        matches!(
            self,
            Self::Id | Self::Category | Self::Country | Self::Email
        )
    }

    /// True when aggregating this column is almost certainly a mistake.
    pub fn is_aggregation_hostile(&self) -> bool {
        matches!(self, Self::Id | Self::Email | Self::Url | Self::Json)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Id => "id",
            Self::Currency => "currency",
            Self::Timestamp => "timestamp",
            Self::Category => "category",
            Self::Country => "country",
            Self::Email => "email",
            Self::Url => "url",
            Self::Quantity => "quantity",
            Self::Score => "score",
            Self::Text => "text",
            Self::Boolean => "boolean",
            Self::Json => "json",
            Self::Other(s) => s,
        }
    }
}

/// The aggregation that makes sense for a column by default; used by the NL
/// layer when the agent says "revenue by country" without naming a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl Aggregation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

/// A declared relationship: `orders.customer_id -> customers.id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_type: Option<SemanticType>,
    /// e.g. "kg", "requests/sec".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// ISO 4217 code when `semantic_type` is `currency`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_aggregation: Option<Aggregation>,
    /// Sensitive columns are excluded from NL prompt context and from
    /// `SELECT *`-style projections unless named explicitly.
    #[serde(default)]
    pub sensitive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<ColumnRef>,
}

fn default_true() -> bool {
    true
}

impl ColumnSchema {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            description: None,
            semantic_type: None,
            unit: None,
            currency: None,
            default_aggregation: None,
            sensitive: false,
            references: None,
        }
    }

    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn described(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn semantic(mut self, semantic_type: SemanticType) -> Self {
        self.semantic_type = Some(semantic_type);
        self
    }

    pub fn aggregated_by(mut self, agg: Aggregation) -> Self {
        self.default_aggregation = Some(agg);
        self
    }

    pub fn sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    /// Whether segment metadata should carry a bloom filter for this column.
    pub fn wants_bloom_filter(&self) -> bool {
        self.semantic_type
            .as_ref()
            .map(SemanticType::wants_bloom_filter)
            .unwrap_or(false)
            || matches!(self.data_type, DataType::Uuid)
    }

    pub fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.data_type.arrow_type(), self.nullable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: TableName,
    pub columns: Vec<ColumnSchema>,
    /// Empty means append-only: `upsert`, `update` and `delete` are rejected and
    /// no key index is maintained (see "Writes, updates and deletes" in ARCHITECTURE.md).
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// Bumped on every accepted schema change; segments record the version they
    /// were written with.
    #[serde(default)]
    pub version: u32,
}

fn default_partitions() -> u32 {
    DEFAULT_PARTITIONS
}

impl TableSchema {
    pub fn new(name: TableName, columns: Vec<ColumnSchema>) -> Self {
        Self {
            name,
            columns,
            primary_key: Vec::new(),
            description: None,
            partitions: DEFAULT_PARTITIONS,
            version: 1,
        }
    }

    pub fn with_primary_key(mut self, key: Vec<String>) -> Self {
        self.primary_key = key;
        self
    }

    pub fn with_partitions(mut self, partitions: u32) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn require_column(&self, name: &str) -> Result<&ColumnSchema> {
        self.column(name)
            .ok_or_else(|| AdbError::not_found("column", format!("{}.{}", self.name, name)))
    }

    pub fn is_append_only(&self) -> bool {
        self.primary_key.is_empty()
    }

    /// Indices of the primary-key columns, in key order.
    pub fn pk_indices(&self) -> Vec<usize> {
        self.primary_key
            .iter()
            .filter_map(|k| self.column_index(k))
            .collect()
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(ArrowSchema::new(
            self.columns
                .iter()
                .map(|c| c.arrow_field())
                .collect::<Vec<_>>(),
        ))
    }

    /// The default projection for `SELECT *`: everything except sensitive columns.
    pub fn default_projection(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| !c.sensitive)
            .map(|c| c.name.clone())
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(AdbError::InvalidSchema(
                "table must have at least one column".into(),
            ));
        }
        if self.columns.len() > MAX_COLUMNS {
            return Err(AdbError::InvalidSchema(format!(
                "table may have at most {MAX_COLUMNS} columns"
            )));
        }
        if self.partitions == 0 || self.partitions > MAX_PARTITIONS {
            return Err(AdbError::InvalidSchema(format!(
                "partitions must be between 1 and {MAX_PARTITIONS}, got {}",
                self.partitions
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for col in &self.columns {
            validate_ident("column", &col.name)?;
            if !seen.insert(col.name.as_str()) {
                return Err(AdbError::InvalidSchema(format!(
                    "duplicate column {:?}",
                    col.name
                )));
            }
            if col.currency.is_some() && col.semantic_type != Some(SemanticType::Currency) {
                return Err(AdbError::InvalidSchema(format!(
                    "column {:?} sets `currency` without semantic_type=currency",
                    col.name
                )));
            }
            if let Some(agg) = col.default_aggregation {
                let ok = match agg {
                    Aggregation::Count => true,
                    Aggregation::Sum | Aggregation::Avg => col.data_type.is_additive(),
                    Aggregation::Min | Aggregation::Max => col.data_type.is_ordered(),
                };
                if !ok {
                    return Err(AdbError::InvalidSchema(format!(
                        "column {:?} of type {} cannot default to aggregation {}",
                        col.name,
                        col.data_type,
                        agg.as_str()
                    )));
                }
            }
        }
        let mut pk_seen = std::collections::HashSet::new();
        for key in &self.primary_key {
            let col = self.column(key).ok_or_else(|| {
                AdbError::InvalidSchema(format!("primary key column {key:?} is not defined"))
            })?;
            if !pk_seen.insert(key.as_str()) {
                return Err(AdbError::InvalidSchema(format!(
                    "duplicate primary key column {key:?}"
                )));
            }
            if col.nullable {
                return Err(AdbError::InvalidSchema(format!(
                    "primary key column {key:?} must be non-nullable"
                )));
            }
            if matches!(col.data_type, DataType::Float64 | DataType::Json) {
                return Err(AdbError::InvalidSchema(format!(
                    "primary key column {key:?} may not be of type {}",
                    col.data_type
                )));
            }
        }
        Ok(())
    }

    /// Check that a proposed replacement is a legal evolution of this schema.
    ///
    /// v0.1 allows adding nullable columns and editing semantic metadata. It
    /// does not allow dropping or retyping columns, or changing the primary key
    /// or partition count, because segments already on disk encode those.
    pub fn check_evolution(&self, next: &TableSchema) -> Result<()> {
        next.validate()?;
        if next.name != self.name {
            return Err(AdbError::InvalidSchema("table name cannot change".into()));
        }
        if next.primary_key != self.primary_key {
            return Err(AdbError::Unsupported("changing the primary key".into()));
        }
        if next.partitions != self.partitions {
            return Err(AdbError::Unsupported("changing the partition count".into()));
        }
        for existing in &self.columns {
            match next.column(&existing.name) {
                None => {
                    return Err(AdbError::Unsupported(format!(
                        "dropping column {:?}",
                        existing.name
                    )))
                }
                Some(updated) => {
                    if updated.data_type != existing.data_type {
                        return Err(AdbError::Unsupported(format!(
                            "changing the type of column {:?} ({} -> {})",
                            existing.name, existing.data_type, updated.data_type
                        )));
                    }
                    if updated.nullable != existing.nullable {
                        return Err(AdbError::Unsupported(format!(
                            "changing nullability of column {:?}",
                            existing.name
                        )));
                    }
                }
            }
        }
        for added in &next.columns {
            if self.column(&added.name).is_none() && !added.nullable {
                return Err(AdbError::InvalidSchema(format!(
                    "added column {:?} must be nullable",
                    added.name
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> TableSchema {
        TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Uuid)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
                ColumnSchema::new("amount", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum),
            ],
        )
        .with_primary_key(vec!["id".to_string()])
    }

    #[test]
    fn valid_table_passes() {
        table().validate().unwrap();
    }

    #[test]
    fn arrow_schema_matches_columns() {
        let s = table().arrow_schema();
        assert_eq!(s.fields().len(), 3);
        assert_eq!(s.field(0).name(), "id");
        assert!(!s.field(0).is_nullable());
        assert_eq!(s.field(2).data_type(), &arrow::datatypes::DataType::Float64);
    }

    #[test]
    fn rejects_duplicate_and_bad_columns() {
        let mut t = table();
        t.columns.push(ColumnSchema::new("country", DataType::Utf8));
        assert!(t.validate().is_err());

        let mut t = table();
        t.columns
            .push(ColumnSchema::new("Bad Name", DataType::Utf8));
        assert!(t.validate().is_err());
    }

    #[test]
    fn rejects_nullable_or_float_primary_key() {
        let mut t = table();
        t.columns[0].nullable = true;
        assert!(t.validate().is_err());

        let t = TableSchema::new(
            TableName::new("t").unwrap(),
            vec![ColumnSchema::new("k", DataType::Float64).required()],
        )
        .with_primary_key(vec!["k".to_string()]);
        assert!(t.validate().is_err());
    }

    #[test]
    fn rejects_impossible_default_aggregation() {
        let mut t = table();
        t.columns[1].default_aggregation = Some(Aggregation::Sum);
        assert!(t.validate().is_err());
    }

    #[test]
    fn sensitive_columns_are_hidden_from_default_projection() {
        let mut t = table();
        t.columns[1].sensitive = true;
        assert_eq!(
            t.default_projection(),
            vec!["id".to_string(), "amount".to_string()]
        );
    }

    #[test]
    fn evolution_allows_adding_nullable_columns_only() {
        let base = table();

        let mut ok = base.clone();
        ok.columns.push(ColumnSchema::new("note", DataType::Utf8));
        base.check_evolution(&ok).unwrap();

        let mut required = base.clone();
        required
            .columns
            .push(ColumnSchema::new("note", DataType::Utf8).required());
        assert!(base.check_evolution(&required).is_err());

        let mut dropped = base.clone();
        dropped.columns.remove(1);
        assert!(base.check_evolution(&dropped).is_err());

        let mut retyped = base.clone();
        retyped.columns[2].data_type = DataType::Int64;
        assert!(base.check_evolution(&retyped).is_err());

        let mut repartitioned = base.clone();
        repartitioned.partitions = 8;
        assert!(base.check_evolution(&repartitioned).is_err());
    }

    #[test]
    fn metadata_edits_are_allowed_by_evolution() {
        let base = table();
        let mut next = base.clone();
        next.columns[1].description = Some("ISO country of the buyer".into());
        next.columns[1].sensitive = true;
        base.check_evolution(&next).unwrap();
    }
}
