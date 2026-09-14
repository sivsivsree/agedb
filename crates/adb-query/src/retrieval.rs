//! Schema retrieval: the context an LLM sees (see "Natural language" in ARCHITECTURE.md).
//!
//! Two rules shape this:
//!
//! * **Semantics, not just types.** `amount decimal` tells a model almost
//!   nothing; `amount float64 [currency USD, aggregate with sum] "Total order
//!   value before refunds"` tells it what to do with the column.
//! * **Sensitive columns never appear.** A column marked `sensitive` is omitted
//!   entirely, so it cannot be leaked through a prompt, a suggestion or an error
//!   message. An agent that already knows the name can still query it
//!   explicitly, which is an authorization decision, not a prompting one.

use std::sync::Arc;

use adb_core::TableSchema;

/// Compact description of one table.
pub fn table_context(schema: &TableSchema) -> String {
    let mut out = format!("table {}", schema.name);
    if let Some(description) = &schema.description {
        out.push_str(&format!(": {description}"));
    }
    out.push('\n');
    if !schema.primary_key.is_empty() {
        out.push_str(&format!(
            "  primary key: {}\n",
            schema.primary_key.join(", ")
        ));
    }
    for column in &schema.columns {
        if column.sensitive {
            continue;
        }
        let mut notes = Vec::new();
        if let Some(semantic) = &column.semantic_type {
            notes.push(semantic.as_str().to_string());
        }
        if let Some(currency) = &column.currency {
            notes.push(currency.clone());
        }
        if let Some(unit) = &column.unit {
            notes.push(unit.clone());
        }
        if let Some(agg) = column.default_aggregation {
            notes.push(format!("aggregate with {}", agg.as_str()));
        }
        if let Some(reference) = &column.references {
            notes.push(format!(
                "references {}.{}",
                reference.table, reference.column
            ));
        }
        if !column.nullable {
            notes.push("required".to_string());
        }
        out.push_str(&format!("  {} {}", column.name, column.data_type));
        if !notes.is_empty() {
            out.push_str(&format!(" [{}]", notes.join(", ")));
        }
        if let Some(description) = &column.description {
            out.push_str(&format!(": {description}"));
        }
        out.push('\n');
    }
    out
}

/// Context for a whole database.
pub fn schema_context(tables: &[Arc<TableSchema>]) -> String {
    if tables.is_empty() {
        return "(this database has no tables yet)\n".to_string();
    }
    tables
        .iter()
        .map(|t| table_context(t))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::{
        Aggregation, ColumnRef, ColumnSchema, DataType, SemanticType, TableName, TableSchema,
    };

    fn orders() -> TableSchema {
        let mut schema = TableSchema::new(
            TableName::new("orders").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Uuid)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("amount", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum)
                    .described("Total order value before refunds."),
                ColumnSchema::new("customer_email", DataType::Utf8)
                    .semantic(SemanticType::Email)
                    .sensitive(),
                ColumnSchema {
                    references: Some(ColumnRef {
                        table: "customers".into(),
                        column: "id".into(),
                    }),
                    ..ColumnSchema::new("customer_id", DataType::Uuid)
                        .described("Customer who placed the order")
                },
            ],
        )
        .with_primary_key(vec!["id".to_string()]);
        schema.columns[1].currency = Some("USD".to_string());
        schema.description = Some("One row per placed order".to_string());
        schema
    }

    #[test]
    fn context_carries_semantics_units_and_relationships() {
        let text = table_context(&orders());
        assert!(
            text.contains("table orders: One row per placed order"),
            "{text}"
        );
        assert!(text.contains("primary key: id"), "{text}");
        assert!(
            text.contains(
                "amount float64 [currency, USD, aggregate with sum]: Total order value before refunds.",
            ),
            "{text}"
        );
        assert!(text.contains("references customers.id"), "{text}");
        assert!(text.contains("id uuid [id, required]"), "{text}");
    }

    #[test]
    fn sensitive_columns_are_never_described() {
        let text = table_context(&orders());
        assert!(
            !text.contains("customer_email"),
            "sensitive column leaked into context:\n{text}"
        );
    }

    #[test]
    fn empty_databases_say_so_rather_than_producing_nothing() {
        assert!(schema_context(&[]).contains("no tables yet"));
    }

    #[test]
    fn multiple_tables_are_separated() {
        let a = Arc::new(orders());
        let mut second = orders();
        second.name = TableName::new("customers").unwrap();
        let text = schema_context(&[a, Arc::new(second)]);
        assert!(text.contains("table orders"));
        assert!(text.contains("table customers"));
    }
}
