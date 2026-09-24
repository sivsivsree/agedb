//! The translator interface.
//!
//! One trait, implemented by the deterministic [`crate::RuleTranslator`]. A
//! translator must return a [`PlanRequest`], and its output is not trusted: it
//! feeds the same validator as a hand-written plan.

use std::sync::Arc;

use adb_core::{AdbError, Result, TableName, TableSchema};
use chrono::{DateTime, Utc};

use crate::plan::PlanRequest;

/// What a translator gets to look at.
#[derive(Debug, Clone)]
pub struct TranslationContext {
    /// Schemas of the tables in scope, with semantic metadata.
    pub tables: Vec<Arc<TableSchema>>,
    /// Table to assume when the request does not name one.
    pub default_table: Option<TableName>,
    /// "Now" for relative time expressions ("last 90 days"). Injected so
    /// translation is reproducible in tests and logs.
    pub now: DateTime<Utc>,
}

impl TranslationContext {
    pub fn new(tables: Vec<Arc<TableSchema>>) -> Self {
        Self {
            tables,
            default_table: None,
            now: Utc::now(),
        }
    }

    pub fn with_default_table(mut self, table: TableName) -> Self {
        self.default_table = Some(table);
        self
    }

    pub fn at(mut self, now: DateTime<Utc>) -> Self {
        self.now = now;
        self
    }

    pub fn table(&self, name: &str) -> Option<&Arc<TableSchema>> {
        self.tables.iter().find(|t| t.name.as_str() == name)
    }

    pub fn require_tables(&self) -> Result<()> {
        if self.tables.is_empty() {
            return Err(AdbError::bad_request(
                "this database has no tables yet, so there is nothing to query",
            ));
        }
        Ok(())
    }
}

/// The result of translating a request.
#[derive(Debug, Clone)]
pub struct Translation {
    pub plan: PlanRequest,
    /// Which translator produced it, for logging and for telling an agent how
    /// its request was interpreted.
    pub translator: &'static str,
    /// Plain-language restatement of what was understood.
    pub interpretation: String,
}

pub trait IntentTranslator: Send + Sync {
    fn name(&self) -> &'static str;

    /// Turn a natural-language request into a structured plan.
    fn translate(&self, request: &str, context: &TranslationContext) -> Result<Translation>;
}
