//! Agent-facing query front ends.
//!
//! Two ways in, one destination (see "Design rationale" in ARCHITECTURE.md, 3 and 11):
//!
//! ```text
//! natural language --translate--> PlanRequest (JSON) --to_ir--> Query IR --validate--> execution
//! structured plan  ------------------^
//! ```
//!
//! No language model sits in the query path. Natural language is parsed by
//! deterministic rules into a [`plan::PlanRequest`], which is plain data: it is turned into IR and then handed to the validator,
//! which is where column existence, types and permissions are decided. A
//! hallucinated column or a nonsensical aggregation becomes an error message the
//! agent can act on, not a bad query.
//!
//! [`rule::RuleTranslator`] runs in-process, needs no network and no API key,
//! and answers in microseconds. A hand-written plan and a translated one pass
//! through the same validation.

pub mod plan;
pub mod retrieval;
pub mod rule;
pub mod translate;

pub use plan::{FilterOp, FilterSpec, MetricSpec, Operation, OrderSpec, PlanRequest};
pub use retrieval::{schema_context, table_context};
pub use rule::RuleTranslator;
pub use translate::{IntentTranslator, Translation, TranslationContext};
