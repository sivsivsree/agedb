//! Agent-facing query front ends.
//!
//! Two ways in, one destination (see "Design rationale" in ARCHITECTURE.md, 3 and 11):
//!
//! ```text
//! natural language --translate--> PlanRequest (JSON) --to_ir--> Query IR --validate--> execution
//! structured plan  ------------------^
//! ```
//!
//! The LLM never emits executable operations. It emits a [`plan::PlanRequest`],
//! which is plain data: it is turned into IR and then handed to the validator,
//! which is where column existence, types and permissions are decided. A
//! hallucinated column or a nonsensical aggregation becomes an error message the
//! agent can act on, not a bad query.
//!
//! [`rule::RuleTranslator`] is a deterministic translator that needs no network
//! and no API key; [`anthropic::AnthropicTranslator`] uses Claude with a JSON
//! schema when one is configured. Both produce the same `PlanRequest` and pass
//! through the same validation.

pub mod anthropic;
pub mod plan;
pub mod retrieval;
pub mod rule;
pub mod translate;

pub use anthropic::AnthropicTranslator;
pub use plan::{FilterOp, FilterSpec, MetricSpec, Operation, OrderSpec, PlanRequest};
pub use retrieval::{schema_context, table_context};
pub use rule::RuleTranslator;
pub use translate::{IntentTranslator, Translation, TranslationContext};
