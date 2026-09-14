//! The Query IR and everything that turns it into an executable plan.
//!
//! The IR is the central abstraction of the whole system (see "The Query IR is the centre" in ARCHITECTURE.md): MCP
//! tools, natural language and, later, SQL all converge here, and only the IR
//! reaches the executor. Nothing downstream of [`validate`] trusts caller input:
//! a plan that leaves the validator has had its column references resolved, its
//! literals coerced to column types, and its aggregations checked for sense.
//!
//! ```text
//! Query (logical IR)  --validate-->  Query + OutputSchema  --optimize-->  PhysicalPlan
//! ```

pub mod expr;
pub mod ir;
pub mod optimize;
pub mod physical;
pub mod validate;

pub use expr::{BinaryOp, Expr};
pub use ir::{AggregateExpr, AggregateFunc, ColumnMeta, OutputSchema, Query, SortExpr, TableRef};
pub use optimize::{PruneAtom, PruneOp};
pub use physical::{AggregateSpec, PhysicalPlan};
pub use validate::{validate, ValidatedQuery};
