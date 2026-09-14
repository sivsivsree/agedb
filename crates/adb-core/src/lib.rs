//! Core types shared by every AgenticDB layer.
//!
//! Nothing in this crate touches I/O: it defines identifiers, the type system,
//! the semantic schema, the versioned catalog, and the per-request context that
//! carries tenancy and permissions (see "Multi-tenancy" in ARCHITECTURE.md, 10, 12).

pub mod catalog;
pub mod context;
pub mod error;
pub mod ids;
pub mod schema;
pub mod types;

pub use catalog::{Catalog, CatalogSnapshot, DatabaseMeta, DdlMutation};
pub use context::{QueryLimits, RequestContext, Scope};
pub use error::{AdbError, Result};
pub use ids::{DatabaseName, RequestId, TableName, TenantId, UserId};
pub use schema::{Aggregation, ColumnRef, ColumnSchema, SemanticType, TableSchema};
pub use types::{DataType, Value};
