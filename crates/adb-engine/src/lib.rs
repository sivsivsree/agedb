//! The engine: one object that owns the catalog, every table's storage, and the
//! query path.
//!
//! This is the seam the MCP gateway, the REST API, the CLI and the benchmark
//! harness all sit on, so the guarantees live here rather than being repeated at
//! each edge:
//!
//! * **Every entry point takes a [`RequestContext`]**: tenant, database, user,
//!   request id, scopes, limits (see "Multi-tenancy" in ARCHITECTURE.md). Tenancy selects the storage
//!   prefix; it is never an application-level filter applied after the fact.
//! * **DDL goes through the log.** Catalog changes are validated against the
//!   current snapshot, appended to the system WAL, and only then applied, so the
//!   catalog is reconstructible by replay.
//! * **Queries only run as validated plans.** There is no path from caller input
//!   to the executor that skips `adb_planner::validate`.
//!
//! The API is synchronous. Async callers wrap it in `spawn_blocking`; see
//! `adb-api`.

pub mod engine;

pub use engine::{Engine, EngineConfig, QueryOutcome, QuerySource, TableStats};
