use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{AdbError, Result};
use crate::ids::{DatabaseName, RequestId, TenantId, UserId};

/// Capability scopes (see "Guardrails" in ARCHITECTURE.md). There is deliberately no
/// `execute_arbitrary_query` scope: the only way in is a validated plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    DatabaseRead,
    DatabaseWrite,
    SchemaRead,
    SchemaWrite,
    DataInsert,
    DataUpdate,
    DataDelete,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DatabaseRead => "database:read",
            Self::DatabaseWrite => "database:write",
            Self::SchemaRead => "schema:read",
            Self::SchemaWrite => "schema:write",
            Self::DataInsert => "data:insert",
            Self::DataUpdate => "data:update",
            Self::DataDelete => "data:delete",
        }
    }

    pub const ALL: [Scope; 7] = [
        Scope::DatabaseRead,
        Scope::DatabaseWrite,
        Scope::SchemaRead,
        Scope::SchemaWrite,
        Scope::DataInsert,
        Scope::DataUpdate,
        Scope::DataDelete,
    ];

    /// Read-only scope set, for agents that must never mutate anything.
    pub fn read_only() -> BTreeSet<Scope> {
        [Scope::DatabaseRead, Scope::SchemaRead]
            .into_iter()
            .collect()
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Scope {
    type Err = AdbError;
    fn from_str(s: &str) -> Result<Self> {
        Scope::ALL
            .into_iter()
            .find(|scope| scope.as_str() == s)
            .ok_or_else(|| AdbError::bad_request(format!("unknown scope {s:?}")))
    }
}

/// Hard ceilings applied *inside* the executor, not at the edge, so a bad plan
/// cannot burn the node (see "Guardrails" in ARCHITECTURE.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryLimits {
    /// Maximum rows returned to the caller.
    pub max_rows: usize,
    /// Maximum bytes read from memtables + segments while executing.
    pub max_bytes_scanned: u64,
    /// Wall-clock budget for a single query.
    pub max_execution_time_ms: u64,
    /// Maximum rows accepted in one insert/upsert call.
    pub max_write_rows: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_bytes_scanned: 4 << 30,
            max_execution_time_ms: 30_000,
            max_write_rows: 1_000_000,
        }
    }
}

impl QueryLimits {
    /// Limits for trusted local use (benchmarks, tests).
    pub fn unlimited() -> Self {
        Self {
            max_rows: usize::MAX,
            max_bytes_scanned: u64::MAX,
            max_execution_time_ms: u64::MAX,
            max_write_rows: usize::MAX,
        }
    }

    /// Narrow these limits to `requested`, never widen them.
    pub fn clamp_to(&self, requested: &QueryLimits) -> QueryLimits {
        QueryLimits {
            max_rows: self.max_rows.min(requested.max_rows),
            max_bytes_scanned: self.max_bytes_scanned.min(requested.max_bytes_scanned),
            max_execution_time_ms: self
                .max_execution_time_ms
                .min(requested.max_execution_time_ms),
            max_write_rows: self.max_write_rows.min(requested.max_write_rows),
        }
    }
}

/// Accompanies every request through every layer (see "Multi-tenancy" in ARCHITECTURE.md).
///
/// Tenancy is not an application-level filter: it selects the storage prefix, so
/// a missing check cannot leak another tenant's rows.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub tenant: TenantId,
    pub database: Option<DatabaseName>,
    pub user: UserId,
    pub request_id: RequestId,
    pub scopes: BTreeSet<Scope>,
    pub limits: QueryLimits,
}

impl RequestContext {
    pub fn new(tenant: TenantId, user: UserId, scopes: BTreeSet<Scope>) -> Self {
        Self {
            tenant,
            database: None,
            user,
            request_id: RequestId::new(),
            scopes,
            limits: QueryLimits::default(),
        }
    }

    /// Full-access local context, for tests, the CLI and benchmarks.
    pub fn root(tenant: &str) -> Self {
        Self {
            tenant: TenantId::new(tenant).expect("valid tenant id"),
            database: None,
            user: UserId("root".to_string()),
            request_id: RequestId::new(),
            scopes: Scope::ALL.into_iter().collect(),
            limits: QueryLimits::unlimited(),
        }
    }

    pub fn with_database(mut self, database: DatabaseName) -> Self {
        self.database = Some(database);
        self
    }

    pub fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn has(&self, scope: Scope) -> bool {
        self.scopes.contains(&scope)
    }

    pub fn require(&self, scope: Scope) -> Result<()> {
        if self.has(scope) {
            Ok(())
        } else {
            Err(AdbError::PermissionDenied(scope.as_str().to_string()))
        }
    }

    /// The database named by the request, or an error if the caller omitted it.
    pub fn require_database(&self) -> Result<&DatabaseName> {
        self.database
            .as_ref()
            .ok_or_else(|| AdbError::bad_request("no database selected"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_round_trip_through_strings() {
        for scope in Scope::ALL {
            assert_eq!(Scope::from_str(scope.as_str()).unwrap(), scope);
        }
        assert!(Scope::from_str("database:everything").is_err());
        assert!(Scope::from_str("execute_arbitrary_query").is_err());
    }

    #[test]
    fn require_reports_the_missing_scope() {
        let ctx = RequestContext::new(
            TenantId::new("t1").unwrap(),
            UserId("agent".into()),
            Scope::read_only(),
        );
        assert!(ctx.require(Scope::DatabaseRead).is_ok());
        let err = ctx.require(Scope::DataInsert).unwrap_err();
        assert_eq!(err.code(), "permission_denied");
        assert!(err.to_string().contains("data:insert"));
    }

    #[test]
    fn clamp_never_widens_limits() {
        let configured = QueryLimits {
            max_rows: 100,
            ..QueryLimits::default()
        };
        let requested = QueryLimits {
            max_rows: 1_000_000,
            ..QueryLimits::default()
        };
        assert_eq!(configured.clamp_to(&requested).max_rows, 100);
        let requested = QueryLimits {
            max_rows: 10,
            ..QueryLimits::default()
        };
        assert_eq!(configured.clamp_to(&requested).max_rows, 10);
    }
}
