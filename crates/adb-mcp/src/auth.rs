//! API keys, scopes and per-key limits (see "Multi-tenancy" and "Guardrails" in ARCHITECTURE.md).
//!
//! A key is not just an identity: it carries the tenant, the scope set, and the
//! query budget. So "this agent may read but never write, and never more than
//! 1000 rows" is a configuration decision, not something each tool has to
//! remember to check.

use std::collections::BTreeSet;

use adb_core::{
    AdbError, DatabaseName, QueryLimits, RequestContext, RequestId, Result, Scope, TenantId, UserId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    /// The secret. Never logged.
    pub key: String,
    pub tenant: TenantId,
    #[serde(default = "default_user")]
    pub user: String,
    /// Granted scopes. Empty means read-only.
    #[serde(default)]
    pub scopes: BTreeSet<Scope>,
    #[serde(default)]
    pub limits: QueryLimits,
    /// Database used when a call does not name one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_database: Option<DatabaseName>,
}

fn default_user() -> String {
    "agent".to_string()
}

impl ApiKey {
    pub fn new(key: impl Into<String>, tenant: TenantId) -> Self {
        Self {
            key: key.into(),
            tenant,
            user: default_user(),
            scopes: Scope::read_only(),
            limits: QueryLimits::default(),
            default_database: None,
        }
    }

    pub fn with_scopes(mut self, scopes: impl IntoIterator<Item = Scope>) -> Self {
        self.scopes = scopes.into_iter().collect();
        self
    }

    pub fn read_write(mut self) -> Self {
        self.scopes = Scope::ALL.into_iter().collect();
        self
    }

    pub fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_default_database(mut self, database: DatabaseName) -> Self {
        self.default_database = Some(database);
        self
    }

    fn context(&self) -> RequestContext {
        RequestContext {
            tenant: self.tenant.clone(),
            database: self.default_database.clone(),
            user: UserId(self.user.clone()),
            request_id: RequestId::new(),
            scopes: self.scopes.clone(),
            limits: self.limits,
        }
    }
}

/// Byte-comparison in constant time, so a wrong key cannot be discovered one
/// character at a time by timing the response.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    // The length is not secret, but the comparison itself must not short-circuit.
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, Default, Clone)]
pub struct AuthRegistry {
    /// Kept as a list rather than a map so lookup can be constant-time.
    keys: Vec<ApiKey>,
    /// Identity for transports where the process boundary *is* the trust
    /// boundary (stdio: whoever launched us already has our files).
    local: Option<ApiKey>,
}

impl AuthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_key(mut self, key: ApiKey) -> Self {
        self.keys.push(key);
        self
    }

    pub fn with_local_identity(mut self, key: ApiKey) -> Self {
        self.local = Some(key);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Resolve a bearer token to a request context.
    pub fn context_for_key(&self, presented: &str) -> Result<RequestContext> {
        let mut found = None;
        for key in &self.keys {
            if constant_time_eq(&key.key, presented) {
                found = Some(key);
            }
        }
        found
            .map(ApiKey::context)
            .ok_or_else(|| AdbError::PermissionDenied("a valid API key".to_string()))
    }

    /// The stdio identity.
    pub fn local_context(&self) -> Result<RequestContext> {
        self.local
            .as_ref()
            .map(ApiKey::context)
            .ok_or_else(|| AdbError::PermissionDenied("a configured local identity".to_string()))
    }

    /// Map an `Authorization: Bearer <key>` header value to a context.
    pub fn context_for_header(&self, header: Option<&str>) -> Result<RequestContext> {
        let header = header.ok_or_else(|| {
            AdbError::PermissionDenied("an Authorization: Bearer header".to_string())
        })?;
        let token = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .unwrap_or(header)
            .trim();
        self.context_for_key(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> AuthRegistry {
        AuthRegistry::new()
            .with_key(
                ApiKey::new("secret-writer", TenantId::new("acme").unwrap())
                    .read_write()
                    .with_default_database(DatabaseName::new("crm").unwrap()),
            )
            .with_key(
                ApiKey::new("secret-reader", TenantId::new("acme").unwrap()).with_limits(
                    QueryLimits {
                        max_rows: 100,
                        ..QueryLimits::default()
                    },
                ),
            )
    }

    #[test]
    fn keys_carry_tenant_scopes_and_limits() {
        let writer = registry().context_for_key("secret-writer").unwrap();
        assert_eq!(writer.tenant.as_str(), "acme");
        assert!(writer.has(Scope::DataInsert));
        assert_eq!(writer.database.as_ref().unwrap().as_str(), "crm");

        let reader = registry().context_for_key("secret-reader").unwrap();
        assert!(!reader.has(Scope::DataInsert));
        assert!(reader.has(Scope::DatabaseRead));
        assert_eq!(reader.limits.max_rows, 100);
        assert!(reader.database.is_none());
    }

    #[test]
    fn unknown_keys_are_refused() {
        let err = registry().context_for_key("nope").unwrap_err();
        assert_eq!(err.code(), "permission_denied");
        // A near-miss must not be accepted either.
        assert!(registry().context_for_key("secret-write").is_err());
        assert!(registry().context_for_key("secret-writer ").is_err());
    }

    #[test]
    fn bearer_headers_are_parsed() {
        let registry = registry();
        assert!(registry
            .context_for_header(Some("Bearer secret-writer"))
            .is_ok());
        assert!(registry
            .context_for_header(Some("bearer secret-writer"))
            .is_ok());
        assert!(registry.context_for_header(Some("secret-writer")).is_ok());
        assert!(registry.context_for_header(None).is_err());
        assert!(registry.context_for_header(Some("Bearer wrong")).is_err());
    }

    #[test]
    fn stdio_needs_a_configured_local_identity() {
        assert!(registry().local_context().is_err());
        let registry = registry().with_local_identity(
            ApiKey::new("unused", TenantId::new("local").unwrap()).read_write(),
        );
        let ctx = registry.local_context().unwrap();
        assert_eq!(ctx.tenant.as_str(), "local");
        assert!(ctx.has(Scope::SchemaWrite));
    }

    #[test]
    fn constant_time_eq_is_still_correct() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn each_context_gets_its_own_request_id() {
        let registry = registry();
        let a = registry.context_for_key("secret-writer").unwrap();
        let b = registry.context_for_key("secret-writer").unwrap();
        assert_ne!(a.request_id, b.request_id);
    }
}
