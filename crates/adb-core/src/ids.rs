use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{AdbError, Result};

/// Maximum length of any user-supplied identifier.
pub const MAX_IDENT_LEN: usize = 64;

/// Identifiers become filesystem path components, so they are deliberately
/// narrow: lowercase ascii, digits, `_` and `-`, never starting with a digit or
/// `-`. This is what makes `tenants/{t}/databases/{d}/tables/{t}` safe to build
/// by concatenation.
pub fn validate_ident(kind: &'static str, value: &str) -> Result<()> {
    let reason = if value.is_empty() {
        Some("must not be empty".to_string())
    } else if value.len() > MAX_IDENT_LEN {
        Some(format!("must be at most {MAX_IDENT_LEN} characters"))
    } else if !value
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        Some("may only contain a-z, 0-9, '_' and '-'".to_string())
    } else if !value.as_bytes()[0].is_ascii_lowercase() && value.as_bytes()[0] != b'_' {
        Some("must start with a lowercase letter or '_'".to_string())
    } else {
        None
    };

    match reason {
        None => Ok(()),
        Some(reason) => Err(AdbError::InvalidIdentifier {
            value: format!("{kind}:{value}"),
            reason,
        }),
    }
}

macro_rules! ident_type {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate_ident($kind, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = AdbError;
            fn from_str(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = AdbError;
            fn try_from(s: String) -> Result<Self> {
                Self::new(s)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = AdbError;
            fn try_from(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }

        // Validation happens on deserialize too: a manifest or WAL record with a
        // path-traversing name must not be loadable.
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                Self::new(s).map_err(serde::de::Error::custom)
            }
        }
    };
}

ident_type!(TenantId, "tenant");
ident_type!(DatabaseName, "database");
ident_type!(TableName, "table");
ident_type!(ColumnName, "column");

/// Free-form caller identity (subject of an API key, or an agent name).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserId(pub String);

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Correlates a single agent request across logs, plans and execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_reasonable_names() {
        for name in ["users", "order_items", "t1", "a-b", "_internal"] {
            assert!(TableName::new(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn rejects_path_traversal_and_case() {
        for name in [
            "", "..", "../etc", "Users", "a/b", "1users", "-x", "a b", "a.b",
        ] {
            assert!(TableName::new(name).is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn rejects_overlong_names() {
        assert!(TableName::new("a".repeat(MAX_IDENT_LEN)).is_ok());
        assert!(TableName::new("a".repeat(MAX_IDENT_LEN + 1)).is_err());
    }

    #[test]
    fn deserialize_validates() {
        let ok: std::result::Result<TableName, _> = serde_json::from_str("\"orders\"");
        assert!(ok.is_ok());
        let bad: std::result::Result<TableName, _> = serde_json::from_str("\"../../etc/passwd\"");
        assert!(bad.is_err());
    }
}
