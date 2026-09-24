use std::fmt::Display;

/// Every fallible AgenticDB operation returns this.
///
/// The variants map directly onto MCP/REST error responses, so the edge never
/// has to guess whether a failure was the caller's fault or ours.
#[derive(Debug, thiserror::Error)]
pub enum AdbError {
    #[error("invalid identifier {value:?}: {reason}")]
    InvalidIdentifier { value: String, reason: String },

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("{kind} {name:?} not found")]
    NotFound { kind: &'static str, name: String },

    #[error("{kind} {name:?} already exists")]
    AlreadyExists { kind: &'static str, name: String },

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("unsupported in v0.1: {0}")]
    Unsupported(String),

    #[error("permission denied: {0} required")]
    PermissionDenied(String),

    #[error("query limit exceeded: {limit} ({detail})")]
    LimitExceeded { limit: &'static str, detail: String },

    #[error("storage: {0}")]
    Storage(String),

    #[error("corruption detected: {0}")]
    Corruption(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl AdbError {
    pub fn not_found(kind: &'static str, name: impl Display) -> Self {
        Self::NotFound {
            kind,
            name: name.to_string(),
        }
    }

    pub fn already_exists(kind: &'static str, name: impl Display) -> Self {
        Self::AlreadyExists {
            kind,
            name: name.to_string(),
        }
    }

    pub fn bad_request(msg: impl Display) -> Self {
        Self::BadRequest(msg.to_string())
    }

    pub fn internal(msg: impl Display) -> Self {
        Self::Internal(msg.to_string())
    }

    pub fn storage(msg: impl Display) -> Self {
        Self::Storage(msg.to_string())
    }

    /// Stable machine-readable code for API responses.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidIdentifier { .. } => "invalid_identifier",
            Self::InvalidSchema(_) => "invalid_schema",
            Self::NotFound { .. } => "not_found",
            Self::AlreadyExists { .. } => "already_exists",
            Self::BadRequest(_) => "bad_request",
            Self::TypeMismatch { .. } => "type_mismatch",
            Self::Unsupported(_) => "unsupported",
            Self::PermissionDenied(_) => "permission_denied",
            Self::LimitExceeded { .. } => "limit_exceeded",
            Self::Storage(_) => "storage_error",
            Self::Corruption(_) => "corruption",
            Self::Internal(_) => "internal_error",
        }
    }
}

impl From<std::io::Error> for AdbError {
    fn from(e: std::io::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for AdbError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Internal(format!("arrow: {e}"))
    }
}

impl From<serde_json::Error> for AdbError {
    fn from(e: serde_json::Error) -> Self {
        Self::BadRequest(format!("json: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, AdbError>;
