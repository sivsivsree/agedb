//! HTTP error mapping.
//!
//! Client mistakes and server faults get different status codes, and the body is
//! always the same shape, so an agent can branch on `error.code` instead of
//! parsing prose.

use adb_core::AdbError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub struct ApiError(pub AdbError);

impl From<AdbError> for ApiError {
    fn from(error: AdbError) -> Self {
        Self(error)
    }
}

impl ApiError {
    pub fn status(&self) -> StatusCode {
        match &self.0 {
            AdbError::NotFound { .. } => StatusCode::NOT_FOUND,
            AdbError::AlreadyExists { .. } => StatusCode::CONFLICT,
            AdbError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            AdbError::Unsupported(_) => StatusCode::UNPROCESSABLE_ENTITY,
            // Retryable in principle: the caller can narrow the query.
            AdbError::LimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            AdbError::InvalidIdentifier { .. }
            | AdbError::InvalidSchema(_)
            | AdbError::BadRequest(_)
            | AdbError::TypeMismatch { .. } => StatusCode::BAD_REQUEST,
            AdbError::Storage(_) | AdbError::Corruption(_) | AdbError::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() {
            tracing::error!(code = self.0.code(), error = %self.0, "request failed");
        } else {
            tracing::debug!(code = self.0.code(), error = %self.0, "request rejected");
        }
        let body = json!({
            "error": { "code": self.0.code(), "message": self.0.to_string() }
        });
        (status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_mistakes_and_server_faults_differ() {
        let cases = [
            (AdbError::not_found("table", "x"), StatusCode::NOT_FOUND),
            (
                AdbError::already_exists("database", "x"),
                StatusCode::CONFLICT,
            ),
            (
                AdbError::PermissionDenied("data:insert".into()),
                StatusCode::FORBIDDEN,
            ),
            (
                AdbError::Unsupported("joins".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                AdbError::LimitExceeded {
                    limit: "query:max_rows",
                    detail: "x".into(),
                },
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (AdbError::bad_request("x"), StatusCode::BAD_REQUEST),
            (AdbError::internal("x"), StatusCode::INTERNAL_SERVER_ERROR),
            (
                AdbError::Corruption("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(ApiError(error).status(), expected);
        }
    }
}
