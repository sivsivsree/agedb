//! Minimal JSON-RPC 2.0, as MCP uses it.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

/// Standard JSON-RPC error codes.
pub mod codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default)]
    pub jsonrpc: String,
    /// Absent for notifications, which take no response.
    #[serde(default)]
    pub id: Option<Json>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Json>,
}

impl JsonRpcRequest {
    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }

    pub fn params(&self) -> &Json {
        self.params.as_ref().unwrap_or(&Json::Null)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Json>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(mut self, data: Json) -> Self {
        self.data = Some(data);
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Json,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Json>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl JsonRpcResponse {
    pub fn ok(id: Json, result: Json) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn failed(id: Json, error: RpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }

    pub fn to_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            // Serializing a response cannot normally fail; if it does, still
            // answer with something a client can parse.
            json!({
                "jsonrpc": "2.0",
                "id": Json::Null,
                "error": { "code": codes::INTERNAL_ERROR, "message": e.to_string() }
            })
            .to_string()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_parse_with_and_without_params() {
        let request: JsonRpcRequest =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#).unwrap();
        assert_eq!(request.method, "tools/list");
        assert!(!request.is_notification());
        assert_eq!(request.params(), &Json::Null);

        let notification: JsonRpcRequest =
            serde_json::from_str(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                .unwrap();
        assert!(notification.is_notification());
    }

    #[test]
    fn responses_serialize_without_null_fields() {
        let line = JsonRpcResponse::ok(json!(1), json!({"ok": true})).to_line();
        assert!(line.contains("\"result\""));
        assert!(!line.contains("\"error\""));

        let line = JsonRpcResponse::failed(json!(2), RpcError::new(codes::INVALID_PARAMS, "bad"))
            .to_line();
        assert!(line.contains("\"error\""));
        assert!(!line.contains("\"result\""));
    }

    #[test]
    fn a_line_is_always_one_line() {
        let line = JsonRpcResponse::ok(json!(1), json!({"text": "a\nb"})).to_line();
        assert_eq!(line.lines().count(), 1, "newlines must be escaped: {line}");
    }
}
