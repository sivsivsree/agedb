//! MCP request handling, independent of transport.
//!
//! Both the stdio loop and the HTTP endpoint call [`McpServer::handle`], so the
//! two transports cannot drift apart in what they allow.

use std::sync::Arc;

use adb_core::{AdbError, RequestContext, Result};
use adb_engine::Engine;
use serde_json::{json, Value as Json};

use crate::auth::AuthRegistry;
use crate::protocol::{codes, JsonRpcRequest, JsonRpcResponse, RpcError};
use crate::tools;

pub const SERVER_NAME: &str = "agedb";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// MCP revision we speak. If a client asks for a different one we still answer
/// with ours, which is what the spec prescribes for version negotiation.
pub const PROTOCOL_VERSION: &str = "2025-06-18";
const SUPPORTED_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

pub struct McpServer {
    engine: Arc<Engine>,
    auth: AuthRegistry,
}

impl McpServer {
    pub fn new(engine: Arc<Engine>, auth: AuthRegistry) -> Self {
        Self { engine, auth }
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn auth(&self) -> &AuthRegistry {
        &self.auth
    }

    /// Handle one parsed request. `None` for notifications, which get no reply.
    pub fn handle(
        &self,
        request: &JsonRpcRequest,
        ctx: &RequestContext,
    ) -> Option<JsonRpcResponse> {
        let id = request.id.clone().unwrap_or(Json::Null);
        if request.is_notification() {
            // `notifications/initialized` and friends: nothing to answer.
            tracing::debug!(method = %request.method, "notification");
            return None;
        }
        if !request.jsonrpc.is_empty() && request.jsonrpc != "2.0" {
            return Some(JsonRpcResponse::failed(
                id,
                RpcError::new(
                    codes::INVALID_REQUEST,
                    format!("unsupported jsonrpc version {:?}", request.jsonrpc),
                ),
            ));
        }

        let response = match request.method.as_str() {
            "initialize" => Ok(self.initialize(request.params())),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tools::definitions_json() })),
            "tools/call" => return Some(self.call_tool(id, request.params(), ctx)),
            // Declared as unsupported rather than silently empty, so a client
            // knows these capabilities are genuinely absent.
            other => Err(RpcError::new(
                codes::METHOD_NOT_FOUND,
                format!("method {other:?} is not supported"),
            )),
        };
        Some(match response {
            Ok(result) => JsonRpcResponse::ok(id, result),
            Err(error) => JsonRpcResponse::failed(id, error),
        })
    }

    fn initialize(&self, params: &Json) -> Json {
        let requested = params.get("protocolVersion").and_then(Json::as_str);
        let version = requested
            .filter(|v| SUPPORTED_VERSIONS.contains(v))
            .unwrap_or(PROTOCOL_VERSION);
        if let Some(requested) = requested {
            if !SUPPORTED_VERSIONS.contains(&requested) {
                tracing::warn!(
                    requested,
                    offered = PROTOCOL_VERSION,
                    "protocol version mismatch"
                );
            }
        }
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
            "instructions": format!(
                "An agent-native analytical database. Create databases and tables, insert rows, \
                 then ask questions with data_query, either in plain language via `request` or \
                 as a structured `plan`. Natural language is handled by: {}. Start with \
                 table_list or schema_get to see what exists.",
                self.engine.translator_name()
            ),
        })
    }

    /// `tools/call`: tool errors are returned as results with `isError`, per the
    /// MCP convention, so the model sees the message and can correct itself.
    /// Only protocol-level problems become JSON-RPC errors.
    fn call_tool(&self, id: Json, params: &Json, ctx: &RequestContext) -> JsonRpcResponse {
        let Some(name) = params.get("name").and_then(Json::as_str) else {
            return JsonRpcResponse::failed(
                id,
                RpcError::new(codes::INVALID_PARAMS, "tools/call needs a string \"name\""),
            );
        };
        let arguments = params.get("arguments").cloned().unwrap_or(Json::Null);

        let started = std::time::Instant::now();
        let outcome = tools::call(&self.engine, ctx, name, &arguments);
        let elapsed = started.elapsed();

        match outcome {
            Ok(result) => {
                tracing::info!(
                    tool = name,
                    request_id = %ctx.request_id,
                    tenant = %ctx.tenant,
                    ms = elapsed.as_millis(),
                    "tool call succeeded"
                );
                JsonRpcResponse::ok(id, tool_result(&result, false))
            }
            Err(error) => {
                tracing::warn!(
                    tool = name,
                    request_id = %ctx.request_id,
                    tenant = %ctx.tenant,
                    code = error.code(),
                    %error,
                    "tool call failed"
                );
                JsonRpcResponse::ok(id, tool_error(&error))
            }
        }
    }

    /// Parse and handle one line of input.
    pub fn handle_line(&self, line: &str, ctx: &RequestContext) -> Option<String> {
        match serde_json::from_str::<JsonRpcRequest>(line) {
            Ok(request) => self.handle(&request, ctx).map(|r| r.to_line()),
            Err(error) => Some(
                JsonRpcResponse::failed(
                    Json::Null,
                    RpcError::new(codes::PARSE_ERROR, format!("invalid JSON-RPC: {error}")),
                )
                .to_line(),
            ),
        }
    }

    /// Context for a stdio session.
    pub fn local_context(&self) -> Result<RequestContext> {
        self.auth.local_context()
    }

    /// Context for an HTTP request.
    pub fn context_for_header(&self, header: Option<&str>) -> Result<RequestContext> {
        self.auth.context_for_header(header)
    }
}

/// MCP tool result: a text block holding the JSON payload.
fn tool_result(payload: &Json, is_error: bool) -> Json {
    let text = serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
        // Also provided structurally, for clients that can use it directly.
        "structuredContent": payload,
    })
}

fn tool_error(error: &AdbError) -> Json {
    let payload = json!({
        "error": { "code": error.code(), "message": error.to_string() },
    });
    let text = format!("{} ({})", error, error.code());
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": true,
        "structuredContent": payload,
    })
}
