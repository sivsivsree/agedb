//! MCP gateway (see "Design rationale" and "Guardrails" in ARCHITECTURE.md).
//!
//! This is the primary interface: an agent sees a handful of tools, not a query
//! language. Three things are deliberate:
//!
//! * **No `execute_arbitrary_query` tool.** The only way to read data is
//!   `data_query`, which takes either a natural-language request or a structured
//!   plan, and both go through the validator.
//! * **Scopes and limits are checked on every call**, from the identity the
//!   transport authenticated, never from the arguments (see "Guardrails" in ARCHITECTURE.md).
//! * **Errors are written for a model to act on.** A missing column comes back
//!   as an error naming the columns that do exist, so the agent's next attempt
//!   can be right.
//!
//! The JSON-RPC plumbing is hand-rolled: it is a small, stable protocol, and one
//! fewer fast-moving dependency in the trust path.

pub mod auth;
pub mod protocol;
pub mod server;
pub mod stdio;
pub mod tools;

pub use auth::{ApiKey, AuthRegistry};
pub use protocol::{JsonRpcRequest, JsonRpcResponse, RpcError};
pub use server::{McpServer, PROTOCOL_VERSION, SERVER_NAME};
pub use stdio::serve_stdio;
pub use tools::{ToolDefinition, TOOL_NAMES};
