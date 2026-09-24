//! REST API and MCP-over-HTTP (see "Interfaces" and "Design rationale" in ARCHITECTURE.md).
//!
//! The REST surface mirrors the MCP tools one-for-one rather than inventing a
//! second data model: the same validated plans, the same scopes, the same
//! limits. `/mcp` (and `/v1/mcp`) implements the MCP Streamable HTTP transport
//! over the same JSON-RPC handler as stdio, so an MCP client can connect over
//! HTTP without a separate implementation.
//!
//! The engine is synchronous, so every handler runs it inside
//! `spawn_blocking`: a scan is CPU- and file-bound, and pretending otherwise
//! would block the async runtime's worker threads.

pub mod error;
pub mod mcp_http;
pub mod rest;

pub use error::ApiError;
pub use rest::{router, AppState};

use adb_core::{AdbError, Result};

/// Bind and serve until `shutdown` resolves.
///
/// Lives here rather than in the binary so axum stays an implementation detail
/// of this crate.
pub async fn serve_http<F>(bind: &str, port: u16, state: AppState, shutdown: F) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let address = format!("{bind}:{port}");
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|e| AdbError::storage(format!("cannot bind {address}: {e}")))?;
    tracing::info!(%address, "HTTP listening");
    let app = router(state.clone());
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            // Graceful shutdown waits for open connections, so end the MCP
            // event streams rather than letting them hold the drain open.
            state.begin_shutdown();
        })
        .await
        .map_err(|e| AdbError::storage(format!("http server: {e}")))
}
