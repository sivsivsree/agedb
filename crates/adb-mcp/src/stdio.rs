//! Newline-delimited JSON-RPC over stdin/stdout, the MCP stdio transport.
//!
//! Nothing may be written to stdout except responses, because a stray `println!`
//! breaks the stream. All logging goes to stderr (see `adb-server`).

use std::io::{BufRead, BufReader, Read, Write};
use std::sync::Arc;

use adb_core::Result;

use crate::server::McpServer;

/// Serve until stdin closes.
pub fn serve_stdio(server: Arc<McpServer>) -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    serve(server, stdin.lock(), stdout.lock())
}

/// Transport loop over any reader/writer, so tests can drive it with pipes.
pub fn serve<R: Read, W: Write>(server: Arc<McpServer>, input: R, mut output: W) -> Result<()> {
    let ctx = server.local_context()?;
    tracing::info!(
        tenant = %ctx.tenant,
        user = %ctx.user,
        "MCP stdio session started"
    );
    let reader = BufReader::new(input);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        // A fresh request id per message keeps logs correlatable, while the
        // identity stays that of the session.
        let mut ctx = ctx.clone();
        ctx.request_id = adb_core::RequestId::new();
        if let Some(response) = server.handle_line(&line, &ctx) {
            writeln!(output, "{response}")?;
            output.flush()?;
        }
    }
    tracing::info!("MCP stdio session ended");
    Ok(())
}
