//! MCP over Streamable HTTP (MCP revision 2025-06-18).
//!
//! One endpoint, three methods:
//!
//! * `POST` carries one JSON-RPC message. A notification or a client response is
//!   acknowledged with `202` and no body. A request is answered either as a
//!   single `application/json` body or, when the client accepts
//!   `text/event-stream`, as an SSE stream carrying the response and then
//!   closing.
//! * `GET` opens a server-to-client SSE stream. The server has no unsolicited
//!   messages yet, so it carries keep-alives until the client leaves or the
//!   server shuts down.
//! * `DELETE` ends a session.
//!
//! Every request dispatches through the same [`adb_mcp::McpServer`] as stdio,
//! so the transports cannot drift apart in what they allow. Sessions are
//! correlation, not authentication: each request still carries its API key.

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};

use adb_core::AdbError;
use axum::extract::State;
use axum::http::header::{ACCEPT, ALLOW, CONTENT_TYPE, ORIGIN};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::stream;
use serde_json::{json, Value as JsonValue};

use crate::error::ApiError;
use crate::rest::AppState;

/// Header carrying the session identifier, per the MCP transport spec.
pub const SESSION_HEADER: &str = "mcp-session-id";
/// Header a client sends after initialization with the negotiated revision.
pub const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Sessions idle longer than this are forgotten, so clients that never send
/// `DELETE` cannot grow the table without bound.
const SESSION_IDLE: Duration = Duration::from_secs(60 * 60);
/// How often an idle SSE stream sends a comment to keep proxies from closing it.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// Known session identifiers and when each was last used.
#[derive(Debug, Default)]
pub struct Sessions {
    seen: HashMap<String, Instant>,
}

impl Sessions {
    fn open(&mut self) -> String {
        let now = Instant::now();
        self.seen
            .retain(|_, last| now.duration_since(*last) < SESSION_IDLE);
        let id = uuid::Uuid::new_v4().to_string();
        self.seen.insert(id.clone(), now);
        id
    }

    /// Mark `id` as used. `false` if it is unknown or expired.
    fn touch(&mut self, id: &str) -> bool {
        match self.seen.get_mut(id) {
            Some(last) if last.elapsed() < SESSION_IDLE => {
                *last = Instant::now();
                true
            }
            _ => false,
        }
    }

    fn close(&mut self, id: &str) -> bool {
        self.seen.remove(id).is_some()
    }
}

/// A transport-level refusal, answered before any JSON-RPC is read. Same body
/// shape as every other API error.
pub struct Refusal {
    status: StatusCode,
    code: &'static str,
    message: String,
}

fn refuse(status: StatusCode, code: &'static str, message: impl Into<String>) -> Refusal {
    Refusal {
        status,
        code,
        message: message.into(),
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code, "message": self.message } });
        (self.status, axum::Json(body)).into_response()
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn accepts_event_stream(headers: &HeaderMap) -> bool {
    header(headers, ACCEPT.as_str())
        .map(|accept| accept.contains("text/event-stream"))
        .unwrap_or(false)
}

/// Checks every method shares: origin, protocol revision, and a known session.
///
/// A browser page on another site can reach a server bound to localhost, so an
/// `Origin` that is neither local nor explicitly allowed is refused, which is
/// the DNS-rebinding defence the transport spec asks for. Clients that are not
/// browsers send no `Origin` and are unaffected.
fn preflight(state: &AppState, headers: &HeaderMap) -> Result<Option<String>, Refusal> {
    if let Some(origin) = header(headers, ORIGIN.as_str()) {
        if !state.origin_allowed(origin) {
            return Err(refuse(
                StatusCode::FORBIDDEN,
                "permission_denied",
                format!("origin {origin:?} is not allowed; start the server with --allow-origin"),
            ));
        }
    }
    if let Some(version) = header(headers, PROTOCOL_VERSION_HEADER) {
        if !adb_mcp::server::SUPPORTED_VERSIONS.contains(&version) {
            return Err(refuse(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!(
                    "MCP protocol version {version:?} is not supported; this server speaks {}",
                    adb_mcp::server::SUPPORTED_VERSIONS.join(", ")
                ),
            ));
        }
    }
    let session = header(headers, SESSION_HEADER).map(str::to_string);
    if let Some(id) = &session {
        if !state.sessions().touch(id) {
            // 404 tells a client its session is gone and it should initialize again.
            return Err(refuse(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown or expired MCP session; send initialize again",
            ));
        }
    }
    Ok(session)
}

/// `POST`: one JSON-RPC message in, one response (or an acknowledgement) out.
pub async fn post(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    if let Err(refusal) = preflight(&state, &headers) {
        return refusal.into_response();
    }
    let ctx = match state.context(&headers, None) {
        Ok(ctx) => ctx,
        Err(error) => return error.into_response(),
    };

    // A message without a method is the client answering a server request.
    // This server sends none, but the transport says to acknowledge them.
    let parsed = serde_json::from_str::<JsonValue>(&body).ok();
    let method = parsed
        .as_ref()
        .and_then(|message| message.get("method"))
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    if method.is_none() && parsed.as_ref().is_some_and(JsonValue::is_object) {
        let is_response = parsed
            .as_ref()
            .is_some_and(|m| m.get("result").is_some() || m.get("error").is_some());
        if is_response {
            return StatusCode::ACCEPTED.into_response();
        }
    }

    let mcp = state.mcp.clone();
    let reply = match tokio::task::spawn_blocking(move || mcp.handle_line(&body, &ctx)).await {
        Ok(reply) => reply,
        Err(error) => {
            return ApiError(AdbError::internal(format!(
                "the request task failed: {error}"
            )))
            .into_response()
        }
    };
    let Some(line) = reply else {
        // A notification gets 202 with no body.
        return StatusCode::ACCEPTED.into_response();
    };

    let initialized = method.as_deref() == Some("initialize")
        && serde_json::from_str::<JsonValue>(&line)
            .ok()
            .is_some_and(|reply| reply.get("result").is_some());
    let new_session = initialized.then(|| state.sessions().open());

    let mut response = if accepts_event_stream(&headers) {
        let event = Event::default().event("message").data(line);
        Sse::new(stream::once(async move { Ok::<_, Infallible>(event) })).into_response()
    } else {
        (
            StatusCode::OK,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            line,
        )
            .into_response()
    };
    if let Some(id) = new_session {
        if let Ok(value) = HeaderValue::from_str(&id) {
            response.headers_mut().insert(SESSION_HEADER, value);
        }
    }
    response
}

/// `GET`: a server-to-client event stream, open until the client goes away or
/// the server begins shutting down.
pub async fn get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(refusal) = preflight(&state, &headers) {
        return refusal.into_response();
    }
    if let Err(error) = state.context(&headers, None) {
        return error.into_response();
    }
    if !accepts_event_stream(&headers) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(ALLOW, HeaderValue::from_static("POST, DELETE"))],
        )
            .into_response();
    }
    // No unsolicited messages exist yet, so the stream yields nothing and ends
    // when shutdown starts. Ending it matters: graceful shutdown waits for open
    // connections, and an idle stream would otherwise hold it for the full
    // drain timeout.
    let stopping = state.stopping();
    let events = stream::unfold(stopping, |mut stopping| async move {
        let _ = stopping.wait_for(|stopped| *stopped).await;
        None::<(Result<Event, Infallible>, _)>
    });
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response()
}

/// `DELETE`: the client is done with its session.
pub async fn delete(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let session = match preflight(&state, &headers) {
        Ok(session) => session,
        Err(refusal) => return refusal.into_response(),
    };
    if let Err(error) = state.context(&headers, None) {
        return error.into_response();
    }
    let Some(id) = session else {
        return refuse(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("DELETE needs the {SESSION_HEADER} header"),
        )
        .into_response();
    };
    state.sessions().close(&id);
    StatusCode::NO_CONTENT.into_response()
}

/// Whether `origin` names this machine: `http://localhost:3000`,
/// `http://127.0.0.1`, `http://[::1]:8080`.
pub(crate) fn is_local_origin(origin: &str) -> bool {
    let Some((_, rest)) = origin.split_once("://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or("")
    } else {
        authority.split(':').next().unwrap_or("")
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_origins_are_recognized_and_lookalikes_are_not() {
        for local in [
            "http://localhost",
            "http://localhost:3000",
            "https://127.0.0.1:8080",
            "http://[::1]:8080",
        ] {
            assert!(is_local_origin(local), "{local}");
        }
        for foreign in [
            "http://localhost.evil.example",
            "https://example.com",
            "http://127.0.0.1.nip.io",
            "null",
            "localhost",
        ] {
            assert!(!is_local_origin(foreign), "{foreign}");
        }
    }

    #[test]
    fn sessions_open_touch_and_close() {
        let mut sessions = Sessions::default();
        let id = sessions.open();
        assert!(sessions.touch(&id));
        assert!(!sessions.touch("not-a-session"));
        assert!(sessions.close(&id));
        assert!(!sessions.touch(&id));
    }
}
