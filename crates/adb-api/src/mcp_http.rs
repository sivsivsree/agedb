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
//! `OPTIONS` answers CORS preflight for browser origins that are allowed.
//!
//! Every request dispatches through the same [`adb_mcp::McpServer`] as stdio,
//! so the transports cannot drift apart in what they allow. Sessions are
//! correlation, not authentication: each request still carries its API key,
//! and a session can only be used by the key that opened it.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use adb_core::{AdbError, RequestContext};
use adb_mcp::JsonRpcRequest;
use axum::extract::State;
use axum::http::header::{
    ACCEPT, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, ALLOW,
    AUTHORIZATION, CONTENT_TYPE, ORIGIN, VARY,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::stream;
use serde::de::IgnoredAny;
use serde::Deserialize;

use crate::error::ApiError;
use crate::rest::AppState;

/// Header carrying the session identifier, per the MCP transport spec.
pub const SESSION_HEADER: &str = "mcp-session-id";
/// Header a client sends after initialization with the negotiated revision.
pub const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Sessions idle longer than this are forgotten.
const SESSION_IDLE: Duration = Duration::from_secs(60 * 60);
/// At most this many live sessions. Past it, the least recently used one is
/// dropped, so a client that re-initializes in a loop cannot grow memory
/// without bound; its oldest sessions simply get `404` and re-initialize.
const MAX_SESSIONS: usize = 10_000;
/// How often an idle SSE stream sends a comment to keep proxies from closing it.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

const ALLOWED_METHODS: &str = "GET, POST, DELETE, OPTIONS";
const ALLOWED_HEADERS: &str =
    "authorization, content-type, accept, mcp-session-id, mcp-protocol-version, last-event-id";

/// A live session: which key opened it, and when it was last used.
#[derive(Debug)]
struct Session {
    owner: u64,
    last_used: Instant,
}

/// Known sessions, each bound to the API key that opened it.
#[derive(Debug, Default)]
pub struct Sessions {
    live: HashMap<String, Session>,
}

impl Sessions {
    fn open(&mut self, owner: u64) -> String {
        let now = Instant::now();
        if self.live.len() >= MAX_SESSIONS {
            self.live
                .retain(|_, session| now.duration_since(session.last_used) < SESSION_IDLE);
        }
        if self.live.len() >= MAX_SESSIONS {
            let oldest = self
                .live
                .iter()
                .min_by_key(|(_, session)| session.last_used)
                .map(|(id, _)| id.clone());
            if let Some(oldest) = oldest {
                self.live.remove(&oldest);
            }
        }
        let id = uuid::Uuid::new_v4().to_string();
        self.live.insert(
            id.clone(),
            Session {
                owner,
                last_used: now,
            },
        );
        id
    }

    /// Mark `id` as used by `owner`. `false` if it is unknown, expired, or
    /// belongs to another key, which are deliberately indistinguishable.
    fn touch(&mut self, id: &str, owner: u64) -> bool {
        match self.live.get_mut(id) {
            Some(session) if session.owner == owner => {
                if session.last_used.elapsed() >= SESSION_IDLE {
                    self.live.remove(id);
                    return false;
                }
                session.last_used = Instant::now();
                true
            }
            _ => false,
        }
    }

    fn close(&mut self, id: &str) {
        self.live.remove(id);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.live.len()
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

/// Identifies the API key a request authenticated with, without keeping the
/// key itself in the session table.
fn key_fingerprint(headers: &HeaderMap) -> u64 {
    let token = header(headers, AUTHORIZATION.as_str())
        .map(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
                .unwrap_or(value)
                .trim()
        })
        .unwrap_or("");
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    hasher.finish()
}

/// Checks every method shares, in order: origin, protocol revision,
/// authentication, and then the session. The session is looked up only after
/// authentication, so an anonymous caller learns nothing about which session
/// identifiers exist and cannot keep one alive.
///
/// A browser page on another site can reach a server bound to localhost, so an
/// `Origin` that is neither local nor explicitly allowed is refused, which is
/// the DNS-rebinding defence the transport spec asks for. Clients that are not
/// browsers send no `Origin` and are unaffected.
fn admit(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(RequestContext, Option<String>), ApiError> {
    if let Some(origin) = header(headers, ORIGIN.as_str()) {
        if !state.origin_allowed(origin) {
            return Err(ApiError(AdbError::PermissionDenied(format!(
                "an allowed origin ({origin:?} is not; start the server with --allow-origin)"
            ))));
        }
    }
    if let Some(version) = header(headers, PROTOCOL_VERSION_HEADER) {
        if !adb_mcp::server::SUPPORTED_VERSIONS.contains(&version) {
            return Err(ApiError(AdbError::bad_request(format!(
                "MCP protocol version {version:?} is not supported; this server speaks {}",
                adb_mcp::server::SUPPORTED_VERSIONS.join(", ")
            ))));
        }
    }
    let ctx = state.context(headers, None)?;
    let session = header(headers, SESSION_HEADER).map(str::to_string);
    if let Some(id) = &session {
        if !state.sessions().touch(id, key_fingerprint(headers)) {
            // 404 tells a client its session is gone and it should initialize again.
            return Err(ApiError(AdbError::not_found(
                "MCP session (send initialize again)",
                id,
            )));
        }
    }
    Ok((ctx, session))
}

/// Add CORS headers for an allowed browser origin. Requests without an
/// `Origin` are not from a browser and get none.
fn with_cors(state: &AppState, headers: &HeaderMap, mut response: Response) -> Response {
    let Some(origin) = header(headers, ORIGIN.as_str()) else {
        return response;
    };
    if !state.origin_allowed(origin) {
        return response;
    }
    let Ok(origin) = HeaderValue::from_str(origin) else {
        return response;
    };
    let out = response.headers_mut();
    out.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    out.insert(
        ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(SESSION_HEADER),
    );
    out.append(VARY, HeaderValue::from_static("origin"));
    response
}

/// What a POST body turned out to be.
enum Outcome {
    /// Nothing to send back: a notification, or the client answering a
    /// server request.
    Accepted,
    Reply {
        line: String,
        initialized: bool,
    },
}

/// Just enough of a JSON-RPC message to recognize a client response, without
/// materializing its payload.
#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    result: Option<IgnoredAny>,
    #[serde(default)]
    error: Option<IgnoredAny>,
}

/// Parse and handle one POST body. Runs on a blocking thread: a body can be a
/// large `data_insert`, and parsing it is CPU work.
fn handle_post(state: &AppState, body: &str, ctx: &RequestContext) -> Outcome {
    let request = match serde_json::from_str::<JsonRpcRequest>(body) {
        Ok(request) => request,
        Err(_) => {
            // A message without a method is the client answering a server
            // request. This server sends none, but the transport says to
            // acknowledge them.
            let is_response = serde_json::from_str::<Envelope>(body)
                .map(|m| m.result.is_some() || m.error.is_some())
                .unwrap_or(false);
            if is_response {
                return Outcome::Accepted;
            }
            // Anything else gets the same parse error stdio would send.
            return match state.mcp.handle_line(body, ctx) {
                Some(line) => Outcome::Reply {
                    line,
                    initialized: false,
                },
                None => Outcome::Accepted,
            };
        }
    };
    match state.mcp.handle(&request, ctx) {
        None => Outcome::Accepted,
        Some(response) => {
            let initialized = request.method == "initialize" && response.error.is_none();
            Outcome::Reply {
                line: response.to_line(),
                initialized,
            }
        }
    }
}

/// `POST`: one JSON-RPC message in, one response (or an acknowledgement) out.
pub async fn post(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    let response = post_inner(&state, &headers, body).await;
    with_cors(&state, &headers, response)
}

async fn post_inner(state: &AppState, headers: &HeaderMap, body: String) -> Response {
    let ctx = match admit(state, headers) {
        Ok((ctx, _)) => ctx,
        Err(error) => return error.into_response(),
    };
    let worker = state.clone();
    let outcome = match tokio::task::spawn_blocking(move || handle_post(&worker, &body, &ctx)).await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return ApiError(AdbError::internal(format!(
                "the request task failed: {error}"
            )))
            .into_response()
        }
    };
    let (line, initialized) = match outcome {
        Outcome::Accepted => return StatusCode::ACCEPTED.into_response(),
        Outcome::Reply { line, initialized } => (line, initialized),
    };

    let mut response = if accepts_event_stream(headers) {
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
    if initialized {
        let id = state.sessions().open(key_fingerprint(headers));
        if let Ok(value) = HeaderValue::from_str(&id) {
            response.headers_mut().insert(SESSION_HEADER, value);
        }
    }
    response
}

/// `GET`: a server-to-client event stream, open until the client goes away or
/// the server begins shutting down.
pub async fn get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let response = get_inner(&state, &headers);
    with_cors(&state, &headers, response)
}

fn get_inner(state: &AppState, headers: &HeaderMap) -> Response {
    if let Err(error) = admit(state, headers) {
        return error.into_response();
    }
    if !accepts_event_stream(headers) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(ALLOW, HeaderValue::from_static(ALLOWED_METHODS))],
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
    let response = delete_inner(&state, &headers);
    with_cors(&state, &headers, response)
}

fn delete_inner(state: &AppState, headers: &HeaderMap) -> Response {
    // `admit` has already checked that the session exists and belongs to
    // this key, so another key cannot end it.
    let session = match admit(state, headers) {
        Ok((_, session)) => session,
        Err(error) => return error.into_response(),
    };
    let Some(id) = session else {
        return ApiError(AdbError::bad_request(format!(
            "DELETE needs the {SESSION_HEADER} header"
        )))
        .into_response();
    };
    state.sessions().close(&id);
    StatusCode::NO_CONTENT.into_response()
}

/// `OPTIONS`: CORS preflight. A browser sends this before any request that
/// carries `Authorization`, so without it no browser client could connect.
pub async fn options(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(origin) = header(&headers, ORIGIN.as_str()) {
        if !state.origin_allowed(origin) {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let response = (
        StatusCode::NO_CONTENT,
        [
            (ALLOW, HeaderValue::from_static(ALLOWED_METHODS)),
            (
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static(ALLOWED_METHODS),
            ),
            (
                ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static(ALLOWED_HEADERS),
            ),
            (ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600")),
        ],
    )
        .into_response();
    with_cors(&state, &headers, response)
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
    fn a_session_belongs_to_the_key_that_opened_it() {
        let mut sessions = Sessions::default();
        let id = sessions.open(1);
        assert!(sessions.touch(&id, 1));
        assert!(!sessions.touch(&id, 2), "another key cannot use it");
        assert!(!sessions.touch("not-a-session", 1));
        sessions.close(&id);
        assert!(!sessions.touch(&id, 1));
    }

    #[test]
    fn the_session_table_is_bounded() {
        let mut sessions = Sessions::default();
        let first = sessions.open(1);
        for _ in 0..MAX_SESSIONS + 50 {
            sessions.open(1);
        }
        assert_eq!(sessions.len(), MAX_SESSIONS);
        assert!(
            !sessions.touch(&first, 1),
            "the least recently used session makes room"
        );
    }
}
