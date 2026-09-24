//! REST API tests, driven through the real router (headers, status codes and
//! bodies included).

use std::sync::Arc;

use adb_api::{router, AppState};
use adb_core::{DatabaseName, QueryLimits, Scope, TenantId};
use adb_engine::{Engine, EngineConfig};
use adb_mcp::{ApiKey, AuthRegistry};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value as Json};
use tempfile::TempDir;
use tower::ServiceExt;

const WRITER: &str = "writer-key";
const READER: &str = "reader-key";

struct Api {
    router: axum::Router,
    state: AppState,
    _dir: TempDir,
}

/// Status, headers and raw body text, for transports that are not JSON.
struct Raw {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: String,
}

impl Raw {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// The JSON-RPC messages carried by an SSE body, in order.
    fn sse_messages(&self) -> Vec<Json> {
        self.body
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|data| serde_json::from_str(data.trim()).unwrap())
            .collect()
    }
}

impl Api {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(EngineConfig::new(dir.path())).unwrap());
        let auth = AuthRegistry::new()
            .with_key(
                ApiKey::new(WRITER, TenantId::new("acme").unwrap())
                    .read_write()
                    .with_limits(QueryLimits::unlimited()),
            )
            .with_key(
                ApiKey::new(READER, TenantId::new("acme").unwrap())
                    .with_scopes([Scope::DatabaseRead, Scope::SchemaRead])
                    .with_limits(QueryLimits {
                        max_rows: 3,
                        ..QueryLimits::default()
                    }),
            )
            .with_local_identity(ApiKey::new("local", TenantId::new("acme").unwrap()).read_write());
        let state = AppState::new(engine, auth)
            .with_allowed_origins(["https://app.example.com".to_string()]);
        Self {
            router: router(state.clone()),
            state,
            _dir: dir,
        }
    }

    /// Send a request with arbitrary headers and read the body as text.
    async fn raw(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<Json>,
    ) -> Raw {
        let mut builder = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = match body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Raw {
            status,
            headers,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        }
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        key: Option<&str>,
        body: Option<Json>,
    ) -> (StatusCode, Json) {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(key) = key {
            builder = builder.header("authorization", format!("Bearer {key}"));
        }
        let request = match body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            Json::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)))
        };
        (status, json)
    }

    async fn get(&self, path: &str, key: Option<&str>) -> (StatusCode, Json) {
        self.send("GET", path, key, None).await
    }

    async fn post(&self, path: &str, key: Option<&str>, body: Json) -> (StatusCode, Json) {
        self.send("POST", path, key, Some(body)).await
    }

    /// Create the demo database and table, and load rows.
    async fn seed(&self) {
        let (status, _) = self
            .post("/v1/databases", Some(WRITER), json!({ "database": "crm" }))
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = self
            .post(
                "/v1/databases/crm/tables",
                Some(WRITER),
                json!({
                    "table": "leads",
                    "primary_key": ["id"],
                    "columns": [
                        { "name": "id", "type": "int64", "nullable": false, "semantic_type": "id" },
                        { "name": "country", "type": "utf8", "semantic_type": "country" },
                        { "name": "value", "type": "float64", "semantic_type": "currency",
                          "default_aggregation": "sum" },
                        { "name": "secret", "type": "utf8", "sensitive": true }
                    ]
                }),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        const COUNTRIES: [&str; 3] = ["uae", "usa", "uk"];
        let rows: Vec<Json> = (0..30)
            .map(|i| {
                json!({ "id": i, "country": COUNTRIES[(i % 3) as usize],
                        "value": (i * 10) as f64, "secret": "hidden" })
            })
            .collect();
        let (status, body) = self
            .post(
                "/v1/databases/crm/tables/leads/rows",
                Some(WRITER),
                json!({ "rows": rows }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows_written"], json!(30));
    }
}

#[tokio::test]
async fn healthz_needs_no_credentials_and_reports_the_build() {
    let api = Api::new();
    let (status, body) = api.get("/healthz", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ok"));
    assert!(body["version"].is_string());
    assert!(body["natural_language"].is_string());
}

#[tokio::test]
async fn every_data_route_requires_a_valid_key() {
    let api = Api::new();
    for (method, path) in [
        ("GET", "/v1/databases"),
        ("GET", "/v1/databases/crm/tables"),
        ("POST", "/v1/databases/crm/query"),
    ] {
        let (status, body) = api
            .send(method, path, None, Some(json!({ "request": "x" })))
            .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} was not protected"
        );
        assert_eq!(body["error"]["code"], json!("permission_denied"));

        let (status, _) = api
            .send(
                method,
                path,
                Some("guessed-key"),
                Some(json!({ "request": "x" })),
            )
            .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} accepted a bad key"
        );
    }
}

#[tokio::test]
async fn the_documented_workflow_works_over_rest() {
    let api = Api::new();
    api.seed().await;

    let (status, body) = api.get("/v1/databases", Some(WRITER)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["databases"], json!(["crm"]));

    let (_, body) = api.get("/v1/databases/crm/tables", Some(WRITER)).await;
    assert_eq!(body["tables"][0]["table"], json!("leads"));
    assert_eq!(body["tables"][0]["rows"], json!(30));

    let (_, body) = api
        .get("/v1/databases/crm/tables/leads", Some(WRITER))
        .await;
    assert_eq!(body["schema"]["primary_key"], json!(["id"]));
    assert_eq!(body["stats"]["rows"], json!(30));

    // Natural language.
    let (status, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(WRITER),
            json!({ "request": "total value by country in leads" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
    assert!(body["interpretation"].is_string());

    // Structured plan.
    let (status, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(WRITER),
            json!({ "plan": { "operation": "aggregate", "table": "leads",
                              "metrics": [{ "function": "count" }] } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["count"], json!(30));

    // Point lookup, upsert, delete.
    let (_, body) = api
        .post(
            "/v1/databases/crm/tables/leads/rows/get",
            Some(WRITER),
            json!({ "keys": [1, { "id": 2 }] }),
        )
        .await;
    assert_eq!(body["found"], json!(2));

    let (status, body) = api
        .send(
            "PUT",
            "/v1/databases/crm/tables/leads/rows",
            Some(WRITER),
            Some(json!({ "rows": [{ "id": 1, "country": "sg", "value": 99.0, "secret": "x" }] })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (_, body) = api
        .post(
            "/v1/databases/crm/tables/leads/rows/delete",
            Some(WRITER),
            json!({ "keys": [{ "id": 0 }] }),
        )
        .await;
    assert_eq!(body["deleted"], json!(1));

    let (_, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(WRITER),
            json!({ "request": "how many leads" }),
        )
        .await;
    assert_eq!(body["rows"][0]["count"], json!(29));
}

#[tokio::test]
async fn explain_shows_the_plan_without_running_it() {
    let api = Api::new();
    api.seed().await;
    let (status, body) = api
        .post(
            "/v1/databases/crm/explain",
            Some(WRITER),
            json!({ "request": "total value by country in leads" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["plan"]["group_by"], json!(["country"]));
    let physical = body["physical"].as_str().unwrap();
    assert!(physical.contains("scan leads"), "{physical}");
    assert!(physical.contains("aggregate"), "{physical}");
}

#[tokio::test]
async fn error_statuses_distinguish_the_kind_of_problem() {
    let api = Api::new();
    api.seed().await;

    // Unknown table.
    let (status, body) = api
        .get("/v1/databases/crm/tables/ghost", Some(WRITER))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("not_found"));

    // Duplicate database.
    let (status, body) = api
        .post("/v1/databases", Some(WRITER), json!({ "database": "crm" }))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], json!("already_exists"));

    // A join: understood, but not executable.
    let (status, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(WRITER),
            json!({ "request": "leads joined with customers" }),
        )
        .await;
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "{status} {body}"
    );

    // Malformed request.
    let (status, body) = api
        .post("/v1/databases/crm/query", Some(WRITER), json!({}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]["message"].as_str().unwrap().contains("plan"),
        "{body}"
    );

    // A write with a read-only key.
    let (status, body) = api
        .post(
            "/v1/databases/crm/tables/leads/rows",
            Some(READER),
            json!({ "rows": [{ "id": 500 }] }),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("data:insert"),
        "{body}"
    );
}

#[tokio::test]
async fn per_key_limits_apply_to_rest_too() {
    let api = Api::new();
    api.seed().await;
    let (status, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(READER),
            json!({ "plan": { "table": "leads" } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"].as_array().unwrap().len(),
        3,
        "the reader's row budget is 3"
    );
    assert_eq!(body["stats"]["truncated"], json!(true));
}

#[tokio::test]
async fn sensitive_columns_are_not_returned_by_default() {
    let api = Api::new();
    api.seed().await;
    let (_, body) = api
        .post(
            "/v1/databases/crm/query",
            Some(WRITER),
            json!({ "plan": { "table": "leads", "limit": 1 } }),
        )
        .await;
    assert!(
        body["rows"][0].get("secret").is_none(),
        "{}",
        body["rows"][0]
    );
}

#[tokio::test]
async fn mcp_over_http_is_the_same_server_as_stdio() {
    let api = Api::new();
    api.seed().await;

    let (status, body) = api
        .post(
            "/v1/mcp",
            Some(WRITER),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["tools"].as_array().unwrap().len(), 14);

    let (status, body) = api
        .post(
            "/v1/mcp",
            Some(WRITER),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": { "name": "data_query",
                                "arguments": { "database": "crm", "request": "how many leads" } } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], json!(false), "{body}");
    assert_eq!(
        body["result"]["structuredContent"]["rows"][0]["count"],
        json!(30)
    );

    // A notification is accepted with no body.
    let (status, _) = api
        .post(
            "/v1/mcp",
            Some(WRITER),
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

const AUTH: (&str, &str) = ("authorization", "Bearer writer-key");
const ACCEPT_BOTH: (&str, &str) = ("accept", "application/json, text/event-stream");

#[tokio::test]
async fn streamable_http_answers_a_request_as_an_event_stream_when_accepted() {
    let api = Api::new();
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ACCEPT_BOTH],
            Some(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::OK);
    assert!(
        raw.header("content-type")
            .unwrap()
            .starts_with("text/event-stream"),
        "{:?}",
        raw.headers
    );
    assert!(raw.body.contains("event: message"), "{}", raw.body);
    let messages = raw.sse_messages();
    assert_eq!(messages.len(), 1, "{}", raw.body);
    assert_eq!(messages[0]["id"], json!(1));
    assert_eq!(messages[0]["result"]["tools"].as_array().unwrap().len(), 14);

    // Without text/event-stream in Accept, the same request is plain JSON.
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ("accept", "application/json")],
            Some(json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::OK);
    assert_eq!(raw.header("content-type"), Some("application/json"));
    let body: Json = serde_json::from_str(&raw.body).unwrap();
    assert_eq!(body["id"], json!(2));

    // A client's response to a server request is acknowledged, not parsed.
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ACCEPT_BOTH],
            Some(json!({ "jsonrpc": "2.0", "id": 7, "result": {} })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::ACCEPTED);
    assert!(raw.body.is_empty());
}

#[tokio::test]
async fn streamable_http_sessions_are_issued_checked_and_ended() {
    let api = Api::new();
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ACCEPT_BOTH],
            Some(json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": { "protocolVersion": "2025-06-18" } })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::OK);
    let session = raw
        .header("mcp-session-id")
        .expect("initialize issues a session")
        .to_string();
    assert_eq!(
        raw.sse_messages()[0]["result"]["protocolVersion"],
        json!("2025-06-18")
    );

    let with_session = [
        AUTH,
        ACCEPT_BOTH,
        ("mcp-session-id", session.as_str()),
        ("mcp-protocol-version", "2025-06-18"),
    ];
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &with_session,
            Some(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::ACCEPTED);

    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ACCEPT_BOTH, ("mcp-session-id", "not-a-session")],
            Some(json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::NOT_FOUND);

    let raw = api.raw("DELETE", "/mcp", &[AUTH], None).await;
    assert_eq!(
        raw.status,
        StatusCode::BAD_REQUEST,
        "DELETE needs a session"
    );

    let raw = api.raw("DELETE", "/mcp", &with_session, None).await;
    assert_eq!(raw.status, StatusCode::NO_CONTENT);

    // An ended session is gone: the client must initialize again.
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &with_session,
            Some(json!({ "jsonrpc": "2.0", "id": 3, "method": "ping" })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn streamable_http_get_streams_until_shutdown() {
    let api = Api::new();

    let raw = api.raw("GET", "/mcp", &[AUTH], None).await;
    assert_eq!(raw.status, StatusCode::METHOD_NOT_ALLOWED);

    let request = Request::builder()
        .method("GET")
        .uri("/mcp")
        .header(AUTH.0, AUTH.1)
        .header("accept", "text/event-stream")
        .body(Body::empty())
        .unwrap();
    let response = api.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));

    // The stream stays open until shutdown begins, then ends by itself, so a
    // graceful stop is not held for the full drain timeout.
    let body = tokio::spawn(axum::body::to_bytes(response.into_body(), usize::MAX));
    api.state.begin_shutdown();
    let finished = tokio::time::timeout(std::time::Duration::from_secs(5), body)
        .await
        .expect("the event stream should end on shutdown");
    assert!(finished.unwrap().is_ok());
}

#[tokio::test]
async fn streamable_http_refuses_foreign_origins_and_unknown_versions() {
    let api = Api::new();
    let ping = json!({ "jsonrpc": "2.0", "id": 1, "method": "ping" });

    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ("origin", "https://evil.example")],
            Some(ping.clone()),
        )
        .await;
    assert_eq!(raw.status, StatusCode::FORBIDDEN, "{}", raw.body);

    for origin in ["http://localhost:3000", "https://app.example.com"] {
        let raw = api
            .raw(
                "POST",
                "/mcp",
                &[AUTH, ("origin", origin)],
                Some(ping.clone()),
            )
            .await;
        assert_eq!(raw.status, StatusCode::OK, "{origin}: {}", raw.body);
    }

    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ("mcp-protocol-version", "1999-01-01")],
            Some(ping.clone()),
        )
        .await;
    assert_eq!(raw.status, StatusCode::BAD_REQUEST, "{}", raw.body);

    // Authentication still applies to every message.
    let raw = api.raw("POST", "/mcp", &[ACCEPT_BOTH], Some(ping)).await;
    assert!(raw.status.is_client_error(), "{}", raw.status);
    assert_ne!(raw.status, StatusCode::OK);
}

#[tokio::test]
async fn streamable_http_sessions_belong_to_the_key_that_opened_them() {
    let api = Api::new();
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH],
            Some(json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": { "protocolVersion": "2025-06-18" } })),
        )
        .await;
    let session = raw.header("mcp-session-id").unwrap().to_string();
    let ping = json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" });

    // Without credentials, a live session looks exactly like a missing one:
    // authentication fails before the session is consulted.
    let anonymous = api
        .raw(
            "POST",
            "/mcp",
            &[("mcp-session-id", session.as_str())],
            Some(ping.clone()),
        )
        .await;
    let anonymous_missing = api
        .raw(
            "POST",
            "/mcp",
            &[("mcp-session-id", "not-a-session")],
            Some(ping.clone()),
        )
        .await;
    assert_eq!(anonymous.status, anonymous_missing.status);
    assert_ne!(anonymous.status, StatusCode::NOT_FOUND);

    // Another key can neither use the session nor end it.
    let other = ("authorization", "Bearer reader-key");
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[other, ("mcp-session-id", session.as_str())],
            Some(ping.clone()),
        )
        .await;
    assert_eq!(raw.status, StatusCode::NOT_FOUND);
    let raw = api
        .raw(
            "DELETE",
            "/mcp",
            &[other, ("mcp-session-id", session.as_str())],
            None,
        )
        .await;
    assert_eq!(raw.status, StatusCode::NOT_FOUND);

    // The owner still can.
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, ("mcp-session-id", session.as_str())],
            Some(ping),
        )
        .await;
    assert_eq!(raw.status, StatusCode::OK, "{}", raw.body);
}

#[tokio::test]
async fn streamable_http_answers_cors_for_allowed_origins_only() {
    let api = Api::new();
    let allowed = ("origin", "https://app.example.com");

    let raw = api
        .raw(
            "OPTIONS",
            "/mcp",
            &[
                allowed,
                ("access-control-request-method", "POST"),
                (
                    "access-control-request-headers",
                    "authorization, content-type",
                ),
            ],
            None,
        )
        .await;
    assert_eq!(raw.status, StatusCode::NO_CONTENT);
    assert_eq!(
        raw.header("access-control-allow-origin"),
        Some("https://app.example.com")
    );
    assert!(raw
        .header("access-control-allow-headers")
        .unwrap()
        .contains("authorization"));
    assert!(raw
        .header("access-control-allow-methods")
        .unwrap()
        .contains("POST"));

    let raw = api
        .raw(
            "OPTIONS",
            "/mcp",
            &[("origin", "https://evil.example")],
            None,
        )
        .await;
    assert_eq!(raw.status, StatusCode::FORBIDDEN);
    assert!(raw.header("access-control-allow-origin").is_none());

    // The real request exposes the session header to the page.
    let raw = api
        .raw(
            "POST",
            "/mcp",
            &[AUTH, allowed],
            Some(json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": { "protocolVersion": "2025-06-18" } })),
        )
        .await;
    assert_eq!(raw.status, StatusCode::OK);
    assert_eq!(
        raw.header("access-control-allow-origin"),
        Some("https://app.example.com")
    );
    assert_eq!(
        raw.header("access-control-expose-headers"),
        Some("mcp-session-id")
    );
    assert!(raw.header("mcp-session-id").is_some());
}

#[tokio::test]
async fn the_tool_catalogue_is_discoverable() {
    let api = Api::new();
    let (status, body) = api.get("/v1/tools", None).await;
    assert_eq!(status, StatusCode::OK);
    let tools = body["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 14);
    assert!(tools.iter().any(|t| t["name"] == json!("data_query")));
}

#[tokio::test]
async fn schema_evolution_over_rest() {
    let api = Api::new();
    api.seed().await;
    let (status, body) = api
        .send(
            "PATCH",
            "/v1/databases/crm/tables/leads/schema",
            Some(WRITER),
            Some(json!({
                "columns": [
                    { "name": "id", "type": "int64", "nullable": false, "semantic_type": "id" },
                    { "name": "country", "type": "utf8", "semantic_type": "country" },
                    { "name": "value", "type": "float64", "semantic_type": "currency",
                      "default_aggregation": "sum" },
                    { "name": "secret", "type": "utf8", "sensitive": true },
                    { "name": "source", "type": "utf8" }
                ]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["version"], json!(2));
}

#[tokio::test]
async fn tenants_are_isolated_across_keys() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(dir.path())).unwrap());
    let auth = AuthRegistry::new()
        .with_key(ApiKey::new("acme-key", TenantId::new("acme").unwrap()).read_write())
        .with_key(ApiKey::new("other-key", TenantId::new("othercorp").unwrap()).read_write())
        .with_local_identity(ApiKey::new("local", TenantId::new("acme").unwrap()).read_write());
    let state = AppState::new(engine, auth);
    let api = Api {
        router: router(state.clone()),
        state,
        _dir: dir,
    };

    api.post(
        "/v1/databases",
        Some("acme-key"),
        json!({ "database": "shared" }),
    )
    .await;
    // The same name in another tenant is a different database, not a conflict.
    let (status, _) = api
        .post(
            "/v1/databases",
            Some("other-key"),
            json!({ "database": "shared" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, body) = api.get("/v1/databases", Some("other-key")).await;
    assert_eq!(body["databases"], json!(["shared"]));

    // And one tenant cannot see the other's tables.
    api.post(
        "/v1/databases/shared/tables",
        Some("acme-key"),
        json!({ "table": "t", "columns": [{ "name": "id", "type": "int64" }] }),
    )
    .await;
    let (_, body) = api
        .get("/v1/databases/shared/tables", Some("other-key"))
        .await;
    assert_eq!(body["tables"].as_array().unwrap().len(), 0);
    let _ = DatabaseName::new("shared").unwrap();
}
