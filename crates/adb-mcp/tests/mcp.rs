//! MCP conformance and behaviour tests.
//!
//! Everything here goes through real JSON-RPC frames, the same bytes a client
//! sends, rather than calling the tool functions directly, so the protocol
//! surface is covered too.

use std::sync::Arc;

use adb_core::{DatabaseName, QueryLimits, Scope, TenantId};
use adb_engine::{Engine, EngineConfig};
use adb_mcp::{ApiKey, AuthRegistry, McpServer, PROTOCOL_VERSION};
use serde_json::{json, Value as Json};
use tempfile::TempDir;

struct Client {
    server: Arc<McpServer>,
    _dir: TempDir,
    next_id: std::cell::Cell<i64>,
}

impl Client {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(EngineConfig::new(dir.path())).unwrap());
        let auth = AuthRegistry::new()
            .with_local_identity(
                ApiKey::new("local", TenantId::new("acme").unwrap())
                    .read_write()
                    .with_limits(QueryLimits::unlimited()),
            )
            .with_key(
                ApiKey::new("reader-key", TenantId::new("acme").unwrap())
                    .with_default_database(DatabaseName::new("crm").unwrap()),
            );
        Self {
            server: Arc::new(McpServer::new(engine, auth)),
            _dir: dir,
            next_id: std::cell::Cell::new(0),
        }
    }

    fn send(&self, method: &str, params: Json) -> Json {
        let id = self.next_id.get() + 1;
        self.next_id.set(id);
        let line =
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
        let ctx = self.server.local_context().unwrap();
        let response = self
            .server
            .handle_line(&line, &ctx)
            .unwrap_or_else(|| panic!("{method} should produce a response"));
        let parsed: Json = serde_json::from_str(&response).unwrap();
        assert_eq!(parsed["jsonrpc"], json!("2.0"));
        assert_eq!(parsed["id"], json!(id));
        parsed
    }

    /// Call a tool and require success.
    fn call(&self, name: &str, arguments: Json) -> Json {
        let response = self.send(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = &response["result"];
        assert_eq!(
            result["isError"],
            json!(false),
            "{name} failed: {}",
            result["content"][0]["text"]
        );
        result["structuredContent"].clone()
    }

    /// Call a tool and require a tool-level error; returns its message.
    fn call_err(&self, name: &str, arguments: Json) -> String {
        let response = self.send(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = &response["result"];
        assert_eq!(
            result["isError"],
            json!(true),
            "{name} unexpectedly succeeded: {result}"
        );
        result["content"][0]["text"].as_str().unwrap().to_string()
    }

    /// The full setup an agent would do first.
    fn seed(&self) {
        self.call("database_create", json!({ "database": "crm" }));
        self.call(
            "table_create",
            json!({
                "database": "crm",
                "table": "leads",
                "description": "One row per inbound lead",
                "primary_key": ["id"],
                "partitions": 2,
                "columns": [
                    { "name": "id", "type": "int64", "nullable": false, "semantic_type": "id" },
                    { "name": "company", "type": "utf8", "semantic_type": "category" },
                    { "name": "country", "type": "utf8", "semantic_type": "country" },
                    { "name": "value", "type": "float64", "semantic_type": "currency",
                      "currency": "USD", "default_aggregation": "sum",
                      "description": "Expected deal value" },
                    { "name": "created_at", "type": "timestamp", "semantic_type": "timestamp" },
                    { "name": "contact_email", "type": "utf8", "semantic_type": "email",
                      "sensitive": true }
                ]
            }),
        );
        let rows: Vec<Json> = (0..40)
            .map(|i| {
                const COUNTRIES: [&str; 4] = ["uae", "usa", "uk", "sg"];
                let country = COUNTRIES[(i % 4) as usize];
                json!({
                    "id": i,
                    "company": format!("company {}", i % 8),
                    "country": country,
                    "value": (i * 100) as f64,
                    "created_at": "2026-01-01T00:00:00Z",
                    "contact_email": format!("lead{i}@example.com")
                })
            })
            .collect();
        let written = self.call("data_insert", json!({ "table": "leads", "rows": rows }));
        assert_eq!(written["rows_written"], json!(40));
    }
}

#[test]
fn initialize_negotiates_and_describes_the_server() {
    let client = Client::new();
    let response = client.send(
        "initialize",
        json!({ "protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                "clientInfo": { "name": "test", "version": "1" } }),
    );
    let result = &response["result"];
    assert_eq!(result["protocolVersion"], json!(PROTOCOL_VERSION));
    assert_eq!(result["serverInfo"]["name"], json!("agedb"));
    assert!(result["capabilities"]["tools"].is_object());
    assert!(
        result["instructions"]
            .as_str()
            .unwrap()
            .contains("data_query"),
        "instructions should tell the agent where to start"
    );

    // An older supported revision is honoured; an unknown one gets ours.
    let older = client.send("initialize", json!({ "protocolVersion": "2024-11-05" }));
    assert_eq!(older["result"]["protocolVersion"], json!("2024-11-05"));
    let unknown = client.send("initialize", json!({ "protocolVersion": "1999-01-01" }));
    assert_eq!(
        unknown["result"]["protocolVersion"],
        json!(PROTOCOL_VERSION)
    );
}

#[test]
fn tools_list_advertises_the_documented_surface() {
    let client = Client::new();
    let response = client.send("tools/list", json!({}));
    let tools = response["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 14);
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "database_create",
        "database_list",
        "database_delete",
        "table_create",
        "table_list",
        "table_describe",
        "table_drop",
        "schema_get",
        "schema_update",
        "data_insert",
        "data_upsert",
        "data_get",
        "data_delete",
        "data_query",
    ] {
        assert!(
            names.contains(&expected),
            "{expected} is missing from tools/list"
        );
    }
    for tool in tools {
        assert!(tool["inputSchema"]["type"] == json!("object"));
        assert!(!tool["description"].as_str().unwrap().is_empty());
    }
}

#[test]
fn notifications_get_no_response_and_ping_does() {
    let client = Client::new();
    let ctx = client.server.local_context().unwrap();
    let notification =
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string();
    assert!(client.server.handle_line(&notification, &ctx).is_none());
    assert_eq!(client.send("ping", json!({}))["result"], json!({}));
}

#[test]
fn protocol_errors_are_json_rpc_errors() {
    let client = Client::new();
    let ctx = client.server.local_context().unwrap();

    let response = client.server.handle_line("{not json", &ctx).unwrap();
    let parsed: Json = serde_json::from_str(&response).unwrap();
    assert_eq!(parsed["error"]["code"], json!(-32700));

    let response = client.send("no/such/method", json!({}));
    assert_eq!(response["error"]["code"], json!(-32601));

    let response = client.send("tools/call", json!({ "arguments": {} }));
    assert_eq!(response["error"]["code"], json!(-32602));
}

#[test]
fn the_whole_agent_workflow_runs_over_mcp() {
    let client = Client::new();
    client.seed();

    let listed = client.call("table_list", json!({ "database": "crm" }));
    assert_eq!(listed["tables"][0]["table"], json!("leads"));
    assert_eq!(listed["tables"][0]["rows"], json!(40));

    let described = client.call("table_describe", json!({ "table": "leads" }));
    assert_eq!(described["schema"]["primary_key"], json!(["id"]));
    assert_eq!(described["stats"]["partitions"], json!(2));

    // Plain-language query.
    let outcome = client.call(
        "data_query",
        json!({ "request": "total value by country in leads" }),
    );
    assert_eq!(outcome["rows"].as_array().unwrap().len(), 4);
    assert!(outcome["interpretation"]
        .as_str()
        .unwrap()
        .contains("grouped by country"));
    // The plan that ran is echoed back, so the agent can reuse it.
    assert_eq!(outcome["plan"]["operation"], json!("aggregate"));
    assert_eq!(outcome["plan"]["group_by"], json!(["country"]));

    // Structured query: same question, exact control.
    let outcome = client.call(
        "data_query",
        json!({
            "plan": {
                "operation": "aggregate",
                "table": "leads",
                "group_by": ["country"],
                "metrics": [{ "function": "sum", "column": "value", "alias": "pipeline" }],
                "order_by": [{ "column": "pipeline", "direction": "desc" }],
                "limit": 2
            }
        }),
    );
    let rows = outcome["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0]["pipeline"].as_f64().unwrap() >= rows[1]["pipeline"].as_f64().unwrap());
    assert!(outcome["stats"]["elapsed_ms"].is_number());

    // Update, read back, delete.
    client.call(
        "data_upsert",
        json!({ "table": "leads", "rows": [{ "id": 1, "company": "acme", "country": "de",
                                             "value": 5000.0, "created_at": "2026-02-01T00:00:00Z",
                                             "contact_email": "a@example.com" }] }),
    );
    let fetched = client.call(
        "data_get",
        json!({ "table": "leads", "keys": [1, { "id": 2 }] }),
    );
    assert_eq!(fetched["found"], json!(2));
    assert_eq!(fetched["rows"][0]["company"], json!("acme"));

    let deleted = client.call(
        "data_delete",
        json!({ "table": "leads", "keys": [{ "id": 0 }] }),
    );
    assert_eq!(deleted["deleted"], json!(1));

    let counted = client.call("data_query", json!({ "request": "how many leads" }));
    assert_eq!(counted["rows"][0]["count"], json!(39));
}

#[test]
fn tool_errors_are_results_the_model_can_read() {
    let client = Client::new();
    client.seed();

    // Unknown column: the message should name the real ones.
    let message = client.call_err(
        "data_query",
        json!({ "plan": { "table": "leads", "filters": [
            { "column": "revenue", "op": "gt", "value": 1 }] } }),
    );
    assert!(message.contains("revenue"), "{message}");
    assert!(message.contains("not_found"), "{message}");

    // Unknown table: the message should list the tables that exist.
    let message = client.call_err("data_query", json!({ "plan": { "table": "ordrs" } }));
    assert!(message.contains("leads"), "{message}");

    // Unknown column on insert.
    let message = client.call_err(
        "data_insert",
        json!({ "table": "leads", "rows": [{ "id": 100, "cuontry": "uae" }] }),
    );
    assert!(message.contains("cuontry"), "{message}");

    // Meaningless aggregation.
    let message = client.call_err(
        "data_query",
        json!({ "plan": { "operation": "aggregate", "table": "leads",
                          "metrics": [{ "function": "sum", "column": "id" }] } }),
    );
    assert!(message.contains("not a measure"), "{message}");

    // Neither request nor plan.
    let message = client.call_err("data_query", json!({ "table": "leads" }));
    assert!(message.contains("request"), "{message}");

    // A join.
    let message = client.call_err(
        "data_query",
        json!({ "request": "leads joined with customers" }),
    );
    assert!(
        message.contains("could not turn") || message.contains("join"),
        "{message}"
    );
}

#[test]
fn scopes_are_enforced_per_key_not_per_argument() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(dir.path())).unwrap());
    let auth = AuthRegistry::new()
        .with_local_identity(ApiKey::new("local", TenantId::new("acme").unwrap()).read_write())
        .with_key(
            ApiKey::new("reader", TenantId::new("acme").unwrap())
                .with_scopes([Scope::DatabaseRead, Scope::SchemaRead])
                .with_default_database(DatabaseName::new("crm").unwrap()),
        );
    let server = Arc::new(McpServer::new(engine, auth));

    // Set up with the writer identity.
    let writer = server.local_context().unwrap();
    for line in [
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"database_create","arguments":{"database":"crm"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"table_create","arguments":{"database":"crm","table":"t","columns":[{"name":"id","type":"int64","nullable":false}],"primary_key":["id"]}}}),
    ] {
        let response = server.handle_line(&line.to_string(), &writer).unwrap();
        let parsed: Json = serde_json::from_str(&response).unwrap();
        assert_eq!(parsed["result"]["isError"], json!(false), "{parsed}");
    }

    // The read-only key can query but not write.
    let reader = server.context_for_header(Some("Bearer reader")).unwrap();
    let query = json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"data_query","arguments":{"request":"how many t"}}});
    let response: Json =
        serde_json::from_str(&server.handle_line(&query.to_string(), &reader).unwrap()).unwrap();
    assert_eq!(response["result"]["isError"], json!(false), "{response}");

    let insert = json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"data_insert","arguments":{"table":"t","rows":[{"id":1}]}}});
    let response: Json =
        serde_json::from_str(&server.handle_line(&insert.to_string(), &reader).unwrap()).unwrap();
    assert_eq!(response["result"]["isError"], json!(true));
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("data:insert"), "{text}");

    // An unknown key gets no context at all.
    assert!(server.context_for_header(Some("Bearer guessed")).is_err());
}

#[test]
fn sensitive_columns_stay_out_of_default_results_and_context() {
    let client = Client::new();
    client.seed();

    let outcome = client.call(
        "data_query",
        json!({ "plan": { "table": "leads" }, "limit": 1 }),
    );
    let row = &outcome["rows"][0];
    assert!(row.get("contact_email").is_none(), "{row}");

    let schema = client.call("schema_get", json!({ "database": "crm" }));
    assert!(!schema["context"]
        .as_str()
        .unwrap()
        .contains("contact_email"));

    // Naming it explicitly still works: this is an authorization decision, not
    // a hidden column.
    let outcome = client.call(
        "data_query",
        json!({ "plan": { "table": "leads", "columns": ["contact_email"], "limit": 1 } }),
    );
    assert!(outcome["rows"][0]["contact_email"].is_string());
}

#[test]
fn dangerous_operations_require_explicit_confirmation() {
    let client = Client::new();
    client.seed();
    let message = client.call_err("database_delete", json!({ "database": "crm" }));
    assert!(message.contains("cascade"), "{message}");
    let deleted = client.call(
        "database_delete",
        json!({ "database": "crm", "cascade": true }),
    );
    assert_eq!(deleted["tables_deleted"], json!(1));
}

#[test]
fn schema_evolution_over_mcp() {
    let client = Client::new();
    client.seed();
    let updated = client.call(
        "schema_update",
        json!({
            "table": "leads",
            "columns": [
                { "name": "id", "type": "int64", "nullable": false, "semantic_type": "id" },
                { "name": "company", "type": "utf8", "semantic_type": "category" },
                { "name": "country", "type": "utf8", "semantic_type": "country" },
                { "name": "value", "type": "float64", "semantic_type": "currency",
                  "currency": "USD", "default_aggregation": "sum" },
                { "name": "created_at", "type": "timestamp", "semantic_type": "timestamp" },
                { "name": "contact_email", "type": "utf8", "semantic_type": "email",
                  "sensitive": true },
                { "name": "source", "type": "utf8", "description": "campaign the lead came from" }
            ]
        }),
    );
    assert_eq!(updated["version"], json!(2));

    // Dropping a column is refused rather than silently losing data.
    let message = client.call_err(
        "schema_update",
        json!({ "table": "leads",
                "columns": [{ "name": "id", "type": "int64", "nullable": false }] }),
    );
    assert!(message.contains("dropping column"), "{message}");
}

/// The stdio transport, end to end over pipes.
#[test]
fn the_stdio_transport_speaks_line_delimited_json_rpc() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(dir.path())).unwrap());
    let auth = AuthRegistry::new()
        .with_local_identity(ApiKey::new("local", TenantId::new("acme").unwrap()).read_write());
    let server = Arc::new(McpServer::new(engine, auth));

    let session = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":PROTOCOL_VERSION}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"database_create","arguments":{"database":"demo"}}}),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"table_create","arguments":{"database":"demo","table":"events","columns":[{"name":"at","type":"timestamp","nullable":false},{"name":"kind","type":"utf8","semantic_type":"category"}]}}}),
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"data_insert","arguments":{"database":"demo","table":"events","rows":[{"at":"2026-01-01T00:00:00Z","kind":"click"},{"at":"2026-01-02T00:00:00Z","kind":"view"}]}}}),
        json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"data_query","arguments":{"database":"demo","request":"count events by kind"}}}),
    ]
    .iter()
    .map(|m| m.to_string())
    .collect::<Vec<_>>()
    .join("\n");

    let mut output = Vec::new();
    adb_mcp::stdio::serve(server, session.as_bytes(), &mut output).unwrap();
    let text = String::from_utf8(output).unwrap();
    let responses: Vec<Json> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    // Six requests, one notification: six responses.
    assert_eq!(responses.len(), 6, "got:\n{text}");
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], json!("agedb"));
    assert_eq!(
        responses[1]["result"]["tools"].as_array().unwrap().len(),
        14
    );
    for response in &responses[2..] {
        assert_eq!(response["result"]["isError"], json!(false), "{response}");
    }
    let last = &responses[5]["result"]["structuredContent"];
    assert_eq!(last["rows"].as_array().unwrap().len(), 2);
    assert_eq!(last["schema"]["columns"][0]["name"], json!("kind"));
}
