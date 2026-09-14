//! REST routes.

use std::sync::Arc;

use adb_core::{AdbError, DatabaseName, RequestContext, TableName};
use adb_engine::{Engine, QuerySource};
use adb_mcp::{AuthRegistry, McpServer};
use adb_query::PlanRequest;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::error::{ApiError, ApiResult};

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub auth: Arc<AuthRegistry>,
    pub mcp: Arc<McpServer>,
}

impl AppState {
    pub fn new(engine: Arc<Engine>, auth: AuthRegistry) -> Self {
        let auth = Arc::new(auth);
        let mcp = Arc::new(McpServer::new(engine.clone(), (*auth).clone()));
        Self { engine, auth, mcp }
    }

    /// Authenticate a request and scope it to `database`.
    fn context(&self, headers: &HeaderMap, database: Option<&str>) -> ApiResult<RequestContext> {
        let header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        let mut ctx = self.auth.context_for_header(header)?;
        if let Some(database) = database {
            ctx.database = Some(DatabaseName::new(database)?);
        }
        Ok(ctx)
    }
}

/// Run a blocking engine operation off the async runtime.
async fn blocking<T, F>(work: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> adb_core::Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result.map_err(ApiError::from),
        Err(join) => Err(ApiError(AdbError::internal(format!(
            "the request task failed: {join}"
        )))),
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/tools", get(list_tools))
        .route("/v1/mcp", post(mcp_over_http))
        .route("/v1/databases", get(list_databases).post(create_database))
        .route("/v1/databases/{database}", delete(delete_database))
        .route(
            "/v1/databases/{database}/tables",
            get(list_tables).post(create_table),
        )
        .route(
            "/v1/databases/{database}/tables/{table}",
            get(describe_table).delete(drop_table),
        )
        .route(
            "/v1/databases/{database}/tables/{table}/schema",
            patch(update_schema),
        )
        .route(
            "/v1/databases/{database}/tables/{table}/rows",
            post(insert_rows).put(upsert_rows),
        )
        .route(
            "/v1/databases/{database}/tables/{table}/rows/get",
            post(get_rows),
        )
        .route(
            "/v1/databases/{database}/tables/{table}/rows/delete",
            post(delete_rows),
        )
        .route("/v1/databases/{database}/query", post(query))
        .route("/v1/databases/{database}/explain", post(explain))
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Json<JsonValue> {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "natural_language": state.engine.translator_name(),
        "catalog_epoch": state.engine.snapshot().epoch,
    }))
}

async fn list_tools() -> Json<JsonValue> {
    Json(json!({ "tools": adb_mcp::tools::definitions_json() }))
}

/// MCP JSON-RPC over HTTP: the same handler the stdio transport uses.
async fn mcp_over_http(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> ApiResult<(StatusCode, String)> {
    let ctx = state.context(&headers, None)?;
    let mcp = state.mcp.clone();
    let response = tokio::task::spawn_blocking(move || mcp.handle_line(&body, &ctx))
        .await
        .map_err(|e| ApiError(AdbError::internal(format!("the request task failed: {e}"))))?;
    Ok(match response {
        // A notification gets 202 with no body, as HTTP transports expect.
        None => (StatusCode::ACCEPTED, String::new()),
        Some(line) => (StatusCode::OK, line),
    })
}

#[derive(Debug, Deserialize)]
pub struct CreateDatabaseBody {
    pub database: String,
}

async fn create_database(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateDatabaseBody>,
) -> ApiResult<(StatusCode, Json<JsonValue>)> {
    let ctx = state.context(&headers, None)?;
    let engine = state.engine.clone();
    let name = DatabaseName::new(body.database)?;
    let created = name.clone();
    blocking(move || engine.create_database(&ctx, &name)).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "created": created.to_string() })),
    ))
}

async fn list_databases(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, None)?;
    let engine = state.engine.clone();
    let databases = blocking(move || engine.list_databases(&ctx)).await?;
    Ok(Json(json!({ "databases": databases })))
}

#[derive(Debug, Deserialize)]
pub struct CascadeParams {
    #[serde(default)]
    pub cascade: bool,
}

async fn delete_database(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Query(params): Query<CascadeParams>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, None)?;
    let engine = state.engine.clone();
    let name = DatabaseName::new(database)?;
    let dropped = blocking(move || engine.drop_database(&ctx, &name, params.cascade)).await?;
    Ok(Json(json!({ "tables_deleted": dropped })))
}

async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let tables = blocking(move || {
        let schemas = engine.list_tables(&ctx)?;
        let mut out = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let stats = engine.table_stats(&ctx, &schema.name).ok();
            out.push(json!({
                "table": schema.name.to_string(),
                "description": schema.description,
                "primary_key": schema.primary_key,
                "columns": schema.columns.len(),
                "rows": stats.as_ref().map(|s| s.rows),
                "bytes": stats.as_ref().map(|s| s.bytes),
            }));
        }
        Ok(out)
    })
    .await?;
    Ok(Json(json!({ "tables": tables })))
}

async fn create_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(body): Json<JsonValue>,
) -> ApiResult<(StatusCode, Json<JsonValue>)> {
    // Reuse the MCP tool so REST and MCP cannot diverge in what they accept.
    let result = call_tool(&state, &headers, Some(&database), "table_create", body).await?;
    Ok((StatusCode::CREATED, Json(result)))
}

async fn describe_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let name = TableName::new(table)?;
    let described = blocking(move || {
        let schema = engine.describe_table(&ctx, &name)?;
        let stats = engine.table_stats(&ctx, &name)?;
        Ok(json!({ "schema": &*schema, "stats": stats }))
    })
    .await?;
    Ok(Json(described))
}

async fn drop_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let name = TableName::new(table)?;
    let dropped = name.to_string();
    blocking(move || engine.drop_table(&ctx, &name)).await?;
    Ok(Json(json!({ "dropped": dropped })))
}

async fn update_schema(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(mut body): Json<JsonValue>,
) -> ApiResult<Json<JsonValue>> {
    if let Some(object) = body.as_object_mut() {
        object.insert("table".to_string(), json!(table));
    }
    let result = call_tool(&state, &headers, Some(&database), "schema_update", body).await?;
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
pub struct RowsBody {
    pub rows: Vec<JsonMap<String, JsonValue>>,
}

async fn insert_rows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(body): Json<RowsBody>,
) -> ApiResult<Json<JsonValue>> {
    write_rows(state, headers, database, table, body, false).await
}

async fn upsert_rows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(body): Json<RowsBody>,
) -> ApiResult<Json<JsonValue>> {
    write_rows(state, headers, database, table, body, true).await
}

async fn write_rows(
    state: AppState,
    headers: HeaderMap,
    database: String,
    table: String,
    body: RowsBody,
    upsert: bool,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let name = TableName::new(table)?;
    let outcome = blocking(move || {
        if upsert {
            engine.upsert(&ctx, &name, &body.rows)
        } else {
            engine.insert(&ctx, &name, &body.rows)
        }
    })
    .await?;
    Ok(Json(json!({
        "rows_written": outcome.rows,
        "segments_flushed": outcome.segments_flushed,
    })))
}

#[derive(Debug, Deserialize)]
pub struct KeysBody {
    pub keys: Vec<JsonValue>,
}

async fn get_rows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(body): Json<KeysBody>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let name = TableName::new(table)?;
    let requested = body.keys.len();
    let rows = blocking(move || engine.get(&ctx, &name, &body.keys)).await?;
    Ok(Json(
        json!({ "rows": rows, "found": rows.len(), "requested": requested }),
    ))
}

async fn delete_rows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(body): Json<KeysBody>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let name = TableName::new(table)?;
    let requested = body.keys.len();
    let deleted = blocking(move || engine.delete(&ctx, &name, &body.keys)).await?;
    Ok(Json(json!({ "deleted": deleted, "requested": requested })))
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    /// Natural-language question.
    #[serde(default)]
    pub request: Option<String>,
    /// Structured plan.
    #[serde(default)]
    pub plan: Option<PlanRequest>,
}

impl QueryBody {
    fn into_source(self) -> ApiResult<QuerySource> {
        match (self.request, self.plan) {
            (Some(request), None) => Ok(QuerySource::Request(request)),
            (None, Some(plan)) => Ok(QuerySource::Plan(plan)),
            (Some(_), Some(_)) => Err(ApiError(AdbError::bad_request(
                "send either \"request\" or \"plan\", not both",
            ))),
            (None, None) => Err(ApiError(AdbError::bad_request(
                "send \"request\" (plain language) or \"plan\" (structured)",
            ))),
        }
    }
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(body): Json<QueryBody>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let source = body.into_source()?;
    let engine = state.engine.clone();
    let outcome = blocking(move || engine.query(&ctx, source)).await?;
    Ok(Json(serde_json::to_value(outcome).map_err(AdbError::from)?))
}

async fn explain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(body): Json<QueryBody>,
) -> ApiResult<Json<JsonValue>> {
    let ctx = state.context(&headers, Some(&database))?;
    let engine = state.engine.clone();
    let plan = match body.into_source()? {
        QuerySource::Plan(plan) => plan,
        QuerySource::Request(request) => {
            let ctx = ctx.clone();
            let engine = engine.clone();
            blocking(move || engine.interpret(&ctx, &request)).await?
        }
    };
    let echoed = serde_json::to_value(&plan).map_err(AdbError::from)?;
    let physical = blocking(move || engine.explain(&ctx, &plan)).await?;
    Ok(Json(
        json!({ "plan": echoed, "physical": physical.explain() }),
    ))
}

/// Bridge a REST body to an MCP tool call, so both surfaces share behaviour.
async fn call_tool(
    state: &AppState,
    headers: &HeaderMap,
    database: Option<&str>,
    tool: &'static str,
    mut body: JsonValue,
) -> ApiResult<JsonValue> {
    let ctx = state.context(headers, database)?;
    if let (Some(object), Some(database)) = (body.as_object_mut(), database) {
        object.insert("database".to_string(), json!(database));
    }
    let engine = state.engine.clone();
    blocking(move || adb_mcp::tools::call(&engine, &ctx, tool, &body)).await
}
