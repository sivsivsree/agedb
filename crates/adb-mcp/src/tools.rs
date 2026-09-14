//! Tool definitions and dispatch (see "Design rationale" in ARCHITECTURE.md, 10 and 18).
//!
//! The names follow the readme's `database.create` scheme, but with underscores:
//! several MCP clients restrict tool names to `[A-Za-z0-9_-]`. The dotted forms
//! are accepted as aliases so either spelling works.
//!
//! Every description here is written for a model to read. Errors are too: when a
//! column does not exist, the message lists the ones that do, because the next
//! thing that happens is an LLM trying again.

use std::collections::BTreeMap;

use adb_core::{
    AdbError, Aggregation, ColumnRef, ColumnSchema, DataType, DatabaseName, QueryLimits,
    RequestContext, Result, SemanticType, TableName, TableSchema,
};
use adb_engine::{Engine, QuerySource};
use adb_query::PlanRequest;
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as Json};

/// Canonical tool names, in the order they are advertised.
pub const TOOL_NAMES: [&str; 14] = [
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
];

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: &'static str,
    pub description: String,
    pub input_schema: Json,
}

impl ToolDefinition {
    fn to_json(&self) -> Json {
        json!({
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        })
    }
}

fn database_property() -> Json {
    json!({
        "type": "string",
        "description": "Database name. Optional if the API key has a default database."
    })
}

fn table_property() -> Json {
    json!({ "type": "string", "description": "Table name." })
}

fn column_schema_json() -> Json {
    json!({
        "type": "object",
        "required": ["name", "type"],
        "additionalProperties": false,
        "properties": {
            "name": { "type": "string", "description": "lowercase a-z, 0-9, '_' and '-'" },
            "type": {
                "type": "string",
                "enum": ["bool", "int64", "float64", "utf8", "timestamp", "date", "uuid", "json"],
                "description": "Storage type. Money is float64 with semantic_type 'currency'."
            },
            "nullable": { "type": "boolean", "default": true },
            "description": {
                "type": "string",
                "description": "What the column means. This is shown to the natural-language layer, so write it for a reader who has never seen the table."
            },
            "semantic_type": {
                "type": "string",
                "enum": ["id", "currency", "timestamp", "category", "country", "email", "url",
                         "quantity", "score", "text", "boolean", "json"],
                "description": "What the value means. Drives aggregation guardrails, bloom filters and query interpretation."
            },
            "unit": { "type": "string", "description": "e.g. 'kg', 'requests/sec'" },
            "currency": { "type": "string", "description": "ISO 4217 code, with semantic_type 'currency'" },
            "default_aggregation": {
                "type": "string",
                "enum": ["count", "sum", "avg", "min", "max"],
                "description": "How this column should be aggregated when a user names it without a function."
            },
            "sensitive": {
                "type": "boolean",
                "default": false,
                "description": "Sensitive columns are excluded from 'select *' and from natural-language context. They can still be queried by name."
            },
            "references": {
                "type": "object",
                "required": ["table", "column"],
                "additionalProperties": false,
                "properties": { "table": { "type": "string" }, "column": { "type": "string" } },
                "description": "Declared relationship, e.g. orders.customer_id -> customers.id"
            }
        }
    })
}

fn keys_property() -> Json {
    json!({
        "type": "array",
        "description": "Primary keys. Use an object per row, e.g. [{\"id\": 7}]. A bare scalar works for a single-column key.",
        "items": {}
    })
}

pub fn definitions() -> Vec<ToolDefinition> {
    let mut out = Vec::with_capacity(TOOL_NAMES.len());

    out.push(ToolDefinition {
        name: "database_create",
        description: "Create a database. Databases hold tables and are scoped to your tenant."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["database"],
            "additionalProperties": false,
            "properties": { "database": database_property() }
        }),
    });

    out.push(ToolDefinition {
        name: "database_list",
        description: "List the databases in your tenant.".to_string(),
        input_schema: json!({ "type": "object", "additionalProperties": false, "properties": {} }),
    });

    out.push(ToolDefinition {
        name: "database_delete",
        description:
            "Delete a database. Irreversible. Requires cascade=true if it still has tables."
                .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["database"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "cascade": {
                    "type": "boolean",
                    "default": false,
                    "description": "Delete the database's tables and all their data."
                }
            }
        }),
    });

    out.push(ToolDefinition {
        name: "table_create",
        description: "Create a table. Describe each column's meaning: the description and \
                      semantic_type are what let later questions be answered in plain language. \
                      Give a primary_key only if rows will be updated or deleted individually; \
                      leave it out for append-only event data, which is faster to ingest."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "columns"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "description": { "type": "string", "description": "What one row represents." },
                "columns": { "type": "array", "minItems": 1, "items": column_schema_json() },
                "primary_key": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Non-nullable columns identifying a row. Required for upsert, update and delete."
                },
                "partitions": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 64,
                    "description": "Parallelism unit; defaults to 4. Cannot be changed later."
                }
            }
        }),
    });

    out.push(ToolDefinition {
        name: "table_list",
        description: "List the tables in a database, with row counts.".to_string(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "database": database_property() }
        }),
    });

    out.push(ToolDefinition {
        name: "table_describe",
        description: "Full schema of a table plus storage statistics. Call this before writing a \
                      query if you are unsure of the column names."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table"],
            "additionalProperties": false,
            "properties": { "database": database_property(), "table": table_property() }
        }),
    });

    out.push(ToolDefinition {
        name: "table_drop",
        description: "Delete a table and all of its data. Irreversible.".to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table"],
            "additionalProperties": false,
            "properties": { "database": database_property(), "table": table_property() }
        }),
    });

    out.push(ToolDefinition {
        name: "schema_get",
        description: "The schema of one table, or of every table when no table is named."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "database": database_property(), "table": table_property() }
        }),
    });

    out.push(ToolDefinition {
        name: "schema_update",
        description: "Evolve a table's schema. Pass the complete column list you want, existing \
                      columns included. New columns must be nullable; descriptions and semantic \
                      metadata can be edited freely. Existing columns cannot be dropped or \
                      retyped, and the primary key and partition count are fixed for the life of \
                      the table (they are inherited, not restated)."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "columns"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "description": { "type": "string" },
                "columns": {
                    "type": "array",
                    "minItems": 1,
                    "items": column_schema_json(),
                    "description": "The complete column list after the change, existing columns included."
                }
            }
        }),
    });

    out.push(ToolDefinition {
        name: "data_insert",
        description: "Append rows. Each row is an object keyed by column name; unknown columns \
                      are an error rather than being dropped. Timestamps accept ISO-8601 strings. \
                      On a table with a primary key, duplicate keys are rejected: use data_upsert \
                      to replace."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "rows"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "rows": { "type": "array", "minItems": 1, "items": { "type": "object" } }
            }
        }),
    });

    out.push(ToolDefinition {
        name: "data_upsert",
        description: "Insert rows, replacing any existing row with the same primary key. \
                      Requires a table with a primary key."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "rows"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "rows": { "type": "array", "minItems": 1, "items": { "type": "object" } }
            }
        }),
    });

    out.push(ToolDefinition {
        name: "data_get",
        description: "Fetch rows by primary key. Missing keys are simply absent from the result."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "keys"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "keys": keys_property()
            }
        }),
    });

    out.push(ToolDefinition {
        name: "data_delete",
        description: "Delete rows by primary key. Returns how many keys existed.".to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["table", "keys"],
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "table": table_property(),
                "keys": keys_property()
            }
        }),
    });

    out.push(ToolDefinition {
        name: "data_query",
        description: "Query a table. Either ask in plain language with `request`, or send a \
                      `plan` for exact control. The plan that ran is always returned, so you can \
                      check how a request was interpreted and reuse or adjust it. Supports \
                      projection, filters, sort, limit, count, sum, avg, min, max and group by. \
                      Joins are not supported: query one table at a time."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "database": database_property(),
                "request": {
                    "type": "string",
                    "description": "The question in plain language, e.g. 'total revenue by country in the last 30 days'."
                },
                "plan": PlanRequest::json_schema(),
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Convenience cap on returned rows; also settable inside `plan`."
                }
            }
        }),
    });

    debug_assert_eq!(out.len(), TOOL_NAMES.len());
    out
}

/// Tool list in MCP's `tools/list` shape.
pub fn definitions_json() -> Json {
    Json::Array(definitions().iter().map(ToolDefinition::to_json).collect())
}

/// Accept the readme's dotted spelling as well as the advertised one.
pub fn canonical_name(name: &str) -> String {
    name.replace('.', "_")
}

// --- argument helpers -----------------------------------------------------

fn object<'a>(args: &'a Json, tool: &str) -> Result<&'a JsonMap<String, Json>> {
    match args {
        Json::Object(map) => Ok(map),
        Json::Null => Err(AdbError::bad_request(format!("{tool} needs arguments"))),
        other => Err(AdbError::bad_request(format!(
            "{tool} arguments must be an object, got {other}"
        ))),
    }
}

fn required_str(args: &JsonMap<String, Json>, tool: &str, key: &str) -> Result<String> {
    args.get(key)
        .and_then(Json::as_str)
        .map(str::to_string)
        .ok_or_else(|| AdbError::bad_request(format!("{tool} needs a string {key:?}")))
}

fn required_array<'a>(
    args: &'a JsonMap<String, Json>,
    tool: &str,
    key: &str,
) -> Result<&'a Vec<Json>> {
    args.get(key)
        .and_then(Json::as_array)
        .ok_or_else(|| AdbError::bad_request(format!("{tool} needs an array {key:?}")))
}

fn rows_of(args: &JsonMap<String, Json>, tool: &str) -> Result<Vec<JsonMap<String, Json>>> {
    required_array(args, tool, "rows")?
        .iter()
        .enumerate()
        .map(|(index, row)| {
            row.as_object().cloned().ok_or_else(|| {
                AdbError::bad_request(format!("{tool}: row {index} must be an object"))
            })
        })
        .collect()
}

/// Resolve the database for this call: an explicit argument, else the key's
/// default.
fn scoped(
    engine: &Engine,
    ctx: &RequestContext,
    args: &JsonMap<String, Json>,
) -> Result<RequestContext> {
    let mut scoped = ctx.clone();
    if let Some(name) = args.get("database").and_then(Json::as_str) {
        scoped.database = Some(DatabaseName::new(name)?);
    }
    if scoped.database.is_none() {
        let available = engine.list_databases(ctx).unwrap_or_default();
        // With exactly one database there is nothing to disambiguate, and making
        // an agent repeat the name on every call is friction that buys nothing.
        if available.len() == 1 {
            scoped.database = Some(DatabaseName::new(available[0].clone())?);
            return Ok(scoped);
        }
        return Err(AdbError::bad_request(format!(
            "no database selected: pass \"database\". Available: {}",
            if available.is_empty() {
                "(none yet)".to_string()
            } else {
                available.join(", ")
            }
        )));
    }
    Ok(scoped)
}

/// A column as an agent writes it.
#[derive(Debug, Deserialize)]
struct ColumnSpec {
    name: String,
    #[serde(alias = "type")]
    data_type: DataType,
    #[serde(default)]
    nullable: Option<bool>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    semantic_type: Option<SemanticType>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    currency: Option<String>,
    #[serde(default)]
    default_aggregation: Option<Aggregation>,
    #[serde(default)]
    sensitive: Option<bool>,
    #[serde(default)]
    references: Option<ColumnRef>,
}

impl From<ColumnSpec> for ColumnSchema {
    fn from(spec: ColumnSpec) -> Self {
        ColumnSchema {
            name: spec.name,
            data_type: spec.data_type,
            nullable: spec.nullable.unwrap_or(true),
            description: spec.description,
            semantic_type: spec.semantic_type,
            unit: spec.unit,
            currency: spec.currency,
            default_aggregation: spec.default_aggregation,
            sensitive: spec.sensitive.unwrap_or(false),
            references: spec.references,
        }
    }
}

fn table_schema_from_args(args: &JsonMap<String, Json>, tool: &str) -> Result<TableSchema> {
    let name = TableName::new(required_str(args, tool, "table")?)?;
    let columns = required_array(args, tool, "columns")?;
    let columns: Vec<ColumnSchema> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            serde_json::from_value::<ColumnSpec>(column.clone())
                .map(ColumnSchema::from)
                .map_err(|e| {
                    AdbError::bad_request(format!("{tool}: column {index} is not usable: {e}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut schema = TableSchema::new(name, columns);
    if let Some(keys) = args.get("primary_key").and_then(Json::as_array) {
        schema.primary_key = keys
            .iter()
            .map(|key| {
                key.as_str().map(str::to_string).ok_or_else(|| {
                    AdbError::bad_request(format!("{tool}: primary_key entries must be strings"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
    }
    if let Some(partitions) = args.get("partitions").and_then(Json::as_u64) {
        schema.partitions = partitions as u32;
    }
    if let Some(description) = args.get("description").and_then(Json::as_str) {
        schema.description = Some(description.to_string());
    }
    schema.validate()?;
    Ok(schema)
}

/// Add "here is what does exist" to a not-found error, so the agent's next call
/// can succeed.
fn with_available_tables(engine: &Engine, ctx: &RequestContext, error: AdbError) -> AdbError {
    if !matches!(error, AdbError::NotFound { kind: "table", .. }) {
        return error;
    }
    match engine.list_tables(ctx) {
        Ok(tables) if !tables.is_empty() => AdbError::bad_request(format!(
            "{error}. Tables in this database: {}",
            tables
                .iter()
                .map(|t| t.name.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Ok(_) => AdbError::bad_request(format!("{error}. This database has no tables yet.")),
        Err(_) => error,
    }
}

// --- dispatch -------------------------------------------------------------

/// Execute one tool call.
pub fn call(engine: &Engine, ctx: &RequestContext, name: &str, args: &Json) -> Result<Json> {
    let tool = canonical_name(name);
    let tool = tool.as_str();
    match tool {
        "database_list" => Ok(json!({ "databases": engine.list_databases(ctx)? })),

        "database_create" => {
            let args = object(args, tool)?;
            let database = DatabaseName::new(required_str(args, tool, "database")?)?;
            engine.create_database(ctx, &database)?;
            Ok(json!({ "created": database.to_string() }))
        }

        "database_delete" => {
            let args = object(args, tool)?;
            let database = DatabaseName::new(required_str(args, tool, "database")?)?;
            let cascade = args.get("cascade").and_then(Json::as_bool).unwrap_or(false);
            let dropped = engine.drop_database(ctx, &database, cascade)?;
            Ok(json!({ "deleted": database.to_string(), "tables_deleted": dropped }))
        }

        "table_create" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let schema = table_schema_from_args(args, tool)?;
            let created = engine.create_table(&ctx, schema)?;
            Ok(json!({ "created": created.name.to_string(), "schema": &*created }))
        }

        "table_list" => {
            let args = object(args, tool).unwrap_or(&EMPTY_ARGS);
            let ctx = scoped(engine, ctx, args)?;
            let tables = engine.list_tables(&ctx)?;
            let mut described = Vec::with_capacity(tables.len());
            for schema in tables {
                let stats = engine.table_stats(&ctx, &schema.name).ok();
                described.push(json!({
                    "table": schema.name.to_string(),
                    "description": schema.description,
                    "columns": schema.columns.len(),
                    "primary_key": schema.primary_key,
                    "rows": stats.as_ref().map(|s| s.rows),
                    "bytes": stats.as_ref().map(|s| s.bytes),
                }));
            }
            Ok(json!({ "tables": described }))
        }

        "table_describe" | "schema_get" => {
            let args = object(args, tool).unwrap_or(&EMPTY_ARGS);
            let ctx = scoped(engine, ctx, args)?;
            match args.get("table").and_then(Json::as_str) {
                None if tool == "schema_get" => {
                    let tables = engine.list_tables(&ctx)?;
                    Ok(json!({
                        "schemas": tables.iter().map(|t| &**t).collect::<Vec<_>>(),
                        "context": engine.schema_context(&ctx)?,
                    }))
                }
                None => Err(AdbError::bad_request(format!(
                    "{tool} needs a string \"table\""
                ))),
                Some(name) => {
                    let table = TableName::new(name)?;
                    let schema = engine
                        .describe_table(&ctx, &table)
                        .map_err(|e| with_available_tables(engine, &ctx, e))?;
                    let mut out = json!({ "schema": &*schema });
                    if let Ok(stats) = engine.table_stats(&ctx, &table) {
                        out["stats"] = serde_json::to_value(stats)?;
                    }
                    Ok(out)
                }
            }
        }

        "schema_update" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let mut schema = table_schema_from_args(args, tool)?;
            // The primary key and partition count are fixed for the life of the
            // table, so they are inherited rather than being something a caller
            // has to restate (and could clear by omitting).
            let existing = engine
                .describe_table(&ctx, &schema.name)
                .map_err(|e| with_available_tables(engine, &ctx, e))?;
            if args.get("primary_key").is_none() {
                schema.primary_key = existing.primary_key.clone();
            }
            if args.get("partitions").is_none() {
                schema.partitions = existing.partitions;
            }
            if args.get("description").is_none() {
                schema.description = existing.description.clone();
            }
            schema.validate()?;
            let updated = engine.update_schema(&ctx, schema)?;
            Ok(
                json!({ "table": updated.name.to_string(), "version": updated.version, "schema": &*updated }),
            )
        }

        "table_drop" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let table = TableName::new(required_str(args, tool, "table")?)?;
            engine
                .drop_table(&ctx, &table)
                .map_err(|e| with_available_tables(engine, &ctx, e))?;
            Ok(json!({ "dropped": table.to_string() }))
        }

        "data_insert" | "data_upsert" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let table = TableName::new(required_str(args, tool, "table")?)?;
            let rows = rows_of(args, tool)?;
            let outcome = if tool == "data_upsert" {
                engine.upsert(&ctx, &table, &rows)
            } else {
                engine.insert(&ctx, &table, &rows)
            }
            .map_err(|e| with_available_tables(engine, &ctx, e))?;
            Ok(json!({
                "table": table.to_string(),
                "rows_written": outcome.rows,
                "segments_flushed": outcome.segments_flushed,
            }))
        }

        "data_get" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let table = TableName::new(required_str(args, tool, "table")?)?;
            let keys = required_array(args, tool, "keys")?;
            let rows = engine
                .get(&ctx, &table, keys)
                .map_err(|e| with_available_tables(engine, &ctx, e))?;
            Ok(json!({ "rows": rows, "found": rows.len(), "requested": keys.len() }))
        }

        "data_delete" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let table = TableName::new(required_str(args, tool, "table")?)?;
            let keys = required_array(args, tool, "keys")?;
            let deleted = engine
                .delete(&ctx, &table, keys)
                .map_err(|e| with_available_tables(engine, &ctx, e))?;
            Ok(json!({ "deleted": deleted, "requested": keys.len() }))
        }

        "data_query" => {
            let args = object(args, tool)?;
            let ctx = scoped(engine, ctx, args)?;
            let source = match (args.get("request").and_then(Json::as_str), args.get("plan")) {
                (Some(request), None) => QuerySource::Request(request.to_string()),
                (None, Some(plan)) => {
                    let mut plan: PlanRequest =
                        serde_json::from_value(plan.clone()).map_err(|e| {
                            AdbError::bad_request(format!("{tool}: unusable plan: {e}"))
                        })?;
                    if let Some(limit) = args.get("limit").and_then(Json::as_u64) {
                        plan.limit = Some(limit as usize);
                    }
                    QuerySource::Plan(plan)
                }
                (Some(_), Some(_)) => {
                    return Err(AdbError::bad_request(format!(
                        "{tool}: pass either \"request\" or \"plan\", not both"
                    )))
                }
                (None, None) => {
                    return Err(AdbError::bad_request(format!(
                        "{tool}: pass \"request\" (plain language) or \"plan\" (structured)"
                    )))
                }
            };
            // A convenience `limit` narrows the row budget for a language
            // request, where there is no plan to put it in.
            let ctx = match (args.get("limit").and_then(Json::as_u64), &source) {
                (Some(limit), QuerySource::Request(_)) => {
                    let limits = QueryLimits {
                        max_rows: (limit as usize).min(ctx.limits.max_rows),
                        ..ctx.limits
                    };
                    ctx.with_limits(limits)
                }
                _ => ctx,
            };
            let outcome = engine
                .query(&ctx, source)
                .map_err(|e| with_available_tables(engine, &ctx, e))?;
            Ok(serde_json::to_value(outcome)?)
        }

        unknown => Err(AdbError::not_found("tool", unknown)),
    }
}

/// Shared empty argument object, so tools with all-optional arguments accept a
/// missing `arguments` field.
static EMPTY_ARGS: std::sync::LazyLock<JsonMap<String, Json>> =
    std::sync::LazyLock::new(JsonMap::new);

/// Names, for documentation and the REST layer.
pub fn tool_index() -> BTreeMap<&'static str, String> {
    definitions()
        .into_iter()
        .map(|d| (d.name, d.description))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_tool_has_a_schema_and_a_description() {
        let definitions = definitions();
        assert_eq!(definitions.len(), TOOL_NAMES.len());
        for definition in &definitions {
            assert!(
                TOOL_NAMES.contains(&definition.name),
                "{} is not listed",
                definition.name
            );
            assert!(
                definition.description.len() > 30,
                "{} needs a description a model can use",
                definition.name
            );
            assert_eq!(definition.input_schema["type"], json!("object"));
            assert_eq!(
                definition.input_schema["additionalProperties"],
                json!(false),
                "{} should reject unknown arguments",
                definition.name
            );
        }
    }

    #[test]
    fn there_is_no_arbitrary_query_tool() {
        // see "Guardrails" in ARCHITECTURE.md: unrestricted execution is not part of the surface.
        for name in TOOL_NAMES {
            assert!(
                !name.contains("execute"),
                "{name} looks like an escape hatch"
            );
            assert!(!name.contains("sql"), "{name} looks like an escape hatch");
        }
    }

    #[test]
    fn dotted_names_from_the_readme_are_accepted() {
        assert_eq!(canonical_name("database.create"), "database_create");
        assert_eq!(canonical_name("data.query"), "data_query");
        assert_eq!(canonical_name("data_query"), "data_query");
    }

    #[test]
    fn tool_names_are_client_safe() {
        for name in TOOL_NAMES {
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{name} may be rejected by MCP clients"
            );
        }
    }

    #[test]
    fn column_specs_accept_type_or_data_type() {
        let spec: ColumnSpec =
            serde_json::from_value(json!({ "name": "amount", "type": "float64" })).unwrap();
        assert_eq!(spec.data_type, DataType::Float64);
        let spec: ColumnSpec =
            serde_json::from_value(json!({ "name": "amount", "data_type": "float64" })).unwrap();
        assert_eq!(spec.data_type, DataType::Float64);
        let column: ColumnSchema = spec.into();
        assert!(column.nullable, "columns default to nullable");
    }

    #[test]
    fn the_query_tool_advertises_the_plan_schema() {
        let query = definitions()
            .into_iter()
            .find(|d| d.name == "data_query")
            .unwrap();
        let properties = &query.input_schema["properties"];
        assert!(properties["request"].is_object());
        assert_eq!(
            properties["plan"]["properties"]["metrics"]["type"],
            json!("array")
        );
    }
}
