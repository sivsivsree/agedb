# Operator guide

Everything here is about running `agedb`. For what the project is and why, see
[`../README.md`](../README.md). For how it works inside, see
[`../ARCHITECTURE.md`](../ARCHITECTURE.md).

## The binary

```
agedb [OPTIONS] <COMMAND>

  serve    Serve MCP and/or the REST API
  query    Run one query and print the result
  tools    Print the MCP tool catalogue as JSON
```

Global options, each with an environment variable:

| Flag | Env | Default | |
| --- | --- | --- | --- |
| `--data-dir` | `ADB_DATA_DIR` | `./data` | Where data lives. Created if absent |
| `--config` | `ADB_CONFIG` | none | API keys and limits, as JSON |
| `--log` | `ADB_LOG` | `info` | Filter, for example `adb_exec=debug` |
| `--log-json` | `ADB_LOG_JSON` | off | Emit logs as JSON |
| `--no-fsync` | `ADB_NO_FSYNC` | off | Skip fsync on WAL append. Benchmarks only |

Logs always go to stderr, because stdout carries MCP frames.

## Serving

```bash
# MCP over stdio: an agent launches this and talks JSON-RPC on the pipe.
agedb --data-dir ./data serve --transport stdio --tenant acme

# REST plus MCP over HTTP.
agedb --data-dir ./data serve --transport http --port 8080 --api-key dev-key --tenant acme

# Both at once.
agedb --data-dir ./data serve --transport both --api-key dev-key --tenant acme
```

| `serve` flag | Default | |
| --- | --- | --- |
| `--transport` | `stdio` | `stdio`, `http` or `both` |
| `--bind` | `127.0.0.1` | Interface for HTTP |
| `--port` | `8080` | |
| `--api-key` | none | A single key with full access to `--tenant`. Use `--config` for more |
| `--tenant` | `local` | Tenant for the stdio identity and for `--api-key` |
| `--database` | none | Database assumed when a call does not name one |

The HTTP transport refuses to start without at least one key.

## One-shot queries

```bash
agedb --data-dir ./data query "how many orders" --tenant acme
agedb --data-dir ./data query "top 10 countries by revenue" --tenant acme --explain
agedb --data-dir ./data query '{"table":"orders","limit":5}' --plan --tenant acme
agedb tools                     # the MCP tool catalogue, as JSON
```

`--explain` prints the structured plan and the physical pipeline without running it, which
is the quickest way to see how a question was interpreted.

## Configuration file

`--config keys.json` defines API keys, their scopes and their budgets:

```json
{
  "keys": [
    {
      "key": "writer-key",
      "tenant": "acme",
      "user": "ingest-agent",
      "scopes": ["database:read", "database:write", "schema:read", "schema:write",
                 "data:insert", "data:update", "data:delete"],
      "default_database": "crm"
    },
    {
      "key": "reader-key",
      "tenant": "acme",
      "user": "analyst-agent",
      "scopes": ["database:read", "schema:read"],
      "max_rows": 1000,
      "max_bytes_scanned": 1073741824,
      "max_execution_time_ms": 5000
    }
  ],
  "local": {
    "tenant": "acme",
    "scopes": ["database:read", "schema:read"]
  }
}
```

* `keys` are for HTTP, presented as `Authorization: Bearer <key>`.
* `local` is the identity for the stdio transport, where the process boundary is the trust
  boundary. If omitted, stdio gets full access to `--tenant`.
* An empty `scopes` list means read-only.
* Omitted limits fall back to the defaults below.

### Scopes

| Scope | Allows |
| --- | --- |
| `database:read` | Listing databases, running queries, point lookups |
| `database:write` | Creating and deleting databases |
| `schema:read` | Listing and describing tables |
| `schema:write` | Creating, evolving and dropping tables |
| `data:insert` | `data_insert`, flush, compact |
| `data:update` | `data_upsert` |
| `data:delete` | `data_delete` |

### Limits

| Limit | Default | Enforced |
| --- | --- | --- |
| `max_rows` | 10,000 | Result rows. A truncated result sets `stats.truncated` |
| `max_bytes_scanned` | 4 GiB | Charged as segments are opened, mid-query |
| `max_execution_time_ms` | 30,000 | Checked between batches |
| `max_write_rows` | 1,000,000 | Rows in one insert or upsert call |

A caller-supplied `limit` above `max_rows` is clamped, with a warning in the response.

## REST

| Route | |
| --- | --- |
| `GET /healthz` | Liveness, version, which translator is active. No key needed |
| `GET /v1/tools` | The MCP tool catalogue |
| `POST /v1/mcp` | MCP JSON-RPC over HTTP |
| `GET POST /v1/databases` | List, create |
| `DELETE /v1/databases/{db}?cascade=true` | Delete |
| `GET POST /v1/databases/{db}/tables` | List, create |
| `GET DELETE /v1/databases/{db}/tables/{t}` | Describe, drop |
| `PATCH /v1/databases/{db}/tables/{t}/schema` | Evolve |
| `POST PUT /v1/databases/{db}/tables/{t}/rows` | Insert, upsert |
| `POST /v1/databases/{db}/tables/{t}/rows/get` | Point lookup by key |
| `POST /v1/databases/{db}/tables/{t}/rows/delete` | Delete by key |
| `POST /v1/databases/{db}/query` | Query |
| `POST /v1/databases/{db}/explain` | Plan without running |

Error responses are always the same shape, so a client can branch on the code:

```json
{ "error": { "code": "not_found", "message": "column \"revenue\" not found" } }
```

| Status | Codes |
| --- | --- |
| 400 | `bad_request`, `type_mismatch`, `invalid_schema`, `invalid_identifier` |
| 403 | `permission_denied` |
| 404 | `not_found` |
| 409 | `already_exists` |
| 422 | `unsupported` |
| 429 | `limit_exceeded` |
| 500 | `internal_error`, `storage_error`, `corruption` |

## Designing a table

The schema is what makes plain-language questions work, so it is worth a minute:

```json
{
  "table": "orders",
  "description": "One row per placed order",
  "primary_key": ["id"],
  "partitions": 8,
  "columns": [
    {"name": "id", "type": "int64", "nullable": false, "semantic_type": "id"},
    {"name": "country", "type": "utf8", "semantic_type": "country"},
    {"name": "amount", "type": "float64", "semantic_type": "currency", "currency": "USD",
     "default_aggregation": "sum", "description": "Total order value before refunds"},
    {"name": "created_at", "type": "timestamp", "semantic_type": "timestamp"},
    {"name": "buyer_email", "type": "utf8", "semantic_type": "email", "sensitive": true}
  ]
}
```

* **`primary_key`** enables `upsert`, `get` and `delete`, at the cost of an in-memory key
  index. Leave it out for append-only event data, which ingests faster.
* **`partitions`** is the unit of query parallelism, 4 by default, and cannot change later.
* **`semantic_type`** drives natural language resolution, aggregation guardrails and bloom
  filter selection.
* **`default_aggregation`** lets "total revenue" resolve without naming a column.
* **`description`** is shown to the language layer. Write it for someone who has never seen
  the table.
* **`sensitive`** keeps a column out of `select *` and out of anything sent to a model. It
  can still be queried by name.

Types: `bool`, `int64`, `float64`, `utf8`, `timestamp`, `date`, `uuid`, `json`. Money is
`float64` with `semantic_type: currency`; exact decimals are not implemented yet.

## Natural language

With no configuration, translation is done by a deterministic rule engine: counts, sums,
averages, min/max, group-bys, top-N, comparisons, null checks, and time windows such as
"last 90 days", "this year" or "since 2026-01-01". It refuses what it cannot do and lists
the available columns.

Set `ANTHROPIC_API_KEY` to use a hosted model instead, which is forced to emit the same
structured plan and passes through the same validator. `ADB_NL_MODEL` overrides the model.
`GET /healthz` reports which translator is active. If the model is unreachable, the engine
falls back to the rules and says so in the response warnings.

## Operating notes

* **One process per data directory.** The WAL is single-writer, enforced by an advisory lock
  on `{data_dir}/LOCK`. A second process is refused rather than corrupting the log.
* **`--tenant` must match.** It defaults to `local`, so data written by
  `serve --tenant acme` is invisible to `query` without `--tenant acme`.
* **Backups**: stop the process, copy the data directory. There is no online backup yet.
* **`--no-fsync`** makes ingest several times faster and can lose acknowledged writes on
  power loss. Benchmarks only.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `... is already open by another agedb process` | Another server or CLI holds the data directory |
| `pass --database. Tenant "local" has no databases` | `--tenant` does not match the one that wrote the data |
| `no database selected: pass "database"` | The tenant has several databases and the key has no default |
| `the HTTP transport needs at least one API key` | Pass `--api-key` or `--config` |
| Query returns fewer rows than expected, `stats.truncated` is true | The key's `max_rows` budget cut the result. Add a `limit` or a filter |
| `column "x" not found` | The error lists the columns that do exist. Call `table_describe` |

Raise the log level to see plans and timings:

```bash
ADB_LOG=adb_engine=debug,adb_exec=debug agedb --data-dir ./data serve --transport http --api-key dev-key
```

## Benchmarks

```bash
cargo run --release -p bench -- --rows 2000000 --partitions 8
```

Reports ingest rows/sec and query p50/p95/p99, plus how many segments were pruned. Useful
flags: `--memtable-rows` (how many segments get created, which determines how much pruning
is possible), `--fsync`, `--repeats`, `--keep`. Baseline numbers for one machine are in
[`../ARCHITECTURE.md`](../ARCHITECTURE.md#baseline-numbers).
