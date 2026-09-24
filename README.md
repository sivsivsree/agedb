# agedb

[![CI](https://github.com/sivsivsree/agedb/actions/workflows/ci.yml/badge.svg)](https://github.com/sivsivsree/agedb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.90%2B-orange.svg)](https://rustup.rs)

**A database your AI agent can talk to, that tells you how it read the question and says
so when it can't answer.**

AI agents collect things as they work: leads, prices, support tickets, research notes.
agedb gives them somewhere to keep those records and lets them ask questions in plain
English, such as "which companies look most likely to convert?". It runs on your machine as
one small program. There is no SQL to write, and no extra AI call is made to understand the
question.

```
Agent:  "Create a table for the leads I'm collecting."
Agent:  "Store these 4,000 leads."
Agent:  "Which companies look most likely to convert?"
agedb:   acme 0.99, company 210 0.79, company 139 0.74, ...
         reading leads (grouped by company; avg(score); top 10)
Agent:  "Which of these leads became customers?"
agedb:   unsupported: that request spans 2 tables (customers and leads), which needs a
         join; v0.1 queries one table at a time
```

The last answer is the point. A system that answered from one table would return a
confident, incomplete number. agedb refuses, says why, and the agent can ask a narrower
question.

Status: v0.1, single node, 305 tests, written in Rust. `agedb` is a working name and may
change.

## What it does, in plain words

* **Keeps what your agent collects.** Tables whose columns say what they mean: this is
  money in USD, this is an identifier, this is when the lead arrived, this is what "score"
  means.
* **Answers questions about it.** In plain English or as an exact structured plan: counts,
  totals, averages, rankings, filters and time windows such as "in the last 90 days".
* **Shows its working.** Every answer comes back with the exact plan that ran, so the agent,
  or you, can check the question was read the way it was meant.
* **Refuses rather than guesses.** It refuses any question it cannot answer in full, and
  gives the reason. Nonsense such as adding up customer IDs is refused as meaningless.
* **Stays in bounds.** Each API key has its own permissions, its own row, byte and time
  budgets, and a tenant it cannot see past.

**There is no language model inside.** Plain-language questions are parsed in-process by
deterministic rules. That takes about 17 microseconds per question on a laptop (Apple M1,
release build). The same question over the same schema always gives the same plan, and
nothing is sent anywhere. The agent calling agedb is usually already a language model, so
agedb does not add a second one to every question.

### Who it is for today

There are two starting points. Neither is proven yet.

* **A local workspace for research and operations agents (primary).** This suits agents that
  gather structured records while they work, such as lead lists, price checks, ticket
  triage or experiment logs. They need to keep those records outside their context window
  and run repeatable totals and rankings over them. It runs as one binary, over MCP on
  stdio.
* **A governed data interface for B2B agent products (exploratory).** This suits products
  whose agents answer questions over each customer's data, where tenant isolation, per-key
  permissions and query budgets are product requirements.

If an embedded database your agent already uses serves you well, that is a legitimate
answer. See [How it compares](#how-it-compares).

## What agedb guarantees, and what it does not

| Guaranteed | How |
| --- | --- |
| A plan can only use tables and columns that exist | Every plan is validated against the live schema before anything runs. An unknown name comes back as `not_found`, with the real names listed |
| Meaningless aggregations are refused | Columns carry a `semantic_type`, so `sum(customer_id)` is rejected: "customer_id is a id column, not a measure" |
| You can see how a question was read | Every response carries the plan that ran, a one-line interpretation, and what was scanned and pruned |
| A question that needs two tables is refused, never half-answered | The translator refuses multi-table and "rows missing from another table" questions as `unsupported`. No model is in the path to substitute one table for two |
| One tenant cannot read another's rows | The tenant comes from the API key and selects the storage prefix. There is no `WHERE tenant_id = ?` for anyone to forget |
| A key does only what it is scoped to | `database:read`, `schema:write`, `data:insert` and so on live on the key, never in the request |
| Result size and data read are bounded | Row and byte budgets are enforced inside execution. A truncated result says so |

What agedb does **not** guarantee:

* **Business correctness.** A valid plan can still:
  * sum gross revenue when you meant net;
  * use order date when you meant payment date;
  * count duplicated records twice;
  * cover the wrong reporting period.

  Column descriptions and default aggregations reduce this, and the echoed plan is how you
  catch it. agedb knows only the business definitions your schema states.
* **Column-level authorization.** `sensitive` hides a column from `select *`, from schema
  context and from error suggestions. Any key that may query the table can still select
  the column by name. Keep data a caller must never see in a separate table, database or
  tenant.
* **A hard time limit.** The time budget is checked between batches and between execution
  stages. It is cooperative: a single sort or merge step already running finishes first, so
  a query can overrun its budget by the length of that step.
* **High availability.** agedb v0.1 runs on a single node.

## How it compares

A well-built existing stack can do much of this. PostgreSQL or DuckDB, with schema discovery,
a read-only role and validated SQL, also rejects unknown columns and bounds queries. There is
also direct competition:

* [Cube](https://cube.dev) offers a semantic layer consumed by analytics applications and AI
  agents.
* [MotherDuck](https://motherduck.com) provides MCP access to DuckDB with read-only and
  read-write query tools.

MCP plus analytics plus semantics is not, on its own, a reason to choose agedb.

The bet is narrower:

* **One enforcement point.** Meaning, refusals, permissions, budgets and tenancy are
  enforced in one place, below the agent, and in the same way for every model and every
  front end.
* **No model in the query path.** Answers are fast and reproducible, and they do not change
  when someone upgrades a model.
* **Nothing executable from the caller.** An agent describes what it wants as data. The
  worst it can send is a plan that is refused.

Models will keep getting better at writing SQL, so "models struggle with SQL" is not a
durable reason to exist. Enforced permissions, consistent definitions and predictable
behaviour across models might be. Whether they are worth adopting a new dependency for is
what the [evidence plan](#evidence-and-next-steps) tests.

## Why its own storage engine?

Most of what is distinctive about agedb sits above storage.

The engine exists for three reasons:
* budgets are enforced inside execution: bytes are charged as segments open, and the
  deadline is checked between stages;
* tenant identity selects the storage prefix;
* segment statistics make pruning visible in every response.

Those are easier to guarantee when the executor is ours. That is a design reason, not
evidence that users need it. Adopting agedb today also means adopting its recovery, upgrade
and maintenance story, which is younger than any established database's.

So the next experiment is to put the same validated interface over an established engine
(DuckDB) as a second backend, and see which one users choose. The custom engine stays only
if users show they need what it does differently.

## Quickstart

Requires Rust 1.90 or newer ([rustup](https://rustup.rs)). There are no other dependencies,
no server to install, and no Docker.

```bash
git clone https://github.com/sivsivsree/agedb
cd agedb
cargo build --release
```

See the whole thing work in about 40 seconds:

```bash
examples/demo.sh
```

The demo:
1. starts a server;
2. creates a table and loads 4,000 leads;
3. asks six plain-language questions;
4. shows what errors and refusals look like;
5. replays the same tools over MCP on Streamable HTTP and on stdio.

### As an MCP server on stdio

An agent launches the process and talks JSON-RPC over the pipe:

```json
{
  "mcpServers": {
    "agedb": {
      "command": "/absolute/path/to/agedb",
      "args": ["--data-dir", "/absolute/path/to/data",
               "serve", "--transport", "stdio", "--tenant", "acme"]
    }
  }
}
```

### As an MCP server over Streamable HTTP

```bash
./target/release/agedb --data-dir ./data \
  serve --transport http --port 8080 --api-key dev-key --tenant acme
```

```json
{
  "mcpServers": {
    "agedb": {
      "type": "http",
      "url": "http://localhost:8080/mcp",
      "headers": { "Authorization": "Bearer dev-key" }
    }
  }
}
```

`/mcp` implements the MCP Streamable HTTP transport (revision 2025-06-18):

| Method | What it does |
| --- | --- |
| `POST` | Answers a request as a server-sent event stream, or as plain JSON if the client does not accept streams |
| `GET` | Opens an event stream for server-to-client messages |
| `DELETE` | Ends a session |

Two more behaviours matter:
* `initialize` issues an `Mcp-Session-Id`.
* Browser origins other than this machine are refused unless allowed with `--allow-origin`.

Every request still needs its API key.

### Over REST

The same server also speaks REST:

```bash
H=(-H 'authorization: Bearer dev-key' -H 'content-type: application/json')

curl -sS -X POST localhost:8080/v1/databases "${H[@]}" -d '{"database":"sales"}'

curl -sS -X POST localhost:8080/v1/databases/sales/tables "${H[@]}" -d '{
  "table":"orders","primary_key":["id"],
  "columns":[
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"customer_id","type":"int64","semantic_type":"id"},
    {"name":"country","type":"utf8","semantic_type":"country"},
    {"name":"amount","type":"float64","semantic_type":"currency","default_aggregation":"sum"}]}'

curl -sS -X POST localhost:8080/v1/databases/sales/tables/orders/rows "${H[@]}" -d '{
  "rows":[{"id":1,"customer_id":1,"country":"uae","amount":12000},
          {"id":2,"customer_id":2,"country":"usa","amount":8400},
          {"id":3,"customer_id":1,"country":"uae","amount":19300}]}'

curl -sS -X POST localhost:8080/v1/databases/sales/query "${H[@]}" \
  -d '{"request":"total amount by country in orders"}'
# 200 {"rows":[{"country":"uae","sum_amount":31300.0},{"country":"usa","sum_amount":8400.0}],
#      "interpretation":"reading orders (grouped by country; sum(amount))", "plan":{...}, ...}
```

The operator guide, including config files, scopes, limits and troubleshooting, is in
[`docs/usage.md`](docs/usage.md).

## The agent interface

There are fourteen tools, no query language, and no escape hatch. There is deliberately no
`execute_arbitrary_query`:

```
database_create   table_create     data_insert   data_query
database_list     table_list       data_upsert
database_delete   table_describe   data_get
                  table_drop       data_delete
                  schema_get
                  schema_update
```

`data_query` accepts plain language or a structured plan:

```json
{ "request": "total revenue by country in the last 30 days" }
```

```json
{
  "plan": {
    "operation": "aggregate",
    "table": "orders",
    "filters": [{ "column": "created_at", "op": "gte", "value": "2026-08-01" }],
    "group_by": ["country"],
    "metrics": [{ "function": "sum", "column": "amount", "alias": "revenue" }],
    "order_by": [{ "column": "revenue", "direction": "desc" }],
    "limit": 20
  }
}
```

Either way, the response carries the plan that ran, so an agent can check the
interpretation and reuse or adjust it:

```json
{
  "rows": [{ "country": "uae", "revenue": 3458839.12 }],
  "interpretation": "reading orders (grouped by country; sum(amount))",
  "plan": { "operation": "aggregate", "...": "..." },
  "stats": { "elapsed_ms": 1, "rows_scanned": 4000, "segments_pruned": 24 },
  "warnings": []
}
```

And when it cannot answer, it says so with a code the agent can branch on:

```json
{ "error": { "code": "unsupported",
             "message": "unsupported in v0.1: that request spans 2 tables (customers and orders), which needs a join; v0.1 queries one table at a time" } }
```

```json
{ "error": { "code": "bad_request",
             "message": "bad request: sum(customer_id) is not meaningful: customer_id is a id column, not a measure" } }
```

The plain-language layer handles:
* counts, sums, averages, min/max;
* group-bys, top-N and comparisons;
* null checks;
* relative and absolute time windows.

Anything else is refused with the available columns listed. That refusal is the cue for the
calling agent to send a structured plan instead.

## Architecture

**Every front end converges on one validated Query IR.** Natural language, structured plans
and any future front end lower into the same representation. It is validated once, in one
place, before anything executes.

```
                     Agents / LLMs
                           │
     MCP (stdio or Streamable HTTP)   REST
                           │        │
                  ┌────────▼────────▼────────┐
                  │  adb-mcp  /  adb-api     │  auth, scopes, sessions, per-key limits
                  └────────────┬─────────────┘
                               │
                  ┌────────────▼─────────────┐
                  │        adb-engine        │  catalog, tables, request context
                  └────────────┬─────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
┌───────▼────────┐   ┌─────────▼────────┐   ┌──────────▼─────────┐
│   adb-query    │   │   adb-planner    │   │      adb-exec      │
│ NL to plan JSON│──▶│ plan to IR to    │──▶│ vectorized Arrow   │
│ (local rules)  │   │ validated plan   │   │ operators, budgets │
└────────────────┘   └──────────────────┘   └──────────┬─────────┘
                                                       │
                                            ┌──────────▼─────────┐
                                            │    adb-storage     │
                                            │ WAL, memtable,     │
                                            │ segments, manifest │
                                            └──────────┬─────────┘
                                                       │
                                              filesystem (S3-shaped keys)
```

| Crate | Responsibility |
| --- | --- |
| `adb-core` | Identifiers, type system, semantic schema, versioned catalog, request context |
| `adb-storage` | WAL, memtable, Parquet segments, manifests, key index, compaction |
| `adb-planner` | Query IR, validator, optimizer, physical plan |
| `adb-exec` | Vectorized operators over Arrow, budgets and statistics |
| `adb-query` | Natural language to structured plan (deterministic rules), schema retrieval |
| `adb-engine` | The facade: catalog, tables and the query path |
| `adb-mcp` | MCP tools, JSON-RPC, API keys and scopes, stdio transport |
| `adb-api` | REST, plus MCP over Streamable HTTP |
| `adb-server` | The `agedb` binary |

Underneath:
* writes go to a write-ahead log, then a memtable;
* the memtable flushes into immutable Parquet segments that carry min/max statistics and
  bloom filters;
* updates suppress old rows by bitmap rather than rewriting files;
* queries prune segments from statistics before reading anything.

[`ARCHITECTURE.md`](ARCHITECTURE.md) has the design rationale, on-disk formats, the
concurrency model, and the list of what is deliberately missing.

## Benchmarks, honestly

The numbers in [`ARCHITECTURE.md`](ARCHITECTURE.md#baseline-numbers) track the engine
against itself from change to change. Ingest is measured with WAL fsync off, and there is no
competitor run alongside it. They are useful for catching regressions. They prove nothing
about adoption.

The benchmark that matters is not built yet. It asks whether an agent using agedb completes
real analytical tasks more accurately, cheaply and safely than the same agent using a
well-configured alternative. The plan is to use the same model, the same data and the same
task set on both sides, with equivalent metadata and reasonable configuration for the
alternative, and to measure:

* correct answers, and confidently wrong answers;
* appropriate refusals on unsupported questions;
* tool calls, retries, tokens and total latency;
* permission enforcement;
* time to integrate.

The harness and the results will be published whichever way they come out.

## Evidence and next steps

agedb has solid engineering and no customer evidence yet. No external team uses it every
week. The next 30 days are about finding out whether one should, not about adding features.

1. **Fix silent partial answers.** Done. The hosted-model translator was removed. It could
   answer a two-table question from one table. Multi-table questions are now refused.
2. **Document the safety boundaries precisely.** Done: see
   [what agedb does not guarantee](#what-agedb-guarantees-and-what-it-does-not).
3. **Recruit five external teams** that already have an agent-data problem.
4. **Support one narrow workflow end to end**, starting with the local workspace for
   research and operations agents.
5. **Run the controlled comparison** described under
   [Benchmarks](#benchmarks-honestly), against an established database with an agent
   interface.
6. **Ask for a paid pilot** once that workflow is useful.

The decision gate is:
* at least three teams keep using agedb without repeated prompting;
* there is a measurable advantage on their workloads;
* one team is willing to pay.

If the gate is not met, the direction changes. The most likely change is the validated
interface over an established engine.

The questions the evidence has to answer:
* Which teams use it every week?
* What failed in their previous approach?
* What gets worse if they remove it?
* Why would they pay rather than build the interface themselves?

**Deferred until a user is blocked on it.** Each of these is a substantial programme of work:
* joins and `HAVING`;
* exact decimals;
* vector and full-text search;
* a SQL front end;
* S3, R2 and GCS backends;
* a control plane and dashboard;
* replication. The `LogStore` seam is ready for a Raft log, but snapshots, membership
  changes, read consistency, recovery and failure testing are each their own design work.

**Known limitations today:**
* no joins;
* money is `float64` with `semantic_type: currency`, not an exact decimal;
* single node;
* the key index is rebuilt at startup;
* compaction runs inline after a flush;
* writes to one table are serialized.

## Testing criteria

Every change must keep these green. CI runs all of them on each pull request:

```bash
cargo fmt --all --check                                   # formatting
cargo clippy --workspace --all-targets -- -D warnings     # no warnings, at all
cargo test --workspace                                    # 305 tests, about 15 seconds
examples/demo.sh                                          # end-to-end smoke test
```

The suite is built around four kinds of test, and a change is expected to extend whichever
applies:

| Kind | Where | What it protects |
| --- | --- | --- |
| Unit tests | next to the code, in `#[cfg(test)]` | Behaviour of one function, including its error cases |
| Correctness oracle | `crates/adb-exec/tests/query.rs` | The vectorized engine agreeing with a naive row-at-a-time implementation over the same data |
| Durability | `crates/adb-engine/tests/engine.rs` | Acknowledged writes surviving a hard kill, verified by aborting a child process mid-write |
| Protocol | `crates/adb-mcp/tests/`, `crates/adb-api/tests/` | Real JSON-RPC frames, real HTTP requests and SSE streams, not internal function calls |

What a contribution is expected to bring:

* **New behaviour**: a test that fails without the change.
* **A bug fix**: a regression test that reproduces the bug first.
* **Executor or planner changes**: an oracle case, because subtle wrongness there is
  invisible otherwise.
* **Storage format changes**: a durability test, plus a note in `ARCHITECTURE.md` if the
  on-disk format moved.
* **Performance work**: before and after numbers from
  `cargo run --release -p bench -- --rows 2000000`.

Tests must be deterministic. Where data is generated, seed it (see `Rng` in
`crates/adb-exec/tests/query.rs`), and never depend on wall-clock time. The natural language
layer takes an injected clock, and the execution budget an injected start time, for exactly
this reason.

## Contributing

Start with [`CONTRIBUTING.md`](CONTRIBUTING.md). The short version: fork, branch, make the
four commands above pass, and open a pull request that explains why.

The most useful contributions right now are evidence, not features:

* **Tell us about your agent-data problem.** Open an issue describing the workflow, what you
  use today and where it breaks. That is worth more than any pull request at this stage.
* **Add tasks to the evaluation**: realistic questions with known answers, including ones
  that should be refused.
* **Add cases to the natural language corpus** in `crates/adb-query/tests/nl.rs`. Every
  question an agent asks that gets refused, or read wrongly, is a small, self-contained
  improvement.
* **Improve an error message.** Every error an agent reads is part of the interface.

One repository rule worth stating up front: **no em dashes anywhere**, in code, comments,
documentation or commit messages. CI enforces it.

## License

Apache License 2.0. See [LICENSE](LICENSE).
