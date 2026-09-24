# Architecture

AgenticDB (`agedb`, a working name) is an agent-native analytical database: columnar
storage with a write-ahead log, a vectorized query engine, deterministic natural-language
translation with no model in the query path, and MCP as a first-class interface.

This document covers why the system is shaped the way it is, what v0.1 actually does, the
on-disk formats, and where the seams for later work are. [`README.md`](README.md) is the
introduction and [`docs/usage.md`](docs/usage.md) is the operator guide.

```
                     Agents / LLMs
                           │
   MCP (stdio or Streamable HTTP)   REST
                           │        │
                  ┌────────▼────────▼────────┐
                  │  adb-mcp  /  adb-api     │  auth, scopes, per-key limits
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
│ (local rules)  │   │ validated plan   │   │ operators          │
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

## Design rationale

The starting question was "what would a database look like if its primary user were an
agent rather than a person". Ten decisions follow from that, and they explain most of the
code.

**1. Agent-first, not SQL-first.** The primary interface is a small set of typed tools
(`table_create`, `data_insert`, `data_query`), not a query language. A well-configured SQL
database also rejects a column that does not exist; the difference here is what happens
around that. The error names the real columns so the next attempt can be right, meaning is
checked as well as names (`sum(id)` is refused), the plan that ran is echoed back, and the
same checks, budgets and tenancy apply whichever model or front end sent the request.
None of this makes an answer correct for the business question: a valid plan can still sum
gross revenue when net was meant. The echoed plan exists so that can be caught.

**2. One intermediate representation, and everything converges on it.** Natural language,
structured plans, and later SQL all lower into the same Query IR, which is validated once.
That is what keeps the guarantees in one place instead of being re-implemented per front
end, and it is why adding SQL later is a front end rather than a rewrite.

**3. No model in the query path, and callers never emit executable operations.** Natural
language is translated in-process by deterministic rules, so a question costs microseconds
rather than a model round trip, gives the same plan every time, and sends nothing
anywhere. A calling agent that wants precision sends a structured plan, which is data. The
database decides whether that data is legal. This is the difference between "the agent
wrote a query" and "the agent described what it wanted".

**4. Schemas carry meaning, not just types.** `amount decimal` tells a model nothing.
`amount float64, semantic_type currency, currency USD, aggregate with sum, "Total order
value before refunds"` tells it what the column is for. That metadata drives natural
language resolution, aggregation guardrails, bloom filter selection, and what is hidden
from prompts.

**5. Columnar segments with statistics, in the spirit of MergeTree.** A table is a set of
immutable segments, each carrying min/max, null counts and bloom filters. Most analytical
queries touch a small slice of a large table, so the fastest read is the one that never
happens. Segment elimination is the mechanism.

**6. Log-structured mutation instead of rewriting.** Agents insert, update, upsert and
delete. Rewriting a columnar segment per update would be absurd, so writes go to a log, the
newest version of a row wins, and older copies are suppressed by bitmap. Compaction
materializes the result later.

**7. Partitions as the unit of parallelism and of ownership.** Each partition owns a WAL, a
memtable and a key index. Queries fan out over partitions; the catalog is an immutable
snapshot readers clone. Rust's ownership model makes this cheap to get right, and avoids
one global lock.

**8. Tenancy from day one.** Multi-tenant is not a feature added later without pain. Tenant
identity selects the storage prefix rather than filtering results, so a missing check cannot
leak another tenant's rows.

**9. A deliberately small surface for an LLM.** There is no `execute_arbitrary_query` tool.
Capabilities are scopes (`data:insert`, `schema:write`), and every query runs under a row,
byte and time budget that the executor enforces while running, not the edge before starting.
The time budget is cooperative: it is checked between batches and between stages, so a
single sort or merge step in progress finishes before the query is stopped.

**10. Do not rebuild ClickHouse from scratch.** Arrow for in-memory columnar data, Parquet
for persistence, and a custom WAL, catalog, planner and executor on top. The interesting
part of this project is the agent interface and the guarantees behind it, not a
reimplementation of well-understood primitives.

## Scope of v0.1

Deliberately small, and everything in it works end to end:

| | |
| --- | --- |
| Databases | create, list, delete |
| Tables | create, describe, list, drop, evolve schema |
| Data | insert, upsert, get by key, delete by key |
| Query | projection, filter, sort, limit, offset, count, sum, avg, min, max, group by |
| Front ends | MCP over stdio, MCP over Streamable HTTP, REST, CLI |
| Natural language | deterministic rules, in-process, no model; refuses what it cannot answer in full |

What is absent is listed under [What v0.1 does not do](#what-v01-does-not-do), with the seam
each missing feature attaches to.

## Crates

| Crate | Responsibility |
| --- | --- |
| `adb-core` | Identifiers, type system, semantic schema, versioned catalog, request context. No I/O. |
| `adb-storage` | WAL, memtable, Parquet segments, manifests, key index, compaction, object store. Synchronous. |
| `adb-planner` | Query IR, validator, optimizer, physical plan. |
| `adb-exec` | Vectorized operators over Arrow, budgets and statistics. |
| `adb-query` | Natural language to structured plan (deterministic rules), schema retrieval. |
| `adb-engine` | The facade: catalog, tables and the query path, one entry point per operation. |
| `adb-mcp` | MCP tools, JSON-RPC, API keys and scopes, stdio transport. |
| `adb-api` | REST, plus MCP over Streamable HTTP. |
| `adb-server` | The `agedb` binary. |
| `bench` | Ingest and query benchmarks. |

The `adb-` prefix is historical shorthand for the project name. Crates depend downward only:
`core` knows nothing about storage, `storage` knows nothing about planning, and so on.

## The Query IR is the centre

Everything converges on one validated plan. There is no path from caller input to the
executor that skips validation, and no arbitrary query execution.

```
natural language ─┐
                  ├─▶ PlanRequest (JSON) ─▶ Query IR ─▶ validate ─▶ PhysicalPlan ─▶ executor
structured plan ──┘
```

The validator (`crates/adb-planner/src/validate.rs`) is where caller input stops being
untrusted. It resolves every column against the schema, coerces literals to column types
once, type-checks predicates, rejects aggregations that are nonsense (`sum(id)` on a
semantic id), clamps `limit` to the caller's row budget, and refuses the parts of the IR
that v0.1 does not execute rather than producing a plan that would answer wrongly.

The physical plan is a fixed linear pipeline:

```
scan(projection, pruning) → filter → aggregate → rename → sort → limit
```

Logical plans that do not fit, such as a filter above an aggregate (`HAVING`), a limit below
one, or two aggregations, are rejected with a specific message.

## Interfaces

Three front ends, one engine. They share the tool implementations, so they cannot drift.

**MCP** is the primary interface, over stdio (an agent launches the process) or Streamable
HTTP (revision 2025-06-18). The
tool names follow a `noun_verb` scheme: `database_create`, `table_describe`, `data_query`
and so on, fourteen in total. Dotted spellings (`data.query`) are accepted as aliases. Tool
errors come back as results with `isError` set, so the model reads the message and can
correct itself, while protocol faults are JSON-RPC errors.

**Streamable HTTP** lives at `/mcp` (and `/v1/mcp`). `POST` carries one JSON-RPC message
and answers a request as a one-event SSE stream when the client accepts `text/event-stream`,
or as plain JSON otherwise. `GET` opens a server-to-client event stream, which carries only
keep-alives today because the server has no unsolicited messages; it is closed as soon as
graceful shutdown begins so it cannot hold the drain open. `initialize` issues an
`Mcp-Session-Id` bound to the key that opened it; an unknown one, or one used by another
key, gets `404`, and `DELETE` ends it. The session is checked only after authentication,
so an anonymous caller cannot probe which sessions exist. The table is capped at 10,000
sessions. A foreign `Origin` is refused (DNS rebinding), allowed origins get CORS, and an
unsupported `MCP-Protocol-Version` gets `400`. Sessions are correlation only: every request
still authenticates with its API key.

**REST** mirrors the same operations for scripts and services.

**CLI** (`agedb query`) runs a single query for demos and shell pipelines.

The JSON-RPC plumbing is hand-rolled. It is a small, stable protocol, and one fewer
fast-moving dependency in the trust path.

## Storage

### Layout

Keys are S3-shaped even though v0.1 writes them to a local filesystem, so pointing the
object store at a bucket later is a configuration change:

```
{data_dir}/
  LOCK                                   # exclusive advisory lock, one writer
  system/wal/current.log                 # DDL log; the catalog is its replay
  tenants/{tenant}/databases/{db}/tables/{table}/p{n}/
      manifest.json                      # checkpoint: segments, tombstones, applied_lsn
      wal/current.log                    # this partition's log
      segments/seg-00000001.parquet      # immutable column segments
```

### The log is the only way state changes

Every mutation is appended to a WAL and then applied by a deterministic `apply`. Segments
and manifests are checkpoints of that log, never an independent source of truth.

```
file:   "ADBWAL01" then a sequence of records
record: [u32 payload_len LE][u32 crc32(payload) LE][payload]
payload: bincode(LogEntry { lsn: u64, mutation: Mutation })

Mutation = Ddl { json }            # catalog change, JSON inside the frame
         | Insert { ipc }          # Arrow IPC bytes
         | Upsert { ipc }
         | Delete { keys }
```

Two rules make this replayable, and both are load-bearing:

* **`apply` is deterministic.** No clocks, UUIDs or defaults are resolved during apply. They
  are resolved when the record is built and baked into the payload, so two nodes replaying
  the same log reach identical state.
* **A torn tail is not corruption.** A short or CRC-failing frame at the *end* of the file
  is a crash mid-append: it is truncated and the LSN is reused. The same failure anywhere
  earlier is reported as corruption, never silently skipped.

DDL travels as JSON inside the bincode frame on purpose. `TableSchema` uses
`skip_serializing_if`, and bincode is not self-describing, so a skipped field is simply
absent on read. That bug only surfaces on restart, which is the worst possible time, so the
format avoids it structurally.

### Writes, updates and deletes

```
write → WAL (fsync) → memtable → flush at threshold → immutable segment
                                                   → background compaction
```

Updates and deletes never rewrite a segment. The newest row for a primary key wins, and
older copies are recorded in a per-segment Roaring bitmap of suppressed row ordinals, which
the scan operator applies. An `UPDATE` therefore costs the changed rows, not the table.

Resolving "where does key K live now" in constant time needs an in-memory key index
(`HashMap<RowKey, Locator>`), which is why:

* tables **with** a primary key support `upsert`, `get` and `delete`, and pay roughly 60 to
  100 bytes of memory per live key;
* tables **without** one are append-only, keep no index, and are the right shape for bulk
  analytical ingest.

Flush ordering is: write the segment file, atomically replace the manifest (temp plus
rename), then truncate the WAL prefix. A crash at any point leaves either the old checkpoint
plus a replayable WAL, or the new checkpoint, never a gap. An unreferenced segment left by a
crash is deleted at startup.

### Segments and pruning

A segment is one Parquet file (ZSTD, 128k-row row groups) plus statistics *mirrored into the
manifest*: per-column min/max, null count, and an optional bloom filter. Mirroring is the
point, because pruning a segment then costs no I/O at all.

Pruning is one-sided by construction: `can_skip` may only return true when the segment
provably contains no matching row, and every row that is read still goes through the
residual filter. A missed pruning opportunity costs time; a wrong one would corrupt results.

Bloom filters use a fixed FNV-1a over a canonical value encoding, not `ahash` or
`DefaultHasher`, because the filters are persisted and a hash change would produce *false
negatives*, meaning silently wrong answers, rather than a load error. Float columns are never
filtered, since `2` and `2.0` must not disagree.

### Concurrency

Per-table partitions, each owning a WAL, memtable and key index. Reads take a cheap `Arc`
snapshot per partition and run on separate threads. The catalog is an immutable snapshot in
an `ArcSwap`, so readers never take a lock.

Writes to one table take a table-level lock, because primary-key uniqueness and
all-or-nothing multi-partition writes both need it, and bulk ingest sends large batches, so
it is not the bottleneck. Different tables write concurrently. One process at a time may
open a data directory, enforced by an advisory lock on `LOCK`.

## Multi-tenancy

Every request carries `RequestContext { tenant, database, user, request_id, scopes, limits }`.
Tenancy selects the storage prefix rather than filtering rows after the fact:

```
tenants/{tenant}/databases/{db}/tables/{table}/...
```

Two tenants may use the same database and table names without collision, and a request for
a tenant that does not own an object gets `not_found` rather than someone else's data. The
tenant comes from the API key, never from the request body.

## Natural language

```
request → schema lookup → rule translator → PlanRequest (JSON) → validate → run
```

There is no language model in the query path. Translation runs in-process, in
microseconds, with no API key and no network call. That is a deliberate trade:

* **Latency.** The calling agent already pays for its own model round trip. A second one
  inside the database, on every question, would add hundreds of milliseconds to seconds.
* **Determinism.** The same question over the same schema yields the same plan, so the
  corpus in `crates/adb-query/tests/nl.rs` is an assertion about behaviour rather than about
  a model version, and answers are reproducible across runs and across calling models.
* **Nothing leaves the process.** No prompt is built, so no schema, row or sensitive value
  can leak through one.

`RuleTranslator` handles counts, sums, averages, min/max, group-bys, top-N, comparisons,
null checks, relative and absolute time windows, and ranking questions. "Which companies
look most likely to convert" resolves through the `score` column's description and its
declared aggregation. Columns resolve by name, loose forms of the name, unambiguous semantic
synonyms, and descriptions.

It refuses rather than guesses. A request that names two tables, or asks about rows missing
from another table, is `unsupported`: it is never answered from one table as though that
were the whole answer, because a validator cannot tell that a valid one-table plan left
half the question out. Anything else it cannot parse is refused with the available columns
listed. The refusal is the cue for the calling agent, which is usually a language model
already, to send a structured plan.

The echoed plan is part of the answer, so an agent can see how its question was
interpreted and reuse or adjust it.

## Guardrails

Scopes: `database:read|write`, `schema:read|write`, `data:insert|update|delete`. They live on
the API key, not in the request.

Limits are enforced *inside* execution, not at the edge. Bytes are charged as segments are
opened, and a result that hits the row budget is truncated *and says so* in
`stats.truncated` and `warnings`. A caller-supplied `limit` above the budget is clamped,
with a warning, rather than silently honoured or rejected.

The deadline is checked before each segment and memtable batch in the scan, and again on
the coordinator after the scans, per merged partial, and before finishing and sorting.
It is cooperative, not a hard kill: a step already running, such as one sort, completes
first, so a query can overrun by the length of that step. A finished answer is not thrown
away for running over during the cheap offset and limit step.

`sensitive` is not access control. It keeps a column out of `select *`, out of schema
context and out of error suggestions, but any key that may query the table can select the
column by name. Data a caller must never read belongs in a separate table, database or
tenant. Column-level authorization is not implemented.

## Verification

```bash
cargo test --workspace          # 308 tests
cargo clippy --workspace --all-targets -- -D warnings
examples/demo.sh                # the full agent story, end to end
```

Beyond unit tests, the suite includes:

* **A correctness oracle** (`crates/adb-exec/tests/query.rs`): the vectorized engine and a
  naive row-at-a-time implementation answer the same questions over the same generated data,
  covering null handling, cross-partition group merging, suppressed rows and pruning at once.
* **Durability under a hard kill** (`crates/adb-engine/tests/engine.rs`): a child process
  writes rows and calls `abort()`; the parent reopens the directory and requires every
  acknowledged row to be queryable.
* **Pruning actually prunes**: assertions on segment read and prune counters, because a
  correct-but-unpruned result is a silent performance failure.
* **Real protocol frames**: the MCP tests drive JSON-RPC over pipes, and the REST and
  Streamable HTTP tests go through the real router with headers, status codes and SSE
  bodies, including an event stream that must end when shutdown begins.

See [CONTRIBUTING.md](CONTRIBUTING.md) for what a change is expected to bring with it.

## Baseline numbers

Measured with `cargo run --release -p bench -- --rows 2000000 --partitions 8`, append-only
table, WAL fsync off (the harness measures the engine, not the disk's flush latency).

These are a baseline for this engine, not a comparison with anything else. Comparing to
ClickHouse means running the same queries on ClickHouse and reporting its version and
hardware, which has not been done here. Nor do they measure the claim the project rests on,
that an agent completes analytical tasks more accurately, cheaply and safely with agedb than
with a well-configured alternative. That needs a task-level evaluation with the same
model, data and tasks on both sides, which is planned and not yet run.

Machine: Apple M1, 8 cores, 8 GiB RAM, macOS 26.2, rustc 1.97.1. 2,000,000 rows, 8
partitions, 9 repetitions per query.

### Ingest

| memtable threshold | segments | ingest | on disk |
| --- | --- | --- | --- |
| 32,768 rows (default) | 56 | 334k rows/sec | 38.2 MiB (20.0 bytes/row) |
| 512k rows | 8 | 833k rows/sec | 37.0 MiB (19.4 bytes/row) |

Bigger memtables mean fewer flushes and faster ingest, but coarser segment statistics and
therefore less to prune. The row generator produces high-cardinality strings (100k distinct
customers) and a monotonic timestamp, so 20 bytes/row after ZSTD is representative rather
than a best case.

### Queries (56 segments)

| query | p50 | p95 | segments read | pruned |
| --- | --- | --- | --- | --- |
| `count(*)` | 0.01 ms | 2.1 ms | 0 | 0 |
| `sum(amount)` | 74.9 ms | 165.2 ms | 56 | 0 |
| scan where `timestamp > mid` limit 1000 | 17.7 ms | 22.3 ms | 8 | 24 |
| `count(*)` where `timestamp > mid` | 40.2 ms | 46.5 ms | 32 | 24 |
| group by country (8 groups) | 191.3 ms | 233.7 ms | 56 | 0 |
| group by country, quantity (80 groups) | 195.9 ms | 242.9 ms | 56 | 0 |
| group by customer (100k groups) | 687.9 ms | 1185.4 ms | 56 | 0 |
| top 20 customers by revenue | 582.2 ms | 1421.0 ms | 56 | 0 |
| min/max/avg(amount) by country | 242.5 ms | 267.0 ms | 56 | 0 |

Reading these:

* `count(*)` with no filter touches no data at all. It is answered from segment and memtable
  metadata, which is exact because suppression counts are exact.
* The time filter is where pruning shows up. The same scan costs 125 ms with one segment per
  partition (nothing to skip) and 17.7 ms once segments are fine-grained enough to
  eliminate.
* Ungrouped aggregation runs through Arrow's kernels: `sum` over 2M rows in 75 ms is about
  27M rows/sec on one M1.
* Grouped aggregation is slower than it should be, and the gap widens with cardinality.
  100k groups costs about 3.5 times what eight groups costs. That is the row-wise group-key
  path noted under "Known gaps", not a fundamental limit.
* p95 sits well above p50 on the larger aggregations because this machine has 8 GiB of RAM
  and the working set plus 8 parallel partition scans is enough to cause allocator and page
  cache variance.

## What v0.1 does not do

Deliberately out of scope, each with the seam it attaches to:

* **Joins and `HAVING`.** The IR has a `Join` variant for shape parity; the validator
  rejects it. Adding them means a join operator in `adb-exec` and a non-linear physical plan.
* **Vector and full-text search.** The intended shape is relational plus vector plus
  full-text plus JSON in one engine, filtered and aggregated together.
* **A SQL frontend.** The IR is designed so SQL is just another front end feeding
  `adb-planner`.
* **Exact decimals.** Money is `float64` with `semantic_type: currency`. A `Decimal128` type
  would touch `adb-core::types`, the Arrow mapping, and the aggregate states.
* **Distribution and high availability.** Single node. The seam is `LogStore`: a Raft log
  can drive `apply` from committed entries, which is why `apply` determinism is enforced
  now. That is the starting point, not the job. Snapshots and log truncation, membership
  changes, read consistency (leader reads or read index), recovery of a lagging or
  replaced node, and failure testing under partitions each need their own design and
  validation.
* **S3, R2 and GCS.** `ObjectStore` is a trait with one filesystem implementation. A remote
  backend also needs an async variant.
* **A control plane and dashboard.** Organizations, billing, usage metering, and a
  spreadsheet-style data browser are phase two.

## Known gaps

Good places to start contributing, roughly in order of impact:

* Grouped aggregation walks rows to build dynamically-typed group keys, while the ungrouped
  path is vectorized. This is the main performance gap.
* The key index is rebuilt at startup by reading each segment's primary-key columns, so
  opening a large keyed table costs a scan of those columns. Persisting it, or rebuilding it
  lazily, would fix that.
* Writes to a single table are serialized. Per-partition write locking for append-only
  tables is a straightforward follow-up.
* `ObjectStore` is synchronous, which suits a filesystem but blocks a remote backend.
* Compaction runs inline after a flush when the policy asks for it, rather than on a
  background scheduler.
