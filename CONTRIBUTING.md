# Contributing to agedb

Thanks for considering it. This is a young project, which means most of it is still open to
being shaped, and a careful pull request has a good chance of landing.

## Your first 10 minutes

```bash
git clone https://github.com/sivsivsree/agedb
cd agedb
cargo test --workspace     # 301 tests, about 15 seconds after the first build
examples/demo.sh           # the whole system end to end, about 40 seconds
```

You need Rust 1.90 or newer ([rustup](https://rustup.rs)). Nothing else: no database to
install, no Docker, no services. The first build compiles Arrow and Parquet and takes a few
minutes.

If `examples/demo.sh` prints its final `done`, your checkout is healthy.

## Finding the code

```
crates/
  adb-core/      types, semantic schema, catalog, request context      (no I/O)
  adb-storage/   WAL, memtable, Parquet segments, manifests, compaction
  adb-planner/   Query IR, validator, optimizer, physical plan
  adb-exec/      vectorized Arrow operators, budgets, statistics
  adb-query/     natural language to structured plan, schema retrieval
  adb-engine/    the facade that ties catalog, storage and query together
  adb-mcp/       MCP tools, JSON-RPC, API keys and scopes
  adb-api/       REST and MCP over HTTP
  adb-server/    the `agedb` binary
bench/           ingest and query benchmarks
examples/        demo.sh
```

Where to look for a given change:

| You want to change | Start in |
| --- | --- |
| What an agent can call | `crates/adb-mcp/src/tools.rs` |
| Which questions the plain-language layer understands | `crates/adb-query/src/rule.rs` and its corpus in `crates/adb-query/tests/nl.rs` |
| What a plan is allowed to say | `crates/adb-planner/src/validate.rs` |
| How a query executes | `crates/adb-exec/src/` (one file per operator) |
| How rows are stored or recovered | `crates/adb-storage/src/{wal,table,segment}.rs` |
| The type system or semantic metadata | `crates/adb-core/src/{types,schema}.rs` |
| REST routes | `crates/adb-api/src/rest.rs` |

[`ARCHITECTURE.md`](ARCHITECTURE.md) explains why each layer is shaped the way it is. It is
worth reading before a first non-trivial change; it is short.

## The development loop

```bash
cargo test -p adb-storage           # the crate you are working on
cargo test --workspace              # everything, before you push
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

For performance work:

```bash
cargo run --release -p bench -- --rows 2000000 --partitions 8
```

Tracing helps when a test does something surprising:

```bash
ADB_LOG=debug cargo test -p adb-engine -- --nocapture
```

## What a pull request needs

CI runs formatting, a check for stray em dashes, clippy with warnings denied, the full test
suite, the end-to-end demo, and a build on the minimum supported Rust version. All of it
must pass. Beyond that:

**Tests.** New behaviour needs a test that fails without the change. A bug fix needs a
regression test that reproduces the bug first. Specific areas expect specific tests:

* Executor or planner changes: add a case to the correctness oracle in
  `crates/adb-exec/tests/query.rs`, which compares the vectorized engine against a naive
  row-at-a-time implementation. Subtle wrongness in query execution is invisible otherwise.
* Storage changes: add or extend a durability test in `crates/adb-engine/tests/engine.rs`.
  The existing one hard-kills a child process mid-write and requires every acknowledged row
  to come back.
* MCP or REST changes: drive real frames, as the tests in `crates/adb-mcp/tests/mcp.rs` and
  `crates/adb-api/tests/rest.rs` do, rather than calling the handler functions directly.
* Natural language changes: add the question to the corpus in
  `crates/adb-query/tests/nl.rs`. Every case there goes the whole way from request to
  validated plan, because a plan that looks right but fails validation is not an answer.

**Determinism.** Tests must not depend on wall-clock time or unseeded randomness. Seed
generators (see `Rng` in `crates/adb-exec/tests/query.rs`) and inject clocks (see
`TranslationContext::at`). A flaky test is worse than no test.

**A description of the why.** The diff shows what changed. The pull request should say what
problem it solves and what alternatives you rejected.

## Code conventions

**Comments explain why, not what.** `// increment the counter` is noise. `// The key index
is in-memory only, so it is rebuilt from the primary-key columns of each segment` is the
reason someone will need in six months. If a decision was non-obvious or a trap was avoided,
say so at the point where a reader would otherwise wonder.

**Errors are part of the interface.** An agent reads them and decides what to do next. Name
the thing that failed, and where you can, list the valid options:

```rust
// Not this:
return Err(AdbError::bad_request("invalid column"));
// This:
return Err(AdbError::not_found("column", format!("{}.{}", schema.name, name)));
```

**No panics in library code.** Return `AdbError`, which carries a stable machine-readable
code. `expect` is acceptable only where a failure means an invariant of this crate was
already violated, and the message should say which one.

**Log application must stay deterministic.** Nothing in a WAL `apply` may read a clock,
generate a UUID, or resolve a default. Those are resolved when the record is built. Two
nodes replaying the same log must reach identical state, which is what makes Raft
replication possible later without a redesign.

**Refuse rather than approximate.** If the engine cannot execute something correctly, it
returns `Unsupported` with a message saying what to do instead. A plausible wrong answer is
the worst outcome in this system.

**Persisted formats are contracts.** The WAL frame layout, the manifest JSON, the bloom
filter hash and partition routing are all on disk somewhere. Changing any of them needs a
format version bump and a note in `ARCHITECTURE.md`. The hash function in
`crates/adb-storage/src/hash.rs` has pinned test vectors for exactly this reason: changing
it silently would produce false negatives, not load errors.

**No em dashes anywhere.** In code, comments, documentation, or commit messages. Use a
colon, a comma, a full stop or parentheses. CI fails the build on one.

Formatting is whatever `cargo fmt` produces, with default settings. Do not hand-format
around it.

## Commits and pull requests

* One logical change per commit. Formatting sweeps go in their own commit.
* Imperative subject line, 72 characters or fewer: `add HAVING support to the validator`,
  not `Added HAVING support.`.
* The body explains why, wrapped at 72 columns.
* Do not reference AI tools, assistants or code generators in commit messages or pull
  request descriptions. Commit messages describe the code.
* Rebase rather than merge when updating a branch.

## Recipes

### Adding an MCP tool

1. Add the name to `TOOL_NAMES` in `crates/adb-mcp/src/tools.rs`.
2. Add a `ToolDefinition` in `definitions()`, with a description written for a model to
   read and a closed JSON schema (`additionalProperties: false`).
3. Add the dispatch arm in `call()`, taking the scope check from the engine method you call.
4. Add the engine method in `crates/adb-engine/src/engine.rs` if one does not exist. It
   must start with `ctx.require(Scope::...)`.
5. Add a test in `crates/adb-mcp/tests/mcp.rs` that calls it over a real JSON-RPC frame,
   including the failure case.
6. If it belongs on the HTTP surface too, add the route in `crates/adb-api/src/rest.rs`.
   Prefer calling the MCP tool from the route, as `create_table` does, so the two surfaces
   cannot drift.

### Adding a query operator

1. Extend `Query` in `crates/adb-planner/src/ir.rs`.
2. Teach `validate.rs` to type-check it and compute its output schema. Reject it clearly if
   the executor cannot run it yet.
3. Extend `physical.rs`. The pipeline is linear on purpose; if your operator does not fit,
   say so explicitly rather than bending the plan into something that will answer wrongly.
4. Implement it in `crates/adb-exec/src/`, one file per operator.
5. Add an oracle case in `crates/adb-exec/tests/query.rs`.

### Changing an on-disk format

1. Bump the format constant (`MANIFEST_FORMAT`, or the WAL magic).
2. Decide what happens to existing data: refuse to open it with a clear message, or migrate.
   Silent misreading is not an option.
3. Add a test that opens data written by the old format.
4. Document the change in `ARCHITECTURE.md`.

## Reporting bugs and security issues

Open an issue with a reproduction: the schema, the rows, the query, and what you expected.
For anything with security impact, such as tenant isolation or scope enforcement, please
report it privately through GitHub's security advisory form on this repository rather than
in a public issue.

## Code of conduct

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
