## What this changes

## Why

## How it was verified

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `examples/demo.sh`

Tests added or updated:

- [ ] Unit tests for the new behaviour, failing before the change
- [ ] Oracle case in `crates/adb-exec/tests/query.rs` (executor or planner changes)
- [ ] Durability test (storage changes)
- [ ] Protocol test (MCP or REST changes)
- [ ] Corpus case in `crates/adb-query/tests/nl.rs` (natural language changes)
- [ ] Not applicable, because:

Performance-sensitive change? Include before and after from
`cargo run --release -p bench -- --rows 2000000`.
