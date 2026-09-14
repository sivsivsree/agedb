---
name: Bug report
about: Something behaves differently from what it says it does
labels: bug
---

## What happened

## What you expected

## Reproduction

The smaller the better. Schema, rows, and the query or tool call:

```json
{ "table": "...", "columns": [] }
```

```bash
agedb --data-dir /tmp/repro query "..."
```

## Environment

- agedb version or commit:
- `rustc --version`:
- OS:

## Anything else

Logs from `ADB_LOG=debug` are often the fastest way to see what the planner did.
