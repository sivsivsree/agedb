#!/usr/bin/env bash
# The readme's section 20 story, end to end, against a real server.
#
#   1. an agent creates a database and a table for the leads it is collecting
#   2. it stores several thousand leads
#   3. it asks questions in plain language
#   4. the same questions, as structured plans
#   5. the same tools over MCP on stdio, which is how an agent would connect
#
# Usage: examples/demo.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

DATA_DIR="${ADB_DEMO_DATA_DIR:-$(mktemp -d -t agedb-demo)}"
PORT="${ADB_DEMO_PORT:-8099}"
KEY="demo-key"
BASE="http://127.0.0.1:${PORT}"
AUTH=(-H "authorization: Bearer ${KEY}" -H "content-type: application/json")

say() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
post() { curl -sS -X POST "${BASE}$1" "${AUTH[@]}" -d "$2"; }
get()  { curl -sS "${BASE}$1" "${AUTH[@]}"; }

# jq is nice but not required.
if command -v jq >/dev/null 2>&1; then
  show() { jq "${1:-.}"; }
else
  show() { cat; echo; }
fi

say "building"
cargo build -q -p adb-server

say "starting the server (data in ${DATA_DIR})"
cargo run -q -p adb-server -- \
  --data-dir "${DATA_DIR}" \
  serve --transport http --port "${PORT}" --api-key "${KEY}" --tenant acme &
SERVER_PID=$!

cleanup() {
  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  if [[ "${KEEP}" -eq 1 ]]; then
    echo "data kept at ${DATA_DIR}"
  else
    rm -rf "${DATA_DIR}"
  fi
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  if curl -sS "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.2
done
get /healthz | show

say "1. \"Create a table for the leads I'm collecting.\""
post /v1/databases '{"database":"crm"}' | show
post /v1/databases/crm/tables '{
  "table": "leads",
  "description": "One row per inbound lead",
  "primary_key": ["id"],
  "partitions": 4,
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"name","type":"utf8","description":"Contact name"},
    {"name":"company","type":"utf8","semantic_type":"category"},
    {"name":"country","type":"utf8","semantic_type":"country"},
    {"name":"source","type":"utf8","semantic_type":"category","description":"Campaign that produced the lead"},
    {"name":"score","type":"float64","semantic_type":"score","default_aggregation":"avg","description":"Model-predicted likelihood to convert, 0 to 1"},
    {"name":"value","type":"float64","semantic_type":"currency","currency":"USD","default_aggregation":"sum","description":"Expected deal value"},
    {"name":"created_at","type":"timestamp","semantic_type":"timestamp"},
    {"name":"email","type":"utf8","semantic_type":"email","sensitive":true}
  ]
}' | show '.created'

say "2. \"Store these 4,000 leads.\""
python3 - "${DATA_DIR}/leads.json" <<'PY'
import json, random, sys
random.seed(7)
countries = ["uae", "usa", "uk", "de", "sg", "in"]
sources = ["webinar", "outbound", "referral", "ads", "conference"]
rows = []
for i in range(4000):
    rows.append({
        "id": i,
        "name": f"contact {i}",
        "company": f"company {i % 400}",
        "country": random.choice(countries),
        "source": random.choice(sources),
        "score": round(random.random(), 3),
        "value": round(random.uniform(500, 50000), 2),
        # Spread over the last ~200 days.
        "created_at": f"2026-{(i % 9) + 1:02d}-{(i % 27) + 1:02d}T12:00:00Z",
        "email": f"contact{i}@example.com",
    })
json.dump({"rows": rows}, open(sys.argv[1], "w"))
PY
curl -sS -X POST "${BASE}/v1/databases/crm/tables/leads/rows" "${AUTH[@]}" \
  --data-binary "@${DATA_DIR}/leads.json" | show

get /v1/databases/crm/tables/leads | show '{columns: .schema.columns | length, rows: .stats.rows, partitions: .stats.partitions}'

say "3. Questions in plain language"
for question in \
  "how many leads are there" \
  "total value by country in leads" \
  "top 5 sources by value in leads" \
  "average score by country in leads" \
  "leads where value is greater than 45000" \
  "how many leads created in the last 120 days"
do
  printf '\n> %s\n' "${question}"
  post /v1/databases/crm/query "{\"request\": $(printf '%s' "${question}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}" \
    | show '{interpretation, rows: .rows[0:5], plan_operation: .plan.operation, ms: .stats.elapsed_ms, scanned: .stats.rows_scanned, pruned: .stats.segments_pruned}'
done

say "4. The same question as a structured plan (exact control)"
post /v1/databases/crm/query '{
  "plan": {
    "operation": "aggregate",
    "table": "leads",
    "filters": [{"column": "score", "op": "gte", "value": 0.8}],
    "group_by": ["country"],
    "metrics": [
      {"function": "sum", "column": "value", "alias": "pipeline"},
      {"function": "count", "alias": "leads"}
    ],
    "order_by": [{"column": "pipeline", "direction": "desc"}],
    "limit": 5
  }
}' | show '{rows, ms: .stats.elapsed_ms}'

say "5. Point lookup, update, delete"
post /v1/databases/crm/tables/leads/rows/get '{"keys":[{"id":7}]}' | show '.rows[0] | {id, company, value}'
curl -sS -X PUT "${BASE}/v1/databases/crm/tables/leads/rows" "${AUTH[@]}" -d '{
  "rows": [{"id":7,"name":"contact 7","company":"acme","country":"uae","source":"referral",
            "score":0.99,"value":123456.0,"created_at":"2026-09-01T00:00:00Z",
            "email":"contact7@example.com"}]
}' | show
post /v1/databases/crm/tables/leads/rows/get '{"keys":[7]}' | show '.rows[0] | {id, company, value}'
post /v1/databases/crm/tables/leads/rows/delete '{"keys":[{"id":0}]}' | show

say "6. Sensitive columns are not returned by default"
post /v1/databases/crm/query '{"plan":{"table":"leads","limit":1}}' | show '.rows[0] | keys'

say "7. Errors are written for an agent to act on"
post /v1/databases/crm/query '{"plan":{"table":"leads","filters":[{"column":"revenue","op":"gt","value":1}]}}' | show '.error'
post /v1/databases/crm/query '{"plan":{"operation":"aggregate","table":"leads","metrics":[{"function":"sum","column":"id"}]}}' | show '.error'
post /v1/databases/crm/query '{"request":"leads joined with customers"}' | show '.error'

say "8. The same tools over MCP on stdio"
# Only one process may hold a data directory (the WAL is single-writer), so the
# HTTP server stops before the stdio session opens the same data.
kill "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true

cargo run -q -p adb-server -- --data-dir "${DATA_DIR}" --log warn serve --transport stdio --tenant acme --database crm <<'MCP' | python3 -c '
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    message = json.loads(line)
    result = message.get("result", {})
    if "serverInfo" in result:
        print("initialize ->", result["serverInfo"]["name"], result["protocolVersion"])
    elif "tools" in result:
        print("tools/list ->", len(result["tools"]), "tools:", ", ".join(t["name"] for t in result["tools"]))
    else:
        payload = result.get("structuredContent", result)
        rows = payload.get("rows")
        if rows is not None:
            print("data_query ->", json.dumps(rows[:3]))
        else:
            print("tools/call ->", json.dumps(payload)[:160])
'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"demo","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"table_list","arguments":{}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"data_query","arguments":{"request":"top 3 countries by value in leads"}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"data_query","arguments":{"request":"which companies look most likely to convert"}}}
MCP

say "done"
