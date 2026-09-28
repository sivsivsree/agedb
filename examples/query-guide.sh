#!/usr/bin/env bash
# Every example in docs/querying.md, run against a real server.
#
# Five small, realistic datasets, one database each:
#
#   crm       leads and customers     a sales agent working a pipeline
#   shop      orders                  an operations agent watching revenue
#   support   tickets                 an agent triaging a support queue
#   product   events                  an agent answering product analytics questions
#   research  prices                  a research agent tracking prices across retailers
#
# Rows are generated from a fixed seed, with timestamps relative to now, so
# "last 7 days" always has something in it. Counts inside time windows drift
# from day to day; everything else is identical on every run.
#
# Usage: examples/query-guide.sh [--keep]
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

DATA_DIR="${ADB_GUIDE_DATA_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/agedb-guide.XXXXXX")}"
PORT="${ADB_GUIDE_PORT:-8098}"
KEY="guide-key"
BASE="http://127.0.0.1:${PORT}"
AUTH=(-H "authorization: Bearer ${KEY}" -H "content-type: application/json")

section() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
post() { curl -sS -X POST "${BASE}$1" "${AUTH[@]}" -d "$2"; }

# Print a query response the way the guide shows it: how the question was
# read, then the first rows, or the error an agent would get.
render() {
  python3 -c '
import json, sys
body = json.load(sys.stdin)
if "error" in body:
    print("   error:", body["error"]["code"], "|", body["error"]["message"])
    sys.exit()
if body.get("interpretation"):
    print("   read as:", body["interpretation"])
plan = {k: v for k, v in body["plan"].items() if v not in (None, [], {})}
print("   plan:", json.dumps(plan, separators=(",", ":")))
rows = body["rows"]
for row in rows[:5]:
    print("  ", json.dumps(row, separators=(", ", ": ")))
if len(rows) > 5:
    print("   ... %d rows" % len(rows))
for warning in body.get("warnings", []):
    print("   warning:", warning)
'
}

# ask <database> <question>
ask() {
  printf '\n> %s\n' "$2"
  post "/v1/databases/$1/query" "$(python3 -c 'import json,sys; print(json.dumps({"request": sys.argv[1]}))' "$2")" | render
}

# plan <database> <label> <plan json>
plan() {
  printf '\n> [plan] %s\n' "$2"
  post "/v1/databases/$1/query" "{\"plan\": $3}" | render
}

cargo build -q -p adb-server

cargo run -q -p adb-server -- \
  --data-dir "${DATA_DIR}" --log warn \
  serve --transport http --port "${PORT}" --api-key "${KEY}" --tenant guide &
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

# --- schemas -----------------------------------------------------------------

section "creating five databases"
for db in crm shop support product research; do
  post /v1/databases "{\"database\":\"${db}\"}" >/dev/null
done

post /v1/databases/crm/tables '{
  "table": "leads", "description": "One row per inbound lead", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"company","type":"utf8","semantic_type":"category"},
    {"name":"country","type":"utf8","semantic_type":"country"},
    {"name":"source","type":"utf8","semantic_type":"category","description":"Campaign that produced the lead"},
    {"name":"score","type":"float64","semantic_type":"score","default_aggregation":"avg","description":"Model-predicted likelihood to convert, 0 to 1"},
    {"name":"value","type":"float64","semantic_type":"currency","currency":"USD","default_aggregation":"sum","description":"Expected deal value"},
    {"name":"email","type":"utf8","semantic_type":"email","sensitive":true},
    {"name":"created_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null
post /v1/databases/crm/tables '{
  "table": "customers", "description": "Leads that signed a contract", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"lead_id","type":"int64","semantic_type":"id","references":{"table":"leads","column":"id"}},
    {"name":"plan","type":"utf8","semantic_type":"category"},
    {"name":"signed_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null

post /v1/databases/shop/tables '{
  "table": "orders", "description": "One row per checkout", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"customer_id","type":"int64","semantic_type":"id"},
    {"name":"country","type":"utf8","semantic_type":"country"},
    {"name":"status","type":"utf8","semantic_type":"category","description":"paid, refunded or pending"},
    {"name":"payment_method","type":"utf8","semantic_type":"category"},
    {"name":"gross_amount","type":"float64","semantic_type":"currency","currency":"USD","default_aggregation":"sum","description":"Order value charged at checkout, before refunds"},
    {"name":"net_amount","type":"float64","semantic_type":"currency","currency":"USD","description":"Order value kept after refunds; zero for a refunded order"},
    {"name":"items","type":"int64","semantic_type":"quantity"},
    {"name":"created_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null

post /v1/databases/support/tables '{
  "table": "tickets", "description": "Customer support tickets", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"priority","type":"utf8","semantic_type":"category","description":"p1 (urgent) to p4"},
    {"name":"status","type":"utf8","semantic_type":"category","description":"open, pending or closed"},
    {"name":"assignee","type":"utf8","semantic_type":"category","description":"Support agent handling the ticket; empty while unassigned"},
    {"name":"channel","type":"utf8","semantic_type":"category"},
    {"name":"first_response_minutes","type":"float64","default_aggregation":"avg","description":"Minutes until the first human reply"},
    {"name":"created_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null

post /v1/databases/product/tables '{
  "table": "events", "description": "Product analytics events", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"user_id","type":"int64","semantic_type":"id"},
    {"name":"event_type","type":"utf8","semantic_type":"category","description":"page_view, signup, purchase or error"},
    {"name":"page","type":"utf8","semantic_type":"category"},
    {"name":"country","type":"utf8","semantic_type":"country"},
    {"name":"occurred_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null

post /v1/databases/research/tables '{
  "table": "prices", "description": "One price observation per product, retailer and visit", "primary_key": ["id"],
  "columns": [
    {"name":"id","type":"int64","nullable":false,"semantic_type":"id"},
    {"name":"product","type":"utf8","semantic_type":"category"},
    {"name":"retailer","type":"utf8","semantic_type":"category"},
    {"name":"price","type":"float64","semantic_type":"currency","currency":"USD","default_aggregation":"avg","description":"Listed price at the time of observation"},
    {"name":"in_stock","type":"bool"},
    {"name":"observed_at","type":"timestamp","semantic_type":"timestamp"}
  ]}' >/dev/null

# --- rows --------------------------------------------------------------------

section "loading rows"
python3 - "${DATA_DIR}" <<'PY'
import json, random, sys
from datetime import datetime, timedelta, timezone

out = sys.argv[1]
random.seed(11)
now = datetime.now(timezone.utc).replace(microsecond=0)

def ago(days):
    return (now - timedelta(days=days, minutes=random.randint(0, 600))).isoformat().replace("+00:00", "Z")

def dump(name, rows):
    # Chunks of 2,000 keep each request well under the HTTP body limit.
    for part, start in enumerate(range(0, len(rows), 2000)):
        with open(f"{out}/{name}.{part}.json", "w") as f:
            json.dump({"rows": rows[start:start + 2000]}, f)

countries = ["uae", "usa", "uk", "de", "in", "sg"]

leads = []
for i in range(3000):
    leads.append({
        "id": i,
        "company": f"company {i % 300}",
        "country": random.choice(countries),
        "source": random.choice(["webinar", "outbound", "referral", "ads", "conference"]),
        "score": round(random.random(), 3),
        "value": round(random.uniform(500, 50000), 2),
        "email": None if i % 9 == 0 else f"contact{i}@example.com",
        "created_at": ago(random.randint(0, 180)),
    })
dump("leads", leads)
dump("customers", [
    {"id": n, "lead_id": lead, "plan": random.choice(["starter", "growth", "enterprise"]),
     "signed_at": ago(random.randint(0, 60))}
    for n, lead in enumerate(random.sample(range(3000), 240))
])

orders = []
for i in range(5000):
    gross = round(random.uniform(12, 900), 2)
    status = random.choices(["paid", "refunded", "pending"], [85, 8, 7])[0]
    orders.append({
        "id": i, "customer_id": random.randint(1, 1500), "country": random.choice(countries),
        "status": status,
        "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
        "gross_amount": gross, "net_amount": 0.0 if status == "refunded" else gross,
        "items": random.randint(1, 6), "created_at": ago(random.randint(0, 120)),
    })
dump("orders", orders)

tickets = []
for i in range(1200):
    status = random.choices(["open", "pending", "closed"], [25, 15, 60])[0]
    tickets.append({
        "id": i,
        "priority": random.choices(["p1", "p2", "p3", "p4"], [5, 20, 45, 30])[0],
        "status": status,
        "assignee": None if status == "open" and i % 3 == 0 else random.choice(["amira", "ben", "chen", "dana"]),
        "channel": random.choice(["email", "chat", "phone"]),
        "first_response_minutes": round(random.expovariate(1 / 45), 1),
        "created_at": ago(random.randint(0, 45)),
    })
dump("tickets", tickets)

events = []
for i in range(20000):
    kind = random.choices(["page_view", "signup", "purchase", "error"], [80, 6, 4, 10])[0]
    events.append({
        "id": i, "user_id": random.randint(1, 2500), "event_type": kind,
        "page": random.choice(["/", "/pricing", "/docs", "/blog", "/signup", "/checkout"]),
        "country": random.choice(countries), "occurred_at": ago(random.randint(0, 30)),
    })
dump("events", events)

products = {"standing desk": 420, "office chair": 260, "monitor arm": 90, "desk lamp": 35, "usb hub": 18}
prices = []
for i in range(1500):
    product = random.choice(list(products))
    prices.append({
        "id": i, "product": product,
        "retailer": random.choice(["amazon", "ikea", "bestbuy", "noon"]),
        "price": round(products[product] * random.uniform(0.8, 1.2), 2),
        "in_stock": random.random() > 0.15, "observed_at": ago(random.randint(0, 90)),
    })
dump("prices", prices)
PY

load() {
  local inserted=0 file
  for file in "${DATA_DIR}/$2".*.json; do
    inserted=$(( inserted + $(curl -sS -X POST "${BASE}/v1/databases/$1/tables/$2/rows" \
      "${AUTH[@]}" --data-binary "@${file}" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows_written"])') ))
  done
  echo "$1.$2: ${inserted} rows"
}
load crm leads
load crm customers
load shop orders
load support tickets
load product events
load research prices

# --- 1. sales pipeline -------------------------------------------------------

section "1. Sales pipeline (crm)"
ask crm "which companies look most likely to convert"
ask crm "which companies in leads look most likely to convert"
ask crm "total value by source in leads this quarter"
ask crm "top 5 countries by value in leads"
ask crm "how many leads were created in the last 7 days"
ask crm "leads where value is greater than 45000"
ask crm "leads from uae"
ask crm "leads where country is uae"
ask crm "average score of leads by country"
ask crm "what is the average in leads"
ask crm "count leads where email is null"
plan crm "leads with no email (sensitive column, named explicitly)" '{
  "table": "leads", "columns": ["id", "company", "source"],
  "filters": [{"column": "email", "op": "is_null"}], "limit": 3}'
plan crm "counting them needs the same column in an aggregate, which is refused" '{
  "operation": "aggregate", "table": "leads",
  "filters": [{"column": "email", "op": "is_null"}],
  "metrics": [{"function": "count", "alias": "leads"}]}'
ask crm "which of these leads became customers"
ask crm "how many customers by plan"

# --- 2. e-commerce orders ----------------------------------------------------

section "2. E-commerce orders (shop)"
ask shop "total revenue by country last month"
ask shop "total gross amount by country last month"
ask shop "total net amount by country last month"
ask shop "average gross amount since 2026-06-01"
ask shop "what is the total for orders"
ask shop "total orders"
ask shop "biggest 10 orders by gross amount"
ask shop "count of refunded orders by payment method"
ask shop "count orders where status is refunded by payment method"
ask shop "orders with more than 5 items"
ask shop "orders where items is more than 5"
plan shop "net revenue by country in September" '{
  "operation": "aggregate", "table": "orders",
  "filters": [{"column": "status", "op": "ne", "value": "refunded"},
              {"column": "created_at", "op": "between", "values": ["2026-09-01", "2026-10-01"]}],
  "group_by": ["country"],
  "metrics": [{"function": "sum", "column": "net_amount", "alias": "net_revenue"},
              {"function": "sum", "column": "gross_amount", "alias": "gross_revenue"},
              {"function": "count", "alias": "orders"}],
  "order_by": [{"column": "net_revenue", "direction": "desc"}], "limit": 3}'

# --- 3. support tickets ------------------------------------------------------

section "3. Support tickets (support)"
ask support "how many tickets where status is open by priority"
ask support "count tickets where assignee is null"
ask support "average first response minutes by assignee"
ask support "which channel has the slowest first response"
ask support "tickets created today"
ask support "how many tickets were created this day"
ask support "tickets where status is open sorted by created at limit 5"
ask support "first 5 tickets where status is open sorted by created at"
ask support "p1 or p2 tickets that are open"
plan support "open p1 or p2 tickets, oldest first" '{
  "table": "tickets",
  "columns": ["id", "priority", "assignee", "created_at"],
  "filters": [{"column": "status", "op": "eq", "value": "open"},
              {"column": "priority", "op": "in", "values": ["p1", "p2"]}],
  "order_by": [{"column": "created_at", "direction": "asc"}],
  "limit": 5}'

# --- 4. product analytics ----------------------------------------------------

section "4. Product analytics (product)"
ask product "count events by event type"
ask product "how many events where event type is signup in the last 7 days by country"
ask product "top 5 pages by count of events"
ask product "count events where event type is error this week"
ask product "how many distinct users signed up"
plan product "signups and errors per page" '{
  "operation": "aggregate", "table": "events",
  "filters": [{"column": "event_type", "op": "in", "values": ["signup", "error"]}],
  "group_by": ["page", "event_type"],
  "metrics": [{"function": "count", "alias": "events"}],
  "order_by": [{"column": "events", "direction": "desc"}], "limit": 4}'

# --- 5. price research -------------------------------------------------------

section "5. Price research (research)"
ask research "average price by retailer"
ask research "lowest price by product"
ask research "minimum price by product"
ask research "prices where price is under 20"
ask research "list observations where price is under 20"
ask research "show observations where price is under 20"
ask research "observations where price is under 20 sorted by price"
ask research "cheapest 5 observations by price"
ask research "how many observations where in stock is false by retailer"
ask research "latest price of standing desk"
plan research "latest observations of one product" '{
  "table": "prices",
  "columns": ["retailer", "price", "in_stock", "observed_at"],
  "filters": [{"column": "product", "op": "eq", "value": "standing desk"}],
  "order_by": [{"column": "observed_at", "direction": "desc"}], "limit": 3}'
plan research "out of stock, by retailer" '{
  "operation": "aggregate", "table": "prices",
  "filters": [{"column": "in_stock", "op": "eq", "value": false}],
  "group_by": ["retailer"], "metrics": [{"function": "count", "alias": "out_of_stock"}]}'
plan research "a price band, paged" '{
  "table": "prices", "columns": ["product", "retailer", "price"],
  "filters": [{"column": "price", "op": "between", "values": [30, 40]},
              {"column": "retailer", "op": "like", "value": "%a%"}],
  "order_by": [{"column": "price", "direction": "asc"}], "limit": 3, "offset": 2}'

section "done"
