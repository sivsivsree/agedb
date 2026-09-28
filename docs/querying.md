# Querying AgeDB

There are two ways to ask AgeDB a question, and they end up in the same place:

* **Plain language** (`request`): `"total value by source in leads this quarter"`.
  AgeDB parses the question in-process, with deterministic rules and no language model.
  It is fast, and the same question always gives the same plan. It covers a lot, but it is
  not a mind reader.
* **A structured plan** (`plan`): a small JSON object naming the table, filters, grouping,
  metrics and order. It is exact, and covers everything the engine can do.

Either way, every answer carries the plan that ran. **Read it before you use the number.**
Anything in your question that is not in the plan was ignored.

Every example on this page was produced by a real server. Run
[`examples/query-guide.sh`](../examples/query-guide.sh) to reproduce them all. Numbers are
rounded here for readability, and counts inside time windows ("last 7 days") change from day
to day because the data is generated relative to today.

* [Where to send a query](#where-to-send-a-query)
* [Five rules for plain-language questions](#five-rules-for-plain-language-questions)
* [Real-world examples](#real-world-examples)
* [Phrase reference](#phrase-reference)
* [What plain language cannot do](#what-plain-language-cannot-do)
* [Structured plans](#structured-plans)
* [Reading the answer](#reading-the-answer)
* [When a query fails](#when-a-query-fails)
* [Designing tables so questions work](#designing-tables-so-questions-work)

## Where to send a query

**MCP**, the `data_query` tool:

```json
{ "name": "data_query",
  "arguments": { "database": "shop", "request": "total net amount by country last month" } }
```

**REST**:

```bash
curl -sS -X POST localhost:8080/v1/databases/shop/query \
  -H 'authorization: Bearer dev-key' -H 'content-type: application/json' \
  -d '{"request": "total net amount by country last month"}'
```

**Preview without running.** `POST /v1/databases/{db}/explain` takes the same body (a
`request` or a `plan`) and returns the plan and the physical pipeline, without reading any
data. It is useful for checking how a question will be read before you trust it.

**CLI**, for scripts and quick checks:

```bash
agedb query "total net amount by country last month" --database shop
agedb query '{"table":"orders","limit":3}' --plan --database shop
agedb query "top 5 countries by value in leads" --database crm --explain
```

## Five rules for plain-language questions

**1. Name the table, unless the database has only one.**

| Asked | Result |
| --- | --- |
| which companies look most likely to convert | `which table? this database has: customers, leads` |
| which companies **in leads** look most likely to convert | ranked companies by `avg(score)`, top 10 |

**2. Write filters as `<column> <condition>`, with the column first.** Filters written as
adjectives are dropped, and nothing warns you.

| Asked | Read as | |
| --- | --- | --- |
| count of refunded orders by payment method | grouped by payment_method; counting rows | **wrong**: all 5,000 orders, "refunded" ignored |
| count orders where **status is refunded** by payment method | status eq "refunded"; grouped by payment_method; counting rows | right: 137, 131, 141 |
| orders with more than 5 items | refused: could not turn it into a plan | the column comes after the condition |
| orders where **items is more than 5** | items gt 5 | right |
| leads from uae | refused | "from" only starts a date |
| leads where **country is uae** | country eq "uae" | right |

**3. Name one metric and its column: `count`, `total`, `average`, `minimum` or `maximum`.**
Everyday words for a metric often mean something else to the parser.

| Asked | Read as | |
| --- | --- | --- |
| lowest price by product | grouped by product; **avg(price)** | **wrong**: "lowest" needs a number ("lowest 5") |
| **minimum** price by product | grouped by product; min(price) | right |
| latest price of standing desk | **max(price)** over every product | **wrong**: "latest" means maximum, and the product is ignored |
| total revenue by country last month | refused: `sum(country) is not meaningful` | "revenue" means the currency column, but `orders` has two (gross and net), so it matched neither and the next column was taken |
| total **gross amount** by country last month | grouped by country; sum(gross_amount) | right |

**4. Group with `by <column>`, rank with `which <column>` or `top N <column> by <measure>`.**

| Asked | Read as |
| --- | --- |
| average score of leads by country | grouped by country; avg(score) |
| which channel has the slowest first response | grouped by channel; avg(first_response_minutes); top 10, highest first |
| top 5 countries by value in leads | grouped by country; sum(value); top 5 |
| biggest 10 orders by gross amount | 10 rows sorted by gross_amount descending |

**5. Read `interpretation` before you use the answer.** It is one line, and it lists
everything that was understood. Compare it with what you asked. If a filter, a column or a
time window is missing, the answer does not mean what you think. Rephrase the question, or
send a plan.

## Real-world examples

### A sales agent working a pipeline

Database `crm`:

* `leads`: id, company, country, source, `score` ("Model-predicted likelihood to
  convert", aggregated with avg), `value` (currency, aggregated with sum), `email`
  (sensitive), created_at.
* `customers`: id, lead_id, plan, signed_at.

| Question | Read as | Answer |
| --- | --- | --- |
| which companies in leads look most likely to convert | grouped by company; avg(score); top 10 | company 111 0.81, company 213 0.76, company 294 0.72, ... |
| total value by source in leads this quarter | created_at within this quarter; grouped by source; sum(value) | referral 8.31M, webinar 7.60M, ads 7.56M, ... |
| top 5 countries by value in leads | grouped by country; sum(value); top 5 | sg 13.57M, uk 13.07M, de 12.56M, in 12.25M, uae 12.18M |
| how many leads were created in the last 7 days | created_at in the last 7 days; counting rows | 116 (changes daily) |
| leads where value is greater than 45000 | value gt 45000 | 302 rows |
| how many customers by plan | grouped by plan; counting rows | starter 89, growth 82, enterprise 69 |
| which of these leads became customers | refused: `unsupported`, spans 2 tables (customers and leads), needs a join | |

"Most likely to convert" works because the `score` column's description says "likelihood
to convert" and declares `avg` as its aggregation. Nothing in the question names `score`.

The last question needs `leads` and `customers` together. AgeDB refuses it rather than
answering from one table. Ask each table separately ("how many customers by plan"), or keep
the fact on one table, for example a `converted` column on `leads`.

Sensitive columns are invisible to plain language, by design:

| Asked | Result |
| --- | --- |
| count leads where email is null | `column "email" not found` |

A plan can filter on a sensitive column in a row listing, if it names the column:

```json
{ "table": "leads", "columns": ["id", "company", "source"],
  "filters": [{ "column": "email", "op": "is_null" }], "limit": 3 }
```

That returns ids 0, 36 and 72. The same filter inside an `aggregate` plan is refused with
`column "email" not found` in v0.1. `sensitive` is hygiene, not access control: see
[what AgeDB does not guarantee](../README.md#what-agedb-guarantees-and-what-it-does-not).

### An operations agent watching revenue

Database `shop`, table `orders`: id, customer_id, country, `status` (paid, refunded,
pending), payment_method, `gross_amount` ("charged at checkout, before refunds", summed by
default), `net_amount` ("kept after refunds"), `items`, created_at.

| Question | Read as | Answer |
| --- | --- | --- |
| total gross amount by country last month | created_at within last month; grouped by country; sum(gross_amount) | in 104.7k, de 104.7k, uae 99.6k, ... |
| total net amount by country last month | same window; sum(net_amount) | in 97.2k, de 95.5k, uae 92.5k, ... |
| average gross amount since 2026-06-01 | created_at gte 2026-06-01; avg(gross_amount) | 453.66 |
| biggest 10 orders by gross amount | 10 rows sorted by gross_amount descending | order 682 at 899.89 first |
| count orders where status is refunded by payment method | status eq "refunded"; grouped by payment_method; counting rows | bank_transfer 141, card 137, paypal 131 |
| orders where items is more than 5 | items gt 5 | 906 rows |

**Gross or net?** This is where plain language stops being enough. "Revenue" could mean
either column, and a sum can be correct arithmetic over the wrong definition. The schema
helps: two currency columns with clear descriptions, and only one declared as the default
sum. Anything more specific belongs in a plan, where the definition is explicit and
reviewable:

```json
{ "operation": "aggregate", "table": "orders",
  "filters": [{ "column": "status", "op": "ne", "value": "refunded" },
              { "column": "created_at", "op": "between", "values": ["2026-09-01", "2026-10-01"] }],
  "group_by": ["country"],
  "metrics": [{ "function": "sum", "column": "net_amount", "alias": "net_revenue" },
              { "function": "sum", "column": "gross_amount", "alias": "gross_revenue" },
              { "function": "count", "alias": "orders" }],
  "order_by": [{ "column": "net_revenue", "direction": "desc" }], "limit": 3 }
```

```
de   net 87,375.93   gross 87,375.93   180 orders
sg   net 84,069.92   gross 84,069.92   183 orders
uae  net 81,945.59   gross 81,945.59   185 orders
```

That shows three metrics in one answer, which plain language cannot express.

### An agent triaging a support queue

Database `support`, table `tickets`: id, `priority` (p1 to p4), `status` (open, pending,
closed), `assignee` (empty while unassigned), channel, `first_response_minutes` (averaged by
default), created_at.

| Question | Read as | Answer |
| --- | --- | --- |
| how many tickets where status is open by priority | status eq "open"; grouped by priority; counting rows | p3 138, p4 106, p2 50, p1 24 |
| count tickets where assignee is null | assignee is_null; counting rows | 89 |
| average first response minutes by assignee | grouped by assignee; avg(first_response_minutes) | amira 50.0, dana 49.6, ben 45.3, chen 42.1, unassigned 45.2 |
| which channel has the slowest first response | grouped by channel; avg(first_response_minutes); top 10 | phone 49.2, chat 47.1, email 43.7 |
| how many tickets were created this day | created_at within this day; counting rows | 6 (changes daily) |
| tickets where status is open sorted by created at limit 5 | status eq "open"; sorted by created_at ascending; at most 5 rows | the five oldest open tickets |

Watch for these:

| Asked | Result | Instead |
| --- | --- | --- |
| tickets created today | refused | "created this day" |
| first 5 tickets where status is open sorted by created at | two sorts on created_at, ascending then descending; the first wins | "... sorted by created at limit 5" |
| p1 or p2 tickets that are open | refused | a plan with `in` |

```json
{ "table": "tickets", "columns": ["id", "priority", "assignee", "created_at"],
  "filters": [{ "column": "status", "op": "eq", "value": "open" },
              { "column": "priority", "op": "in", "values": ["p1", "p2"] }],
  "order_by": [{ "column": "created_at", "direction": "asc" }], "limit": 5 }
```

That returns tickets 761 (p1), 760 (p2), 1177 (p2), 154 (p1) and 540 (p2, unassigned).
The plan filters on `status` without returning it, which is fine.

### An agent answering product analytics questions

Database `product`, table `events`: id, user_id, `event_type` (page_view, signup,
purchase, error), page, country, occurred_at.

| Question | Read as | Answer |
| --- | --- | --- |
| count events by event type | grouped by event_type; counting rows | page_view 16,058, error 1,997, signup 1,155, purchase 790 |
| how many events where event type is signup in the last 7 days by country | occurred_at in the last 7 days; event_type eq "signup"; grouped by country; counting rows | uk 53, de 48, sg 42, ... (changes daily) |
| top 5 pages by count of events | grouped by page; count; top 5 | /docs 3,401, /signup 3,352, /checkout 3,345, ... |
| count events where event type is error this week | occurred_at within this week; event_type eq "error"; counting rows | 28 (changes daily) |

A trap worth knowing:

| Asked | Read as | |
| --- | --- | --- |
| how many distinct users signed up | counting rows | **wrong**: 20,000 events, not distinct users, and "signed up" is ignored |

v0.1 has no distinct count. The closest honest answer is a count of signup events, with a
filter the parser can see: "how many events where event type is signup".

Two values of one column at once need a plan:

```json
{ "operation": "aggregate", "table": "events",
  "filters": [{ "column": "event_type", "op": "in", "values": ["signup", "error"] }],
  "group_by": ["page", "event_type"],
  "metrics": [{ "function": "count", "alias": "events" }],
  "order_by": [{ "column": "events", "direction": "desc" }], "limit": 4 }
```

### A research agent tracking prices

Database `research`, table `prices`: id, product, retailer, `price` (currency, averaged by
default, described as "Listed price at the time of observation"), `in_stock` (bool),
observed_at.

| Question | Read as | Answer |
| --- | --- | --- |
| average price by retailer | grouped by retailer; avg(price) | amazon 172.77, bestbuy 170.75, ikea 169.25, noon 164.01 |
| minimum price by product | grouped by product; min(price) | usb hub 14.46, desk lamp 28.06, monitor arm 72.11, ... |
| show observations where price is under 20 | price lt 20 | 233 rows |
| cheapest 5 observations by price | 5 rows sorted by price ascending | usb hub at noon 14.46 first |

This table shows the most surprising traps on the page:

| Asked | Read as | Why |
| --- | --- | --- |
| prices where price is under 20 | price lt 20; **avg(price)** | "prices" is also the plural of the column `price`, which is averaged by default, so a listing became an average |
| list observations where price is under 20 | price lt 20; **avg(price)** | "list" matched the word "Listed" in the column description |
| lowest price by product | **avg(price)** | "lowest" needs a count |
| latest price of standing desk | **max(price)** over all products | "latest" is maximum, and "standing desk" is not a filter |
| how many observations where in stock is false by retailer | refused: `type mismatch: expected bool, got "false"` | plain language does not read true or false yet |

"Show observations where price is under 20" works because nothing in it resolves to a
measure. The lesson is general: when a table is named after one of its own columns
(`prices` and `price`), start listings with "show".

The latest price per product, and anything true or false, needs a plan:

```json
{ "table": "prices", "columns": ["retailer", "price", "in_stock", "observed_at"],
  "filters": [{ "column": "product", "op": "eq", "value": "standing desk" }],
  "order_by": [{ "column": "observed_at", "direction": "desc" }], "limit": 3 }
```

```json
{ "operation": "aggregate", "table": "prices",
  "filters": [{ "column": "in_stock", "op": "eq", "value": false }],
  "group_by": ["retailer"], "metrics": [{ "function": "count", "alias": "out_of_stock" }] }
```

## Phrase reference

Everything the plain-language parser recognizes. Words are matched case-insensitively.

### Counting and metrics

| Say | Becomes |
| --- | --- |
| `how many`, `count`, `number of` | `count` of rows |
| `total`, `sum`, `summed`, `revenue`, `spend` + column | `sum(column)` |
| `average`, `avg`, `mean` + column | `avg(column)` |
| `minimum`, `min`, `earliest` + column | `min(column)` |
| `maximum`, `max`, `latest` + column | `max(column)` |

One metric per question: the first one wins. The metric's column is looked for in the
next few words. So:
* "total gross amount by country" sums `gross_amount`;
* "total by country" tries to sum `country` and is refused;
* "total revenue by country" works on a table with one currency column, because "revenue"
  resolves to it. On `orders`, which has two (gross and net), "revenue" matches neither,
  and `country` is summed instead, which is refused.

Name the measure column right after the metric word.

When no column follows at all ("what is the total for orders"), AgeDB picks one in this
order:
1. the column that declares this aggregation;
2. the only currency column;
3. the only numeric column.

If none of these is unique, it refuses: `avg() of which column? orders has several
candidates: gross_amount, net_amount, items; name one`. When a question names no metric
but mentions a column that declares a `default_aggregation`, that aggregation is used. This
is how "most likely to convert" becomes `avg(score)`.

| Asked | Read as |
| --- | --- |
| what is the total for orders | sum(gross_amount), the declared default |
| what is the average in leads | avg(score), the declared default |
| total orders | sum(gross_amount): **not a count**. Say "how many orders" |

### Grouping and ranking

| Say | Becomes |
| --- | --- |
| `by`, `per`, `across`, `each` + column | group by that column |
| `which`, `what`, `who` + a label column, first word of the question | group by it, sort by the metric descending, top 10 |
| grouping with no metric named | count of rows per group |

A "which ..." question only groups by label-like columns: categories, countries,
identifiers, or text with no default aggregation. "What is the total value of leads" does
not group.

### Top N, order and limits

| Say | Becomes |
| --- | --- |
| `top`, `highest`, `largest`, `biggest`, `best` + number | limit, highest first |
| `bottom`, `lowest`, `smallest`, `cheapest` + number | limit, lowest first |
| `first`, `limit`, `any` + number | limit |
| `top N <column> by <measure>` | group by column, rank by the measure's default aggregation |
| `top N <table> by <column>` | N rows sorted by column |
| `sorted by`, `ordered by`, `order by` + column | sort ascending |
| ... + `desc`, `descending`, `highest`, `down` | sort descending |

The ranking words need a number. "Lowest price" is not a ranking; "lowest 5 prices" is.
Use either `top N ... by` or `sorted by ... limit N`, not both in one question.

### Filters

All filters combine with AND. The column comes before the condition.

| Say | Becomes |
| --- | --- |
| `<column> is <value>`, `equals`, `=`, `==` | `eq` |
| `<column> is not <value>` | `ne` |
| `<column> greater than`, `more than`, `over` + number | `gt` |
| `<column> less than`, `fewer than`, `under` + number | `lt` |
| `<column> at least` + number | `gte` |
| `<column> at most` + number | `lte` |
| `<column> is null`, `is empty`, `is missing` | `is_null` |
| `<column> is not null`, `is not empty` | `is_not_null` |

Values are one word. They are read as numbers for numeric columns (`45,000` and `$45000`
both work) and as text otherwise. Multi-word values ("standing desk") and true/false are
not understood; use a plan.

### Time windows

Times are UTC, and weeks start on Monday.

| Say | Becomes |
| --- | --- |
| `last`, `past`, `previous` + N + `days`, `weeks`, `hours`, `minutes` | from N units ago until now |
| ... + N + `months`, `quarters`, `years` | 30, 91 or 365 days per unit |
| `this day`, `this week`, `this month`, `this quarter`, `this year` | the current calendar period |
| `last day`, `last week`, `last month`, `last quarter`, `last year` | the previous calendar period |
| `since`, `from` + date | on or after the date |
| `after` + date | after the date |
| `before`, `until` + date | before the date |
| `in`, `during`, `for` + year | that calendar year |

Dates are ISO: `2026-06-01` or `2026-06-01T09:00:00Z`. A window applies to the timestamp
column named just before it ("created at in the last 7 days"). Otherwise it applies to the
table's main timestamp: the only one there is, or the one with `semantic_type:
timestamp`, or the one whose name contains "created". "Today" alone is not understood;
say "this day".

### How column names are matched

A phrase in the question is matched to a column in this order, and the first match wins:

1. The exact name: `gross_amount`.
2. Spaces, hyphens and underscores are interchangeable: "gross amount", "gross-amount".
3. Plurals: "countries" matches `country`, and "items" matches `items`.
4. Synonyms, used only when exactly one column has the semantic type:

   | Words | Semantic type |
   | --- | --- |
   | revenue, sales, amount, spend, value, money, price, cost | currency |
   | country, market, region, nation | country |
   | date, time, when, day, timestamp | timestamp |
   | category, kind, type, segment, status | category |
   | email | email |
5. A prefix of at least four letters: "created" matches `created_at`.
6. A word found in exactly one column's description: "convert" matches `score`,
   described as "likelihood to convert".

Rule 6 is powerful and occasionally surprising. "List" matched a description containing
"Listed". Write descriptions in plain words, and check the interpretation when a question
uses a common verb.

### Tables

A question reads one table:
1. the table it names, singular or plural;
2. otherwise, the only table in the database;
3. otherwise, AgeDB asks "which table?".

Naming two tables is refused as `unsupported`, because it needs a join. So are "rows
missing from another table" questions (words such as "never", "without", "haven't").

## What plain language cannot do

Each of these is refused, or read in a narrower way than you meant. Every one of them works
as a plan.

| You want | Plan |
| --- | --- |
| OR, or a list of values | `{"column": "priority", "op": "in", "values": ["p1", "p2"]}` |
| NOT one of a list | `{"column": "status", "op": "not_in", "values": ["closed"]}` |
| A range | `{"column": "price", "op": "between", "values": [30, 40]}` |
| Text containing something | `{"column": "retailer", "op": "like", "value": "%a%"}` |
| True or false | `{"column": "in_stock", "op": "eq", "value": false}` |
| A multi-word value | `{"column": "product", "op": "eq", "value": "standing desk"}` |
| Several metrics at once | `"metrics": [{"function": "sum", ...}, {"function": "count"}]` |
| Grouping by two columns | `"group_by": ["page", "event_type"]` |
| Specific columns only | `"columns": ["id", "priority", "created_at"]` |
| Paging | `"limit": 3, "offset": 2` |

These are not available in v0.1 at all, in either form:
* joins;
* distinct counts;
* filtering on an aggregate (`HAVING`);
* computed columns ("price times items");
* ratios;
* aggregates over sensitive columns.

## Structured plans

```json
{
  "operation": "select | aggregate",
  "table": "orders",
  "columns": ["id", "status"],
  "filters": [{ "column": "status", "op": "eq", "value": "paid" }],
  "group_by": ["country"],
  "metrics": [{ "function": "sum", "column": "net_amount", "alias": "net" }],
  "order_by": [{ "column": "net", "direction": "desc" }],
  "limit": 20,
  "offset": 0
}
```

| Field | Notes |
| --- | --- |
| `operation` | `select` returns rows (the default). `aggregate` returns metrics, grouped or not |
| `table` | Required |
| `columns` | For `select`. Omit to get every non-sensitive column. Filters may use columns not listed here |
| `filters` | Combined with AND. `op` is one of `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `not_in`, `between`, `like`, `is_null`, `is_not_null`. Use `value` for one operand, `values` for `in`, `not_in` and `between` (exactly two). Dates are ISO strings |
| `group_by` | For `aggregate` |
| `metrics` | `function` is `count`, `sum`, `avg`, `min` or `max`. `column` is required except for `count`. `alias` names the output column; the default is `sum_net_amount` style, or `count` |
| `order_by` | Output columns or aliases. `direction` is `asc` (the default) or `desc` |
| `limit`, `offset` | `offset` needs a `limit`. A limit above the key's row budget is clamped, with a warning |

The validator checks every plan before it runs:
* unknown columns come back as `not_found`, with the real names;
* `sum` or `avg` of an identifier or an email is refused as meaningless;
* a filter value that does not fit the column type is a `type_mismatch`.

## Reading the answer

```json
{
  "rows": [{ "country": "de", "sum_net_amount": 95450.3 }],
  "interpretation": "reading orders (created_at within last month; grouped by country; sum(net_amount))",
  "plan": { "operation": "aggregate", "table": "orders", "...": "..." },
  "stats": { "bytes_scanned": 123084, "rows_scanned": 5000, "segments_read": 4,
             "segments_pruned": 0, "rows_returned": 6, "elapsed_ms": 3, "truncated": false },
  "warnings": []
}
```

* **`interpretation`** (plain language only): what was understood, in order. Compare it
  with the question.
* **`plan`**: the exact plan that ran. Reuse it: change a value and send it back as `plan`,
  and you get the same shape of answer with no parsing involved.
* **`stats.truncated`**: `true` means the key's row budget cut the result short. Add a
  filter or a limit.
* **`warnings`**: for example, a limit that was clamped to the budget.

A good loop for an agent:
1. Call `schema_get` or `table_describe` once.
2. Ask in plain language.
3. Check `interpretation` against the intent.
4. On a refusal or a misreading, send a plan, starting from the echoed one.
5. Save plans that work for questions that repeat.

## When a query fails

| Error | Meaning | What to do |
| --- | --- | --- |
| `bad_request`: which table? this database has: ... | Several tables, none named | Name the table in the question |
| `bad_request`: could not turn "..." into a query plan | Nothing recognizable: no metric, grouping or condition | Add one ("how many ...", "... by country", "... where status is open") or send a plan |
| `bad_request`: avg() of which column? ... has several candidates | A metric with no column and no obvious default | Name the column after the metric word |
| `bad_request`: sum(x) is not meaningful ... | A metric on text, an id or an email, often because no measure column followed the metric word, so the next column was taken | Name a numeric column after the metric word |
| `not_found`: column "x" not found | No such column, or a sensitive one referenced in plain language | Check `table_describe`; for a sensitive column, use a plan that names it |
| `type_mismatch` | A value that does not fit the column, such as "false" for a bool in plain language | Send a plan with a JSON value |
| `unsupported`: ... needs a join | The question spans two tables | Ask one table at a time |
| `limit_exceeded` (HTTP 429) | Byte or time budget spent | Add a filter or a time window |

## Designing tables so questions work

Most plain-language quality is decided when the table is created:

* **Give every column a `description`** in the words people will use. "Model-predicted
  likelihood to convert, 0 to 1" is why "most likely to convert" works.
* **Set `semantic_type`.** Use `category` for labels you group by, `currency` for money,
  `id` for identifiers (so they are never summed), and `timestamp` for the main time
  column.
* **Declare `default_aggregation`** on the measure people mean when they do not say, for
  example `sum` on `gross_amount`. Declare it on one currency column, not on all of them.
* **Keep one obvious timestamp**, or mark the main one with `semantic_type: timestamp`.
* **Name tables apart from their columns.** A `prices` table with a `price` column turns
  listings into averages; `price_observations` would not.
* **Keep one question to one table.** If agents keep asking "leads that became customers",
  store `converted` on `leads`.

The full column options are in [`usage.md`](usage.md#designing-a-table).
