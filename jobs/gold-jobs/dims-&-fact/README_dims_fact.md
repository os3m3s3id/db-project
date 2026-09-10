# Dims & Fact (Data Warehouse) Job

## Overview

This job is the third stage of the pipeline, sitting on top of the **silver**
layer. It reads the cleaned event data in `silver.silver_table` and builds a
**star schema** in a new `data_warehouse` schema in Postgres: two dimension
tables (`products_dim`, `users_dim`) and one fact table
(`sales_transactions_fact`).

Like `silver-transform`, this job uses **DuckDB** as the processing engine,
attached to Postgres via its `postgres` extension — DuckDB does the
deduplication, ranking, and joins in its vectorized engine, then writes the
result straight into Postgres.

This work originated from a course assignment (Session 2: design a DWH star
schema from source tables; Session 3: query it with JOINs, subqueries, and
CTEs) and was adapted to fit our actual dataset, which is e-commerce
clickstream/event data — not the "typical" customer/sales dataset the
assignment slides assumed.

---

## Why the schema differs from the assignment's original design

The assignment template called for `Country_Dim`, `Customer_Dim`,
`Product_Dim`, and `Sales_Transactions_Fact`, assuming customer records with
country/profile data and a clean transactional sales table. Our actual data
doesn't have that shape:

| Assignment expected | What we actually have |
|---|---|
| `Country_Dim` (customer country) | No country data anywhere in the source — **dropped entirely**. |
| `Customer_Dim` (name, profile, etc.) | Only an anonymous `user_id` — renamed to **`users_dim`**, holding just `user_id`. |
| `Product_Dim` | Maps cleanly: `product_id`, `category_id`, `category_code`, `brand` → **`products_dim`**. |
| `Sales_Transactions_Fact` (discrete sales) | Clickstream event data — `event_type` is `view` / `cart` / `purchase`, not inherently "a sale." |

**Decision: `sales_transactions_fact` is filtered to `event_type =
'purchase'` only.** This makes it a true sales-transactions fact table
(18,367 rows), rather than including all 1,047,963 raw events (which are
mostly `view`s — 1,014,628 of them — with 14,968 `cart` adds). Views/carts
are intentionally excluded from the fact table, since they aren't sales.

---

## Star schema shape

- **`data_warehouse.products_dim`** — one row per unique `product_id`
  (~67,354 rows). Columns: `product_key` (surrogate PK), `product_id`,
  `category_id`, `category_code`, `brand`.
- **`data_warehouse.users_dim`** — one row per unique `user_id` (176,639
  rows). Columns: `user_key` (surrogate PK), `user_id`.
- **`data_warehouse.sales_transactions_fact`** — one row per purchase event
  (18,367 rows). Columns: `sales_trans_key` (surrogate PK), `event_time`,
  `event_type`, `price`, `user_session`, `user_key` (FK), `product_key`
  (FK).

This is the same core distinction we reinforced explicitly: `silver` holds
one row **per event** (a user can appear hundreds of times), while the
dimension tables hold one row **per entity** (that same user still only gets
one row in `users_dim`, no matter how many events they generated) — the
fact table is what re-links the many events back to the single dimension
rows, via foreign keys.

---

## Problems hit along the way, and how we solved them

### 1. `SERIAL PRIMARY KEY` isn't available through DuckDB's Postgres `ATTACH`
The first version of this script (via SQLAlchemy, run directly against
Postgres) used `CREATE TABLE ... AS SELECT` followed by
`ALTER TABLE ... ADD COLUMN key SERIAL PRIMARY KEY`. This works once, but
breaks on any second run (the column already exists) and isn't incremental
at all — re-running does nothing to pick up new data.

**Fix:** table structures are now defined upfront in a single
`CREATE TABLE IF NOT EXISTS`, and surrogate keys are generated manually:
```sql
(SELECT COALESCE(MAX(key), 0) FROM target_table) + ROW_NUMBER() OVER ()
```
This computes the next key by taking the current max key in the table (0 if
empty) and adding a fresh `ROW_NUMBER()` count for the new rows being
inserted — giving unique, increasing keys across repeated runs without
relying on a native Postgres sequence object, which DuckDB's `ATTACH`
doesn't create for us.

### 2. The job needed to be genuinely incremental
Matching the same principle as `silver-transform`: re-running this job
should only add rows that don't already exist, not reprocess everything or
error. Each insert uses `NOT EXISTS` (anti-join) against the target table:
- `products_dim` / `users_dim`: skip any `product_id` / `user_id` already
  present.
- `sales_transactions_fact`: skip any row already present, matched on a
  composite key (`user_session`, `event_time`, `product_key`), since there's
  no single natural ID on the source event rows.

### 3. Performance — first run took 16+ minutes
Root cause: no indexes existed on the columns being compared in the
`NOT EXISTS` checks, so every comparison triggered a full table scan.

**Fix:** added indexes on `product_id` / `user_id` (unique, doubling as the
uniqueness guarantee since DuckDB's `ATTACH` doesn't create native Postgres
constraints for us), a composite index on the fact table's dedup key, and
indexes on `silver.silver_table`'s `product_id` / `user_id` / `event_type`
columns (since every insert scans silver by these). This brought the first
run down to ~3.5 minutes — the remaining time being the one-time cost of
building those indexes across ~1M rows, which isn't paid again on later
runs.

### 4. Duplicate key violation on `products_dim`
`SELECT DISTINCT product_id, category_id, category_code, brand` does not
guarantee one row per `product_id` — it guarantees one row per unique
*combination* of all four columns. Some `product_id`s appeared in the
source with conflicting `category_code`/`brand` values across different
events (data drift), producing two "distinct" rows sharing the same
`product_id` — which violated the table's `UNIQUE` constraint on
`product_id`.

**Fix:** rank each `product_id`'s candidate rows by `event_time DESC` using
`ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time DESC)`, and
keep only the top-ranked (`rn = 1`) — i.e., the most recent attributes for
that product. Guarantees exactly one row per `product_id`, regardless of
how many conflicting variants exist upstream. `users_dim` didn't need this
fix, since it selects only `user_id` and nothing else.

### 5. Considered but did not apply: partitioning
Discussed adding partitioning (e.g. by `event_time`) to speed up the load.
Concluded it wouldn't help this specific workload: the load script's
`NOT EXISTS` dedup checks and joins scan/compare against the *entire*
target table's content, not a date-filtered slice — partitioning only pays
off for queries that can skip irrelevant partitions. Partitioning would be
worth revisiting later for the **fact table**, once it grows large and is
being queried with date-range filters by downstream reporting — not for
this load script's own performance.

### 6. Considered and adopted: DuckDB instead of plain SQLAlchemy/Postgres SQL
Reused the same DuckDB-attached-to-Postgres pattern from `silver-transform`,
for consistency and to take advantage of DuckDB's vectorized execution
engine for the dedup/join-heavy transform logic, while still writing the
final result into Postgres through the same `ATTACH` connection.

---

## Development process

1. Reviewed the course assignment's original star-schema requirements
   (Session 2 slides) and identified the mismatch with our actual event
   data (no country, no customer profile, no discrete "sale" concept beyond
   `event_type = 'purchase'`).
2. Adapted the schema: `Country_Dim` dropped, `Customer_Dim` →
   `users_dim`, `Product_Dim` → `products_dim`,
   `Sales_Transactions_Fact` → `sales_transactions_fact`, filtered to
   purchase events.
3. Wrote an initial version via SQLAlchemy directly against Postgres;
   identified it wasn't safely re-runnable (`ALTER TABLE` crash on second
   run) and wasn't incremental.
4. Rewrote using `CREATE TABLE IF NOT EXISTS` with full structure upfront,
   and `NOT EXISTS`-based incremental inserts.
5. Switched the whole script to DuckDB (attached to Postgres), matching the
   `silver-transform` pattern, for vectorized performance.
6. Diagnosed and fixed a 16+ minute first run by adding indexes on both the
   source (`silver.silver_table`) and target (`data_warehouse.*`) tables.
7. Diagnosed and fixed a duplicate-key error on `products_dim` caused by
   `product_id`s having conflicting attributes across events, using
   `ROW_NUMBER()` to pick one representative row per product.
8. Validated expected row counts before running the load:
   `event_type` breakdown (1,014,628 view / 14,968 cart / 18,367 purchase),
   distinct `product_id` count (67,354), distinct `user_id` count
   (176,639) — to know what to expect in each target table.
9. Converted the validated notebook (`dims-fact.ipynb`) to a script via
   `jupyter nbconvert --to script dims-fact.ipynb`.
10. Containerized the job: `Dockerfile`, `requirements.txt`,
    `.dockerignore`, and registered as a one-shot `dims-fact` service in
    `docker-compose.yml`, following the same profile-based pattern as
    `bronze-ingest` and `silver-transform`. No volume mount needed, since
    this job reads/writes entirely within Postgres (no local files
    involved).

---

## Known open items / next steps

- No native Postgres `PRIMARY KEY` / `FOREIGN KEY` constraints on the
  `data_warehouse` tables — DuckDB's `ATTACH` has limited support for
  creating these. Uniqueness is currently only enforced via `UNIQUE`
  indexes on `product_id` / `user_id`; true FK enforcement on
  `sales_transactions_fact` would need to be added separately via a plain
  Postgres client if required by the assignment's grading criteria.
- Surrogate key generation (`MAX(key) + ROW_NUMBER()`) is only safe for a
  single job running at a time — concurrent runs could read the same `MAX`
  and generate colliding keys. Not a concern for the current scheduled,
  one-at-a-time usage.
- Partitioning was considered but not applied; worth revisiting for
  `sales_transactions_fact` specifically once it grows large and is queried
  with date filters by downstream reporting/assignments.
- Debug/inspection cells from the notebook (event-type breakdown, distinct
  counts) carry over verbatim into the converted script.
