# Joins Job

## Overview

This job sits on top of the completed star schema in `data_warehouse`
(`users_dim`, `products_dim`, `sales_transactions_fact`, built by the
`gold-jobs/dims-&-fact` job). It connects directly to Postgres via
`psycopg2` (no DuckDB in this job), adds the primary/foreign key
constraints the star schema was still missing, indexes the fact table's
foreign key columns, and runs the base dimension-to-fact join that any
downstream analytical query would build on.

---

## Why `psycopg2` instead of DuckDB here

Every other job in this project (`bronze-ingest`, `silver-transform`,
`dims-fact`) uses DuckDB attached to Postgres (`ATTACH ... TYPE postgres`)
as the processing engine, for vectorized performance on larger transforms.

This job is different on purpose: its core task is adding real
`PRIMARY KEY` / `FOREIGN KEY` constraints, and **DuckDB's Postgres
attachment cannot do this** — confirmed directly by testing it, which
returned:
```
Not implemented Error: BindAlterAddIndex not supported by this catalog
```
This is a hard, unimplemented limitation of DuckDB's Postgres catalog
integration, not a syntax or permissions issue. The only reliable way to
create these constraints is a direct Postgres connection, which is what
`psycopg2` provides. Since this job's primary purpose is schema
constraints, it made sense to use `psycopg2` throughout rather than mixing
two connections for one small script.

The data sizes involved here are also small enough (fact table: 18,367
rows; `products_dim`: ~67,354; `users_dim`: 176,639) that DuckDB's
vectorized engine wouldn't have offered a meaningful performance advantage
for the join itself — the choice of `psycopg2` here is about correctness
(constraints) and simplicity (one connection, one tool), not a performance
trade-off.

---

## What the job does

1. **Adds `PRIMARY KEY` constraints** on `users_dim.user_key` and
   `products_dim.product_key`. These are the dimension tables' natural
   surrogate keys, and were never given real constraints when the tables
   were originally created (since that creation happened through DuckDB's
   `ATTACH`, which has the same limitation described above).
2. **Adds `FOREIGN KEY` constraints** (`fk_user`, `fk_product`) on
   `sales_transactions_fact`, referencing the two dimension tables' new
   primary keys. This gives the schema real, enforced referential
   integrity, and is also what allows external tools (e.g. DBVisualizer's
   References diagram) to correctly draw the relationships between the
   tables — without these constraints, such tools show no links at all,
   even though the data itself already lines up correctly.
3. **Indexes the fact table's foreign key columns**
   (`idx_fact_user_key`, `idx_fact_product_key`). Postgres does not
   automatically index the referencing side of a foreign key (a
   well-known Postgres behavior, true in any schema) — so these are
   added explicitly to keep joins fast as the fact table grows, avoiding
   a full sequential scan of the (much larger) dimension tables on every
   join.
4. **Runs the base join**, reuniting the fact table's transactional data
   (`price`, `event_time`) with the dimension tables' readable identity
   (`user_id`, `product_id`, `brand`) via `user_key`/`product_key`.

---

## Problems hit while building this, and how they were resolved

### 1. Confusing which connection could do what
Early attempts tried running `ALTER TABLE ... ADD PRIMARY KEY` through the
DuckDB `con` connection (with the `pg.` prefix used everywhere else in the
project). This consistently failed. It took directly testing a single,
isolated statement with explicit success/failure printing to get the real
error message (`BindAlterAddIndex not supported by this catalog`) rather
than continuing to guess — this confirmed definitively that constraint
DDL must go through `psycopg2`, never DuckDB, regardless of syntax used.

### 2. Rollback undoing already-successful statements
Several early attempts wrapped all four `ALTER TABLE` statements in one
`try` block with a single `commit()` at the end. When a later statement in
the same block failed, the `except: rollback()` undid **every** statement
in that transaction — including earlier ones that had genuinely succeeded
moments before. This produced confusing, seemingly contradictory results
(a constraint appearing to exist, then appearing to not exist again).

**Fix:** commit immediately after each individual statement succeeds, so a
later failure can only roll back the one statement currently in progress,
never anything already committed.

### 3. Diagnosing "already exists" errors correctly
Once constraints started succeeding individually, later re-runs of the same
statements (naturally) failed with errors like `multiple primary keys for
table "users_dim" are not allowed` or `constraint "fk_user" ... already
exists`. These are not bugs — they're Postgres correctly confirming the
constraint is already in place. Verified this directly by querying
`pg_constraint` for the `data_warehouse` schema, which is the authoritative
way to check current constraint state rather than assuming based on a
tool's cached diagram view (DBVisualizer's ER diagram, for example, needs
a manual refresh to reflect newly added constraints).

### 4. Docker build failure: `psycopg2` vs `psycopg2-binary`
The container build failed at `pip install -r requirements.txt` with a
non-obvious error. Root cause: `requirements.txt` listed plain `psycopg2`,
which needs to compile from C source at install time (requiring
PostgreSQL's development headers and a C compiler) — neither of which
exist in the `python:3.11-slim` base image used by the Dockerfile.

**Fix:** use `psycopg2-binary` instead, which ships as a precompiled
wheel and installs cleanly with no system dependencies.

---

## Known open item

The script as currently written attempts to (re-)add all four constraints
every time it runs, with no "already exists, skip" check. Since the
constraints are now permanently in place, running this job again (e.g. via
`docker compose run --rm joins`) will fail on the very first `ALTER TABLE`
statement, print that error, and never reach the join query at the bottom.
This is harmless (nothing is broken or lost), but means the container's
output currently ends in an expected error rather than a successful join
result on repeat runs. A cleaner version would check `pg_constraint` first
and only add a constraint if it's missing, so the same script can run
safely and usefully both on a fresh database and on one that already has
the constraints in place.
