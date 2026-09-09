# Silver Transform Job

## Overview

This job is the second stage of the medallion-architecture pipeline. It reads
raw event data landed by `bronze-ingestion` (CSV → Parquet), cleans and
standardizes it, and loads the result into a **silver** schema in Postgres.
Rows that fail validation are never dropped and never crash the job — they are
routed to a **dead-letter queue (DLQ)** table for later inspection.

The job is built with **DuckDB** as the processing engine end-to-end, for
consistency with `bronze-ingestion` (which also uses DuckDB) and for
performance — DuckDB reads Parquet natively and streams data through its
vectorized query engine rather than loading whole files into memory.

---

## Architecture decisions and why we made them

### 1. DuckDB as the processing engine, Postgres as the destination
DuckDB does not store this pipeline's real data long-term. It acts purely as
a client/engine: it reads the bronze Parquet files, applies all cleaning
logic in SQL, and — via its `postgres` extension (`ATTACH ... TYPE
postgres`) — writes the results directly into Postgres. Every SQL statement
prefixed with `pg.` is forwarded through this attached connection and
executed for real against Postgres; nothing is stored inside DuckDB itself.

We chose to do the cleaning **inside DuckDB SQL** (not pandas) to stay
vectorized and memory-light, even though each bronze file has ~200,000 rows.

### 2. Incremental processing via a manifest table
The job must be safe to re-run repeatedly without reprocessing files it has
already loaded (which would create duplicate rows). A **manifest table**,
`pg.meta.silver_processed_files`, tracks every bronze file this job has
attempted, with a `status` (`success` / `failed`) and row-count breakdown.
On each run, the job compares the bronze folder against the manifest and
only processes files not yet marked `success`.

### 3. Manifest lives in Postgres, not in DuckDB
`bronze-ingestion`'s own state log stays local (in DuckDB), because its
source, destination, and engine are all local — there's no external
transactional system to gain from. `silver-transform` is different: its
destination is Postgres, so the manifest was deliberately placed **in
Postgres too**, so it can be updated in the **same transaction** as the
silver/DLQ inserts. This guarantees the data and its tracking state can
never disagree — if anything fails mid-file, the whole transaction rolls
back, and the file is safely retried on the next run.

### 4. Batching: 20 files per run
Rather than processing an unbounded number of pending files in one run (which
could make run time unpredictable if a large backlog builds up), the job
caps itself at a batch of 20 files (`BATCH_SIZE`). A large backlog drains
incrementally across multiple runs instead of one long, all-or-nothing
attempt.

### 5. DLQ instead of silent drops or hard crashes
Any row that fails a cleaning rule is written to `pg.silver.dlq_events`
rather than being dropped or causing the job to fail. The DLQ preserves the
row's **original, uncleaned** values (as a JSON blob) plus a
`rejection_reason` code, so nothing is silently lost and failures are fully
auditable.

### 6. DLQ schema: JSON instead of typed columns
The DLQ table uses a single `raw_data JSON` column instead of mirroring the
source schema with typed columns. Reasoning: the DLQ's entire purpose is to
catch the unexpected. A rigid, typed DLQ schema could itself fail to accept
a malformed row (e.g. a value too long for its column type) — the same
class of failure it exists to prevent. JSON is schema-agnostic and always
accepts whatever came in, and also survives upstream schema drift in
bronze without needing a migration.

### 7. `rejection_reason` taxonomy
A fixed, small vocabulary of rejection codes, derived directly from the
cleaning rules:
- `missing_identifier` — row has no usable `user_id`.
- `type_cast_failure` — `event_time` or `price` could not be cast to their
  target types.

### 8. Cleaning rule order (and why this specific order)
1. **Select required event columns** — drop bronze-only pipeline metadata
   (`chunk_id`, `source_id`, `content_hash`, `ingest_date`, `ingested_at`)
   that isn't business data.
2. **Flag missing identifiers** (and required fields) for DLQ — done early,
   before any further cleaning work is spent on rows that are going to be
   quarantined anyway.
3. **Trim whitespace.**
4. **Lowercase text/varchar values.**
5. **Normalize formats** (e.g. phone numbers) — not applicable to this
   dataset (no phone column), kept as an explicit, intentionally-skipped
   step.
6. **Convert column types** — `event_time` → `TIMESTAMP`, `price` →
   `DOUBLE`, using `try_cast` so a single bad value returns `NULL` (caught
   and tagged `type_cast_failure`) instead of crashing the whole query.
7. **Remove duplicates** — defined as an exact match on
   `user_id + product_id + event_type + event_time` (a re-ingested
   identical event), **not** any two rows sharing the same `user_id` (a
   user legitimately generates many different events). Kept: the most
   recent row by `transformed_at` (the time this job processed the row,
   not the source event time).
8. **Split**: rows with no rejection tag → `pg.silver.silver_table`; tagged
   rows → `pg.silver.dlq_events`.

Standardization (trim/lowercase) happens **before** deduplication on
purpose: two rows representing the same event might differ only by
whitespace or casing. Deduping before standardizing would treat them as
different rows and let both through; standardizing first makes true
duplicates collapse correctly.

### 9. Why `user_id` alone was rejected as the dedup key
Initially considered using `user_id` as the row identifier for
deduplication. Rejected: this is event data, not customer-record data — one
user generates many legitimate events (view, cart, purchase). Deduping on
`user_id` alone would have collapsed all of a user's activity down to a
single row. The composite key (`user_id`, `product_id`, `event_type`,
`event_time`) correctly identifies a genuine duplicate event instead.

### 10. Transactional processing per file
Each file's full cycle — clean, dedup, split, insert into silver, insert
into DLQ, update the manifest — is wrapped in a single
`BEGIN TRANSACTION` / `COMMIT`. If any step fails, `ROLLBACK` undoes
everything for that file; it is never marked `success`, so the next run
retries it automatically. No duplicates, no half-loaded files.

---

## Development process

1. **Connected DuckDB to Postgres** via the `postgres` extension
   (`ATTACH ... TYPE postgres`), with all credentials read from environment
   variables (`.env`) — never hardcoded.
2. **Created the schemas**: `silver` (clean data + DLQ) and `meta`
   (manifest), both inside Postgres.
3. **Created the tables**: `pg.silver.silver_table`, `pg.silver.dlq_events`,
   `pg.meta.silver_processed_files`.
4. **Located and read the bronze Parquet files**, which are stored in a
   Hive-partitioned folder structure
   (`raw-data/source=.../ingest_date=.../chunk_*.parquet`), using a
   recursive glob (`**/*.parquet`) and `filename=True` to track provenance.
5. **Built and validated the pending-files query** — compares bronze files
   on disk against the manifest, returns only unprocessed files, capped at
   the batch size.
6. **Prototyped the full cleaning sequence (steps 1–8 above) manually, one
   step at a time, against a single real file** in a Jupyter notebook —
   inspecting intermediate results at each stage before trusting the logic,
   rather than building it all at once.
7. **Validated the split and load** into silver/DLQ for that one file, and
   reconciled row counts (`raw total − duplicates removed = silver + DLQ`)
   to confirm correctness.
8. **Wrapped the validated logic into a loop** over all pending files, with
   per-file transactions and a run summary printed at the end (files
   succeeded/failed, rows to silver, rows to DLQ).
9. **Converted the notebook to a standalone script** (`transform.py`) via
   `jupyter nbconvert --to script transform.ipynb`, so the exact same logic
   can run unattended, outside a notebook environment.
10. **Ran the script directly and verified**: first run processed all 6
    pending bronze files successfully (0 DLQ rows — this dataset turned out
    to be clean, only exact-duplicate rows were removed); confirmed
    resumability by re-running and confirming already-processed files are
    correctly skipped.
11. **Containerized the job**: added a `Dockerfile`, `requirements.txt`, and
    `.dockerignore`, and registered it as a one-shot `silver-transform`
    service in the project's `docker-compose.yml`, following the same
    profile-based, manually-triggered pattern already used for
    `bronze-ingest`.

---

## Known open items / next steps

- `bronze_path` is currently a relative path suited to running on the host
  machine; when running inside the Docker container it needs to instead
  read from an environment variable pointing at the mounted volume path
  (e.g. `/app/bronze-data`), for consistency with how Postgres connection
  details are already configured.
- No healthcheck/wait-for-Postgres logic yet between `postgres` and
  `silver-transform` in `docker-compose.yml` — `depends_on` only waits for
  the container to start, not for Postgres to be ready to accept
  connections.
- Debug/inspection cells from the notebook (`.fetchall()[:5]` peeks) carry
  over verbatim into the auto-converted script; a hand-cleaned version would
  drop these for a slightly leaner production script.
- No alerting yet on DLQ volume — a sudden spike in `row_count_dlq` for a
  file is often a sign of an upstream schema change and could be worth
  flagging automatically in the future.
