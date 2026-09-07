# Bronze Job — CSV → Parquet Ingestion (DuckDB)

## What this is

A standalone ingestion script that loads a raw CSV into a **Bronze layer** made of
Parquet files, using **DuckDB** as the engine. It is the concrete implementation
of every point in [`csv_ingestion_features_checklist.md`](./csv_ingestion_features_checklist.md).

- **Engine:** DuckDB (no server, single binary — the only dependency)
- **Data store:** a Parquet *dataset directory*, `data/raw-data/`
- **Control plane:** a small DuckDB file, `data/pipeline.duckdb` (bookkeeping only — no row data)
- **Source:** one fixed CSV path, `data/2019-Nov.csv` (an e-commerce events file, ~1.05M rows)

---

## Why the data and the bookkeeping are separated

A Parquet file is **write-once** — it cannot be appended to or updated. That makes it
a great archival/query format but a bad place to keep mutable run state such as
"chunk 3 is in progress" or "chunk 5 failed twice".

So the design splits into two stores:

| Store | Path | Holds | Mutable? |
|---|---|---|---|
| **Bronze data** | `data/raw-data/` | the actual rows, as Parquet parts | no — parts are immutable, written once |
| **Control plane** | `data/pipeline.duckdb` | state / rejects / registry / locks | yes — updated as the run progresses |

None of the source rows are ever stored in DuckDB. DuckDB is only the ledger.

### Layout of the Parquet dataset

```
data/raw-data/
  source=2019-Nov/
    ingest_date=2026-09-07/
      chunk_00000.parquet
      chunk_00001.parquet
      ...
```

Hive-partitioned by `source` and `ingest_date`. The whole folder is read as one table:

```sql
SELECT * FROM read_parquet('data/raw-data/**/*.parquet',
                           hive_partitioning = true, union_by_name = true);
```

### The 4 control tables (`pipeline.duckdb`)

| Table | One row per | Purpose |
|---|---|---|
| `source_registry` | (source, content fingerprint) | file identity, header, row/reject counts, status |
| `ingest_state`     | (source, fingerprint, chunk) | per-chunk status, attempts, offset, Parquet path, error |
| `dlq`              | rejected row | dead-letter queue with full diagnostic context |
| `locks`            | resource | single-writer lease |

---

## How a run works

```
uv run python jobs/bronze-jobs/load.py
```

1. **Validate config** — paths, chunk size, encoding, thresholds. Bad config → exit before touching data.
2. **Acquire lock** — write a lease row in `locks`; refuse to start if another run holds a live one. Released in a `finally`.
3. **File-stability check** — stat the CSV twice; if size/mtime is still changing, abort (file still being written).
4. **Fingerprint** — SHA-256 + size + mtime. This, not the filename, decides whether the content was seen before.
5. **Re-ingest decision:**
   - same fingerprint, already `complete` → print `noop` and exit (this is what makes re-runs safe)
   - changed fingerprint → `replace` (default) / `append` / `reject` per `REINGEST_MODE`
6. **Header validation** — compare CSV header to `EXPECTED_COLUMNS`; columns are mapped **by name**, so reordering is fine; missing/extra columns or drift vs. the last run → abort (configurable).
7. **Staging + reject routing** — read raw lines, split on the delimiter, judge field count ourselves:
   - right width → valid
   - too many non-empty fields → `extra_field` reject
   - too few fields → `short_row` reject
   - optional duplicate on `DEDUP_KEYS` → `duplicate` reject

   All rejects go to `dlq` (source, line number, raw line, reason, timestamp). Nothing is dropped silently.
8. **Reject-rate gate** — if > `REJECT_THRESHOLD` (5%) of rows reject, abort the whole source instead of loading a broken file.
9. **Chunk loop** (200k rows each):
   - already `success` → skip (resumability)
   - failed ≥ `MAX_ATTEMPTS_LIFETIME` (5) across runs → `quarantine` and skip (poison chunk)
   - otherwise: mark `in_progress` → `COPY` the slice with enrichment columns to a **temp file** → verify row count → `os.replace()` to the final name (**atomic — this is the commit**) → mark `success`
   - all state writes are in a transaction; on error it rolls back, deletes the temp file, and still records a `failed` row
   - each chunk is wrapped in a **retry loop**: 3 attempts, linear backoff (2s, 4s); permanent failure is logged and the run continues
10. **Reconcile** — every `success` chunk whose Parquet file is missing is flipped back to `failed` so the next run redoes it.
11. **Finish** — drop temp views, sweep stray `._tmp_` files, update `source_registry` to `complete` / `partial`, print a JSON summary.

### Enrichment columns added to every row

`chunk_id`, `source_id`, `source_file`, `content_hash`, `ingest_date`, `ingested_at` (UTC).

---

## Commands

| Command | Effect |
|---|---|
| `uv run python jobs/bronze-jobs/load.py` | ingest the configured CSV |
| `... --force` | re-ingest even if the fingerprint is unchanged |
| `... --reingest-mode replace\|append\|reject` | behavior when the file changed |
| `... --csv <path>` | override the source path |
| `... --prune-days 30` | delete DLQ rows older than 30 days, then exit |
| `... --selftest` | run built-in fixture tests, then exit |

### Docker

Packaged as a one-shot image (`db-project/bronze-ingest:latest`): `python:3.11-slim`,
deps installed with `uv`, `data/` copied in, `ENV DATA_DIR=/app/data`. `load.py` reads
`DATA_DIR` (and optional `CSV_NAME`), so mounting a host `data/` over `/app/data`
redirects both the source CSV and all outputs to the host.

```bash
# build
docker build -t db-project/bronze-ingest:latest jobs/bronze-jobs

# run standalone — args after the image name pass straight to load.py
docker run --rm db-project/bronze-ingest:latest --selftest
docker run --rm -v "$PWD/jobs/bronze-jobs/data:/app/data" db-project/bronze-ingest:latest

# via compose — service is under the `jobs` profile, so `docker compose up` skips it
docker compose run --rm bronze-ingest
docker compose run --rm bronze-ingest --force
```

---

## Verified run (real data)

`data/2019-Nov.csv` — 1,048,575 rows:

```json
{
  "source_id": "2019-Nov",
  "status": "complete",
  "total_valid_rows": 1048575,
  "total_rejected": 0,
  "chunks_total": 6,
  "chunks_written_this_run": 6,
  "chunks_skipped": 0,
  "chunks_failed": 0,
  "dataset_total_rows": 1048575,
  "elapsed_seconds": 18.3
}
```

- 6 zstd-compressed Parquet parts written in ~18s
- second run → `status: noop` (resumability confirmed)
- `--selftest` passes: valid-row count, `extra_field` reject, `short_row` reject, empty-file no-op, idempotent double run

---

## Checklist coverage

All 25 sections of `csv_ingestion_features_checklist.md` are implemented and tagged
`S1`..`S25` in the source:

| S1 minimal schema | S2 resume via state log | S3 chunked processing | S4 manual row validation | S5 DLQ |
|---|---|---|---|---|
| **S6** txn + atomic rename | **S7** enrichment | **S8** Parquet dataset | **S9** retry + backoff | **S10** cleanup |
| **S11** observability / summary | **S12** fingerprint id | **S13** header validation + drift | **S14** encoding / empty-file | **S15** raw-text typing |
| **S16** dedup + re-ingest modes | **S17** file-stability check | **S18** streamed chunk planning | **S19** single-writer lock | **S20** reject threshold + poison chunks |
| **S21** verify + reconcile | **S22** DLQ prune | **S23** config fail-fast | **S24** UTC everywhere | **S25** self-test fixtures |

---

## Known limitations (deliberate)

1. **Naive delimiter split** — does not honor quoted fields containing the delimiter.
   The configured source has none; a comment marks where to switch the valid-row
   path to DuckDB's real CSV parser if that changes.
2. **`--prune-days` trims the DLQ only** — it does not drop old `ingest_date`
   Parquet partitions or their state rows, because that could silently break
   resumability. Partition retention is left as an explicit manual step.

---

## Files

| File | Role |
|---|---|
| `load.py` | the ingestion script |
| `csv_ingestion_features_checklist.md` | the tool-agnostic design checklist it implements |
| `data/2019-Nov.csv` | source data (git-ignored output dirs: `data/raw-data/`, `data/pipeline.duckdb`) |
| `Dockerfile`, `requirements.txt` | container scaffolding |
