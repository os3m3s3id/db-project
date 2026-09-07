from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone, date
from pathlib import Path

import duckdb

# ============================================================================
# S23  CONFIGURATION (validated before any data is touched; fail-fast)
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))   # overridable (Docker)

CSV_PATH = DATA_DIR / os.environ.get("CSV_NAME", "2019-Nov.csv")  # single fixed source (S17: no scan)
RAW_DATASET_DIR = DATA_DIR / "raw-data"       # S8 primary store = Parquet dataset
CONTROL_DB = DATA_DIR / "pipeline.duckdb"     # S1 control plane only

# S1  the ONLY persisted tables (no clean-data table -> that lives in Parquet)
T_REGISTRY = "source_registry"   # S12 one row per (source, content fingerprint)
T_STATE = "ingest_state"         # S2  chunk-level run log (resumability)
T_DLQ = "dlq"                    # S5  rejected rows
T_LOCKS = "locks"                # S19 single-writer lease

# S13  expected header (order-independent; columns are mapped BY NAME below)
EXPECTED_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
]
ALLOW_EXTRA_COLUMNS = False       # S13  extra header columns -> fail unless True
STRICT_HEADER = True              # S13  header drift vs last run -> fail unless False

# S14  parsing dialect
ENCODING = "utf-8"               # DuckDB supports: utf-8 / latin-1 / utf-16
FIELD_DELIM = ","

# S15  Bronze keeps every value as raw text; typing is a downstream (Silver) job
CHUNK_SIZE = 200_000             # S3
MAX_RETRIES = 3                  # S9  attempts per run
RETRY_BACKOFF_SECONDS = 2        # S9  linear backoff base
MAX_ATTEMPTS_LIFETIME = 5        # S20 attempts across all runs -> quarantine
REJECT_THRESHOLD = 0.05          # S20 abort the source if >5% of rows reject

DEDUP_KEYS: list[str] = []       # S16 e.g. ["user_session", "event_time", "product_id"]; [] disables

LOCK_TTL_SECONDS = 3600          # S19 a lease older than this is considered stale
REINGEST_MODE = "replace"        # S16 replace | append | reject (on changed fingerprint)

HOLDER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


# ============================================================================
# small helpers
# ============================================================================
def utcnow() -> datetime:
    """S24  every timestamp in this pipeline is UTC."""
    return datetime.now(timezone.utc)


def log(level: str, msg: str) -> None:
    """S11  one place for step logging."""
    print(f"[{level}] {utcnow().isoformat(timespec='seconds')}  {msg}", flush=True)


def sqlstr(value) -> str:
    """Inline a Python value as a safe SQL string literal (single-quote escaped)."""
    return "'" + str(value).replace("'", "''") + "'"


def posix(p: Path) -> str:
    """DuckDB wants forward-slash paths, even on Windows."""
    return str(p).replace("\\", "/")


def source_id_from_path(p: Path) -> str:
    """S12  stable identifier derived from the file name (extension stripped)."""
    return p.stem


def file_fingerprint(p: Path) -> dict:
    """S12  content hash + size + mtime -> a changed file reusing an old name
    is NOT silently skipped."""
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    st = p.stat()
    return {
        "content_hash": h.hexdigest(),
        "size_bytes": st.st_size,
        "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
    }


def wait_until_file_stable(p: Path, checks: int = 2, gap: float = 1.0) -> None:
    """S17  guard against a file that is still being written / growing."""
    last = None
    for _ in range(checks):
        st = p.stat()
        sig = (st.st_size, st.st_mtime)
        if last is not None and sig != last:
            raise SystemExit(f"[FATAL] source file is still changing: {p}")
        last = sig
        time.sleep(gap)


# ============================================================================
# S23  config validation
# ============================================================================
def validate_config() -> None:
    problems = []
    if not CSV_PATH.exists():
        problems.append(f"CSV_PATH does not exist: {CSV_PATH}")
    elif not os.access(CSV_PATH, os.R_OK):
        problems.append(f"CSV_PATH is not readable: {CSV_PATH}")
    if not EXPECTED_COLUMNS:
        problems.append("EXPECTED_COLUMNS is empty")
    if len(EXPECTED_COLUMNS) != len(set(EXPECTED_COLUMNS)):
        problems.append("EXPECTED_COLUMNS has duplicates")
    if CHUNK_SIZE <= 0:
        problems.append("CHUNK_SIZE must be > 0")
    if MAX_RETRIES < 1:
        problems.append("MAX_RETRIES must be >= 1")
    if not (0.0 <= REJECT_THRESHOLD <= 1.0):
        problems.append("REJECT_THRESHOLD must be in [0, 1]")
    if ENCODING not in {"utf-8", "latin-1", "utf-16"}:
        problems.append(f"ENCODING '{ENCODING}' not supported by DuckDB CSV reader")
    if REINGEST_MODE not in {"replace", "append", "reject"}:
        problems.append(f"REINGEST_MODE '{REINGEST_MODE}' invalid")
    if set(DEDUP_KEYS) - set(EXPECTED_COLUMNS):
        problems.append(f"DEDUP_KEYS not in EXPECTED_COLUMNS: {set(DEDUP_KEYS) - set(EXPECTED_COLUMNS)}")
    if problems:
        for p in problems:
            log("FATAL", p)
        raise SystemExit(2)
    log("INFO", "config validated")


# ============================================================================
# S1  control-plane schema (idempotent)
# ============================================================================
def ensure_control_tables(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_REGISTRY} (
            source_id      VARCHAR,
            content_hash   VARCHAR,
            file_path      VARCHAR,
            size_bytes     BIGINT,
            mtime          TIMESTAMP,
            header_json    VARCHAR,
            row_count      BIGINT,
            reject_count   BIGINT,
            status         VARCHAR,          -- discovered | complete | aborted_threshold
            first_seen_at  TIMESTAMP,
            last_ingest_at TIMESTAMP,
            PRIMARY KEY (source_id, content_hash)
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_STATE} (
            source_id     VARCHAR,
            content_hash  VARCHAR,
            chunk_id      INTEGER,
            status        VARCHAR,           -- in_progress | success | failed | quarantined
            row_count     INTEGER,
            attempts      INTEGER,
            offset_start  BIGINT,
            parquet_path  VARCHAR,
            attempted_at  TIMESTAMP,
            completed_at  TIMESTAMP,
            error_message VARCHAR,
            PRIMARY KEY (source_id, content_hash, chunk_id)
        )
    """)
    con.execute("CREATE SEQUENCE IF NOT EXISTS dlq_seq START 1")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_DLQ} (
            dlq_id        BIGINT DEFAULT nextval('dlq_seq'),
            source_id     VARCHAR,
            content_hash  VARCHAR,
            source_file   VARCHAR,
            line_no       BIGINT,
            raw_line      VARCHAR,
            error_type    VARCHAR,           -- extra_field | short_row | duplicate
            error_message VARCHAR,
            rejected_at   TIMESTAMP
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_LOCKS} (
            resource    VARCHAR PRIMARY KEY,
            holder      VARCHAR,
            acquired_at TIMESTAMP,
            expires_at  TIMESTAMP
        )
    """)


# ============================================================================
# S19  single-writer lease
# ============================================================================
def acquire_lock(con, resource: str) -> None:
    now = utcnow()
    con.execute("BEGIN")
    try:
        row = con.execute(
            f"SELECT holder, expires_at FROM {T_LOCKS} WHERE resource = ?", [resource]
        ).fetchone()
        if row and row[1] is not None and row[1] > now and row[0] != HOLDER_ID:
            con.execute("ROLLBACK")
            raise SystemExit(
                f"[FATAL] lock '{resource}' held by {row[0]} until {row[1].isoformat()}"
            )
        con.execute(
            f"""INSERT INTO {T_LOCKS} (resource, holder, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (resource) DO UPDATE SET
                    holder = excluded.holder,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at""",
            [resource, HOLDER_ID, now, now.fromtimestamp(now.timestamp() + LOCK_TTL_SECONDS, tz=timezone.utc)],
        )
        con.execute("COMMIT")
        log("INFO", f"lock acquired: {resource} (holder {HOLDER_ID})")
    except SystemExit:
        raise
    except Exception:
        con.execute("ROLLBACK")
        raise


def release_lock(con, resource: str) -> None:
    try:
        con.execute(f"DELETE FROM {T_LOCKS} WHERE resource = ? AND holder = ?",
                    [resource, HOLDER_ID])
        log("INFO", f"lock released: {resource}")
    except Exception as e:  # never mask the real error on the way out
        log("WARN", f"could not release lock {resource}: {e}")


# ============================================================================
# S13  header validation + drift
# ============================================================================
def read_actual_header(csv_posix: str) -> list[str]:
    tmp = duckdb.connect()
    try:
        raw = tmp.execute(
            f"SELECT col0 FROM read_csv({sqlstr(csv_posix)}, "
            f"columns={{'col0':'VARCHAR'}}, header=false, auto_detect=false, "
            f"delim=chr(1), quote='', escape='', encoding={sqlstr(ENCODING)}) LIMIT 1"
        ).fetchone()
    finally:
        tmp.close()
    if raw is None or raw[0] is None:
        return []
    return [h.strip() for h in raw[0].split(FIELD_DELIM)]


def validate_header(actual: list[str], previous_json: str | None) -> None:
    if not actual:
        return  # empty file -> handled as a no-op later
    missing = [c for c in EXPECTED_COLUMNS if c not in actual]
    extra = [c for c in actual if c not in EXPECTED_COLUMNS]
    if missing:
        raise SystemExit(f"[FATAL] S13 header missing columns: {missing}")
    if extra and not ALLOW_EXTRA_COLUMNS:
        raise SystemExit(f"[FATAL] S13 unexpected header columns: {extra}")
    if previous_json:
        prev = json.loads(previous_json)
        if prev != actual:
            msg = f"S13 header drift: was {prev} now {actual}"
            if STRICT_HEADER:
                raise SystemExit(f"[FATAL] {msg}")
            log("WARN", msg)


# ============================================================================
# S3/S4/S5/S15/S16  staging views + reject routing  (same method as reference:
# read RAW lines, split on the delimiter, judge width ourselves -- catches what
# DuckDB's own parser silently truncates)
#
# NOTE: naive split does not honour quoted fields containing the delimiter.
# The configured source has none; if that changes, switch the valid-row path to
# DuckDB's real CSV parser and keep this split only for malformed-row detection.
# ============================================================================
STAGING_VIEWS = ("_raw_lines", "_valid_lines", "_bad_lines", "_short_lines",
                 "_dup_lines", "_staging_all", "_staging_source")


def build_staging(con, csv_posix: str, actual_header: list[str],
                  source_id: str, content_hash: str) -> tuple[int, int]:
    width = len(actual_header)
    idx = {c: actual_header.index(c) + 1 for c in EXPECTED_COLUMNS}  # 1-based, BY NAME

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _raw_lines AS
        SELECT row_number() OVER () AS _line,
               col0 AS raw_line,
               string_split(col0, {sqlstr(FIELD_DELIM)}) AS fields
        FROM read_csv({sqlstr(csv_posix)}, columns={{'col0':'VARCHAR'}},
                      header=false, auto_detect=false, skip=1,
                      delim=chr(1), quote='', escape='', encoding={sqlstr(ENCODING)})
        WHERE trim(col0) <> ''
    """)

    trailing = f"trim(array_to_string(fields[{width + 1}:], '')) "
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _valid_lines AS
        SELECT _line, raw_line, fields FROM _raw_lines
        WHERE len(fields) = {width}
           OR (len(fields) > {width} AND {trailing} = '')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _bad_lines AS
        SELECT _line, raw_line FROM _raw_lines
        WHERE len(fields) > {width} AND {trailing} <> ''
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _short_lines AS
        SELECT _line, raw_line FROM _raw_lines WHERE len(fields) < {width}
    """)

    # S5  route malformed rows to the DLQ (set-based, scales to millions)
    meta = f"{sqlstr(source_id)}, {sqlstr(content_hash)}, {sqlstr(csv_posix)}"
    ts = sqlstr(utcnow())
    con.execute(f"""
        INSERT INTO {T_DLQ} (source_id, content_hash, source_file, line_no, raw_line, error_type, error_message, rejected_at)
        SELECT {meta}, _line, raw_line, 'extra_field',
               'row has extra non-empty field(s) beyond header width', {ts}
        FROM _bad_lines
    """)
    con.execute(f"""
        INSERT INTO {T_DLQ} (source_id, content_hash, source_file, line_no, raw_line, error_type, error_message, rejected_at)
        SELECT {meta}, _line, raw_line, 'short_row',
               'row has fewer fields than header width', {ts}
        FROM _short_lines
    """)

    col_exprs = ", ".join(f'fields[{idx[c]}] AS "{c}"' for c in EXPECTED_COLUMNS)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _staging_all AS
        SELECT row_number() OVER () AS _id, {col_exprs}
        FROM _valid_lines
    """)

    # S16  optional dedup; losers go to the DLQ
    if DEDUP_KEYS:
        keys = ", ".join(f'"{k}"' for k in DEDUP_KEYS)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW _dup_lines AS
            SELECT * FROM (
                SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY _id) AS _rn
                FROM _staging_all
            ) WHERE _rn > 1
        """)
        con.execute(f"""
            INSERT INTO {T_DLQ} (source_id, content_hash, source_file, line_no, raw_line, error_type, error_message, rejected_at)
            SELECT {meta}, _id, {sqlstr('')}, 'duplicate',
                   'duplicate on ' || {sqlstr(', '.join(DEDUP_KEYS))}, {ts}
            FROM _dup_lines
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW _staging_source AS
            SELECT * EXCLUDE (_rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY _id) AS _rn
                FROM _staging_all
            ) WHERE _rn = 1
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP VIEW _staging_source AS SELECT * FROM _staging_all")

    # S18  count streams through the view; the CSV is never fully materialised
    valid = con.execute("SELECT count(*) FROM _staging_source").fetchone()[0]
    rejected = con.execute(
        f"SELECT count(*) FROM {T_DLQ} WHERE source_id = ? AND content_hash = ?",
        [source_id, content_hash],
    ).fetchone()[0]
    log("INFO", f"staging ready: {valid} valid rows, {rejected} rejected")
    return valid, rejected


def drop_staging(con) -> None:
    """S10  intermediate objects removed so only the control tables remain."""
    for v in STAGING_VIEWS:
        con.execute(f"DROP VIEW IF EXISTS {v}")


# ============================================================================
# S6/S7/S9/S21  transactional per-chunk write  (Parquet part = the commit)
# ============================================================================
def partition_dir(source_id: str, ingest_date: str) -> Path:
    return RAW_DATASET_DIR / f"source={source_id}" / f"ingest_date={ingest_date}"


def write_chunk(con, source_id: str, content_hash: str, csv_posix_file: str,
                chunk_id: int, offset: int, expected_rows: int,
                ingest_date: str) -> int:
    now = utcnow()
    pdir = partition_dir(source_id, ingest_date)
    pdir.mkdir(parents=True, exist_ok=True)
    final_path = pdir / f"chunk_{chunk_id:05d}.parquet"
    tmp_path = pdir / f"._tmp_chunk_{chunk_id:05d}.parquet"

    con.execute("BEGIN")
    try:
        # S2/S6  mark in-progress (upsert); bump lifetime attempt counter
        con.execute(
            f"""INSERT INTO {T_STATE}
                (source_id, content_hash, chunk_id, status, row_count, attempts, offset_start, attempted_at)
                VALUES (?, ?, ?, 'in_progress', ?, 1, ?, ?)
                ON CONFLICT (source_id, content_hash, chunk_id) DO UPDATE SET
                    status = 'in_progress',
                    attempts = {T_STATE}.attempts + 1,
                    attempted_at = excluded.attempted_at""",
            [source_id, content_hash, chunk_id, expected_rows, offset, now],
        )

        # S7  enrichment columns are attached inside the COPY query
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _run_meta AS
            SELECT {sqlstr(source_id)}   AS source_id,
                   {sqlstr(csv_posix_file)} AS source_file,
                   {sqlstr(content_hash)} AS content_hash,
                   {sqlstr(ingest_date)}  AS ingest_date,
                   CAST({sqlstr(now)} AS TIMESTAMP) AS ingested_at
        """)

        # S6  write to a temp name first; os.replace() below is the atomic commit
        select_cols = ", ".join(f's."{c}"' for c in EXPECTED_COLUMNS)
        con.execute(f"""
            COPY (
                SELECT {select_cols},
                       CAST({chunk_id} AS INTEGER) AS chunk_id,
                       m.source_id, m.source_file, m.content_hash,
                       m.ingest_date, m.ingested_at
                FROM _staging_source s CROSS JOIN _run_meta m
                ORDER BY s._id
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            ) TO {sqlstr(posix(tmp_path))} (FORMAT 'parquet', COMPRESSION 'zstd')
        """)

        # S21  verify the part before promoting it
        written = con.execute(
            f"SELECT count(*) FROM read_parquet({sqlstr(posix(tmp_path))})"
        ).fetchone()[0]
        if written != expected_rows:
            raise RuntimeError(
                f"chunk {chunk_id}: wrote {written} rows, expected {expected_rows}"
            )

        os.replace(tmp_path, final_path)  # atomic on same filesystem

        con.execute(
            f"""UPDATE {T_STATE}
                SET status='success', row_count=?, parquet_path=?, completed_at=?, error_message=NULL
                WHERE source_id=? AND content_hash=? AND chunk_id=?""",
            [written, posix(final_path), utcnow(), source_id, content_hash, chunk_id],
        )
        con.execute("COMMIT")
        log("INFO", f"chunk {chunk_id}: committed {written} rows -> {final_path.name}")
        return written

    except Exception as e:
        con.execute("ROLLBACK")
        if tmp_path.exists():
            tmp_path.unlink()
        # S6  failure must stay visible even though the data write rolled back
        con.execute(
            f"""UPDATE {T_STATE}
                SET status='failed', error_message=?, completed_at=NULL
                WHERE source_id=? AND content_hash=? AND chunk_id=?""",
            [str(e), source_id, content_hash, chunk_id],
        )
        raise


def write_chunk_with_retry(con, source_id, content_hash, csv_posix_file,
                           chunk_id, offset, expected_rows, ingest_date) -> tuple[str, int]:
    """S9  capped retries with linear backoff; a permanent failure is logged and
    the run continues."""
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            n = write_chunk(con, source_id, content_hash, csv_posix_file,
                            chunk_id, offset, expected_rows, ingest_date)
            return "success", n
        except Exception as e:
            last = e
            log("WARN", f"chunk {chunk_id} attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    log("ERROR", f"chunk {chunk_id} permanently failed after {MAX_RETRIES} attempts: {last}")
    return "failed", 0


# ============================================================================
# S21  reconciliation: a 'success' row whose Parquet part vanished is reopened
# ============================================================================
def reconcile(con, source_id: str, content_hash: str) -> int:
    rows = con.execute(
        f"""SELECT chunk_id, parquet_path FROM {T_STATE}
            WHERE source_id=? AND content_hash=? AND status='success'""",
        [source_id, content_hash],
    ).fetchall()
    reopened = 0
    for chunk_id, path in rows:
        if not path or not Path(path).exists():
            con.execute(
                f"""UPDATE {T_STATE} SET status='failed',
                    error_message='parquet part missing at reconcile'
                    WHERE source_id=? AND content_hash=? AND chunk_id=?""",
                [source_id, content_hash, chunk_id],
            )
            reopened += 1
    if reopened:
        log("WARN", f"reconcile reopened {reopened} chunk(s) with missing parts")
    return reopened


# ============================================================================
# S16  fingerprint-driven re-ingest decision
# ============================================================================
def prepare_source(con, source_id: str, fp: dict, force: bool) -> str | None:
    """Returns the content_hash to process, or None if there is nothing to do."""
    ch = fp["content_hash"]
    same = con.execute(
        f"SELECT status FROM {T_REGISTRY} WHERE source_id=? AND content_hash=?",
        [source_id, ch],
    ).fetchone()
    if same and same[0] == "complete" and not force:
        log("INFO", f"fingerprint unchanged and already complete -> nothing to do ({ch[:12]})")
        return None

    other = con.execute(
        f"SELECT content_hash FROM {T_REGISTRY} WHERE source_id=? AND content_hash<>?",
        [source_id, ch],
    ).fetchall()
    if other or (same and force):
        if REINGEST_MODE == "reject":
            raise SystemExit(f"[FATAL] S16 source changed and REINGEST_MODE=reject ({ch[:12]})")
        if REINGEST_MODE == "replace":
            _purge_source(con, source_id)
            log("INFO", f"S16 replace: cleared prior parts/state for {source_id}")
        else:  # append
            log("WARN", "S16 append mode: prior rows kept; cross-version duplicates possible")

    con.execute(
        f"""INSERT INTO {T_REGISTRY}
            (source_id, content_hash, file_path, size_bytes, mtime, header_json,
             row_count, reject_count, status, first_seen_at, last_ingest_at)
            VALUES (?, ?, ?, ?, ?, NULL, 0, 0, 'discovered', ?, NULL)
            ON CONFLICT (source_id, content_hash) DO UPDATE SET
                status='discovered', mtime=excluded.mtime, size_bytes=excluded.size_bytes""",
        [source_id, ch, posix(CSV_PATH), fp["size_bytes"], fp["mtime"], utcnow()],
    )
    return ch


def _purge_source(con, source_id: str) -> None:
    con.execute(f"DELETE FROM {T_STATE} WHERE source_id=?", [source_id])
    con.execute(f"DELETE FROM {T_REGISTRY} WHERE source_id=?", [source_id])
    tree = RAW_DATASET_DIR / f"source={source_id}"
    if tree.exists():
        for p in sorted(tree.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        tree.rmdir()


def previous_header_json(con, source_id: str) -> str | None:
    row = con.execute(
        f"""SELECT header_json FROM {T_REGISTRY}
            WHERE source_id=? AND header_json IS NOT NULL
            ORDER BY last_ingest_at DESC NULLS LAST LIMIT 1""",
        [source_id],
    ).fetchone()
    return row[0] if row else None


# ============================================================================
# S22  maintenance
# ============================================================================
def prune(con, days: int) -> None:
    cutoff = utcnow().fromtimestamp(utcnow().timestamp() - days * 86400, tz=timezone.utc)
    n = con.execute(
        f"DELETE FROM {T_DLQ} WHERE rejected_at < ? RETURNING 1", [cutoff]
    ).fetchall()
    log("INFO", f"S22 pruned {len(n)} DLQ rows older than {days}d (state & parts kept)")


# ============================================================================
# S11  dataset-wide count for the run summary
# ============================================================================
def dataset_row_count(con) -> int:
    glob = posix(RAW_DATASET_DIR / "**" / "*.parquet")
    try:
        return con.execute(
            f"SELECT count(*) FROM read_parquet({sqlstr(glob)}, "
            f"hive_partitioning=true, union_by_name=true)"
        ).fetchone()[0]
    except duckdb.Error:
        return 0


# ============================================================================
# orchestration
# ============================================================================
def run(force: bool) -> dict:
    started = time.time()
    validate_config()                                   # S23

    source_id = source_id_from_path(CSV_PATH)
    csv_posix = posix(CSV_PATH)
    resource = f"ingest:{source_id}"

    RAW_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(CONTROL_DB))
    ensure_control_tables(con)                           # S1
    acquire_lock(con, resource)                          # S19

    summary = {"source_id": source_id}
    try:
        wait_until_file_stable(CSV_PATH)                 # S17
        fp = file_fingerprint(CSV_PATH)                  # S12
        summary["content_hash"] = fp["content_hash"]

        content_hash = prepare_source(con, source_id, fp, force)   # S16
        if content_hash is None:
            summary.update(status="noop", chunks_written=0)
            return summary

        actual_header = read_actual_header(csv_posix)              # S13
        validate_header(actual_header, previous_header_json(con, source_id))

        # S14  empty / header-only file -> valid no-op
        if not actual_header or con.execute(
            f"SELECT count(*) FROM read_csv({sqlstr(csv_posix)}, "
            f"columns={{'c':'VARCHAR'}}, header=false, auto_detect=false, skip=1, "
            f"delim=chr(1), quote='', escape='') WHERE trim(c) <> ''"
        ).fetchone()[0] == 0:
            con.execute(
                f"""UPDATE {T_REGISTRY} SET status='complete', row_count=0,
                    header_json=?, last_ingest_at=?
                    WHERE source_id=? AND content_hash=?""",
                [json.dumps(actual_header), utcnow(), source_id, content_hash],
            )
            summary.update(status="empty", chunks_written=0, total_valid_rows=0)
            return summary

        valid_rows, reject_rows = build_staging(             # S3/S4/S5/S15/S16
            con, csv_posix, actual_header, source_id, content_hash)

        # S20  reject-rate threshold -> abort before writing anything
        total_seen = valid_rows + reject_rows
        if total_seen and reject_rows / total_seen > REJECT_THRESHOLD:
            con.execute(
                f"""UPDATE {T_REGISTRY} SET status='aborted_threshold',
                    reject_count=?, header_json=?, last_ingest_at=?
                    WHERE source_id=? AND content_hash=?""",
                [reject_rows, json.dumps(actual_header), utcnow(), source_id, content_hash],
            )
            drop_staging(con)
            raise SystemExit(
                f"[FATAL] S20 reject rate {reject_rows}/{total_seen} "
                f"> {REJECT_THRESHOLD:.0%}; source aborted"
            )

        num_chunks = -(-valid_rows // CHUNK_SIZE)            # ceil
        ingest_date = date.today().isoformat() if False else utcnow().date().isoformat()  # S24

        # S2  resume: which chunks are already done / permanently parked
        state_rows = {
            r[0]: (r[1], r[2]) for r in con.execute(
                f"""SELECT chunk_id, status, attempts FROM {T_STATE}
                    WHERE source_id=? AND content_hash=?""",
                [source_id, content_hash],
            ).fetchall()
        }

        written = skipped = quarantined = failed = 0
        new_rows = 0
        for chunk_id in range(num_chunks):
            offset = chunk_id * CHUNK_SIZE
            expected = min(CHUNK_SIZE, valid_rows - offset)
            st = state_rows.get(chunk_id)
            if st and st[0] == "success":
                skipped += 1
                continue
            if st and st[0] == "quarantined":
                quarantined += 1
                continue
            if st and st[1] and st[1] >= MAX_ATTEMPTS_LIFETIME:     # S20 poison chunk
                con.execute(
                    f"""UPDATE {T_STATE} SET status='quarantined'
                        WHERE source_id=? AND content_hash=? AND chunk_id=?""",
                    [source_id, content_hash, chunk_id],
                )
                log("ERROR", f"chunk {chunk_id} quarantined after {st[1]} lifetime attempts")
                quarantined += 1
                continue

            outcome, n = write_chunk_with_retry(               # S6/S7/S9/S21
                con, source_id, content_hash, csv_posix,
                chunk_id, offset, expected, ingest_date)
            if outcome == "success":
                written += 1
                new_rows += n
            else:
                failed += 1

        reconcile(con, source_id, content_hash)               # S21

        reg_status = "complete" if failed == 0 and quarantined == 0 else "partial"
        con.execute(
            f"""UPDATE {T_REGISTRY}
                SET status=?, row_count=?, reject_count=?, header_json=?, last_ingest_at=?
                WHERE source_id=? AND content_hash=?""",
            [reg_status, valid_rows, reject_rows, json.dumps(actual_header),
             utcnow(), source_id, content_hash],
        )

        drop_staging(con)                                     # S10
        _sweep_tmp_parts()                                    # S10

        summary.update(                                        # S11
            status=reg_status,
            total_valid_rows=valid_rows,
            total_rejected=reject_rows,
            chunks_total=num_chunks,
            chunks_written_this_run=written,
            chunks_skipped=skipped,
            chunks_quarantined=quarantined,
            chunks_failed=failed,
            new_rows_this_run=new_rows,
            dataset_total_rows=dataset_row_count(con),
            elapsed_seconds=round(time.time() - started, 1),
        )
        return summary

    finally:
        _sweep_tmp_parts()
        release_lock(con, resource)
        con.close()


def _sweep_tmp_parts() -> None:
    if RAW_DATASET_DIR.exists():
        for p in RAW_DATASET_DIR.rglob("._tmp_chunk_*.parquet"):
            try:
                p.unlink()
            except OSError:
                pass


# ============================================================================
# S25  built-in fixture tests
# ============================================================================
def selftest() -> int:
    import tempfile, textwrap
    global CSV_PATH, RAW_DATASET_DIR, CONTROL_DB, REJECT_THRESHOLD

    good = textwrap.dedent("""\
        event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
        2019-11-01 00:00:00 UTC,view,1,2,electronics.smartphone,xiaomi,489.07,520088904,s1
        2019-11-01 00:00:01 UTC,view,2,3,,creed,28.31,561587266,s2
        2019-11-01 00:00:02 UTC,cart,3,4,appliances.kitchen.washer,lg,712.87,518085591,s3
        2019-11-01 00:00:03 UTC,view,4,5,,xiaomi,183.27,558856683,s4,GARBAGE_EXTRA
        2019-11-01 00:00:04 UTC,view,5
    """)
    cases = []
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        csv = root / "fixture.csv"
        csv.write_text(good, encoding="utf-8")
        CSV_PATH = csv
        RAW_DATASET_DIR = root / "raw-data"
        CONTROL_DB = root / "pipeline.duckdb"
        REJECT_THRESHOLD = 0.9  # keep the 2 bad rows from aborting the run

        s1 = run(force=False)
        cases.append(("valid rows == 3", s1.get("total_valid_rows") == 3, s1))
        cases.append(("rejected == 2", s1.get("total_rejected") == 2, s1))
        cases.append(("dataset rows == 3", s1.get("dataset_total_rows") == 3, s1))

        s2 = run(force=False)  # S2 idempotent re-run
        cases.append(("2nd run noop", s2.get("status") == "noop", s2))

        csv.write_text("event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n",
                       encoding="utf-8")
        s3 = run(force=False)
        cases.append(("empty file -> empty", s3.get("status") == "empty", s3))

    ok = True
    for name, passed, ctx in cases:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        if not passed:
            ok = False
            print(f"        ctx={ctx}")
    return 0 if ok else 1


# ============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="CSV -> Bronze Parquet ingestion (DuckDB)")
    ap.add_argument("--csv", type=Path, help="override CSV_PATH")
    ap.add_argument("--force", action="store_true", help="re-ingest even if fingerprint unchanged")
    ap.add_argument("--reingest-mode", choices=["replace", "append", "reject"])
    ap.add_argument("--prune-days", type=int, help="trim DLQ rows older than N days, then exit")
    ap.add_argument("--selftest", action="store_true", help="run fixture tests, then exit")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    global CSV_PATH, REINGEST_MODE
    if args.csv:
        CSV_PATH = args.csv
    if args.reingest_mode:
        REINGEST_MODE = args.reingest_mode

    if args.prune_days is not None:
        con = duckdb.connect(str(CONTROL_DB))
        ensure_control_tables(con)
        prune(con, args.prune_days)
        con.close()
        return 0

    summary = run(force=args.force)
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary.get("status") in {"complete", "noop", "empty"} else 1


if __name__ == "__main__":
    sys.exit(main())