import duckdb
import os
from dotenv import load_dotenv

# override=True ensures freshly-edited .env values are picked up even if
# this kernel already loaded an older version of the environment.
load_dotenv(override=True)

pg_host = os.environ["POSTGRES_HOST"]
pg_port = os.environ["POSTGRES_PORT"]
pg_db = os.environ["POSTGRES_DB"]
pg_user = os.environ["POSTGRES_USER"]
pg_password = os.environ["POSTGRES_PASSWORD"]

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(f"""
    ATTACH 'dbname={pg_db} user={pg_user} host={pg_host} password={pg_password} port={pg_port}'
    AS pg (TYPE postgres);
""")

con.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

con.execute("CREATE SCHEMA IF NOT EXISTS pg.meta;")

con.execute("""
    CREATE TABLE IF NOT EXISTS pg.silver.silver_table (
        event_time      TIMESTAMP,
        event_type      TEXT,
        product_id      TEXT,
        category_id     TEXT,
        category_code   TEXT,
        brand           TEXT,
        price           NUMERIC,
        user_id         TEXT,
        user_session    TEXT,
        source_file     TEXT,
        transformed_at  TIMESTAMP DEFAULT now()
    );
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS pg.meta.silver_processed_files (
        file_name        TEXT PRIMARY KEY,
        status            TEXT NOT NULL,
        row_count_total   INTEGER,
        row_count_silver  INTEGER,
        row_count_dlq     INTEGER,
        processed_at      TIMESTAMP DEFAULT now()
    );
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS pg.silver.dlq_events (
        raw_data          JSON,
        rejection_reason  TEXT,
        source_file       TEXT,
        quarantined_at    TIMESTAMP DEFAULT now(),
        reprocessed       BOOLEAN DEFAULT false
    );
""")

bronze_path = os.environ["BRONZE_PATH"]
BATCH_SIZE = 20  # our agreed cap on files processed per run

pending_files = con.execute(f"""
    SELECT DISTINCT filename
    FROM read_parquet('{bronze_path}', filename=True)
    WHERE filename NOT IN (
        SELECT file_name FROM pg.meta.silver_processed_files WHERE status = 'success'
    )
    LIMIT {BATCH_SIZE}
""").fetchall()

pending_files = [row[0] for row in pending_files]
print(f"{len(pending_files)} file(s) pending this run:")
print(pending_files)

run_summary = []  # collects a per-file summary to print at the end

for current_file in pending_files:
    print(f"Processing: {current_file}")

    try:
        # Start a transaction: the silver insert, DLQ insert, and manifest
        # update below either all commit together, or none do.
        con.execute("BEGIN TRANSACTION;")

        # --- Steps 1-6: select, flag missing IDs, trim, lowercase, cast types ---
        # --- Step 7: dedup on (user_id, product_id, event_type, event_time)  ---
        # --- Step 8a: clean rows -> silver_table                              ---
        con.execute(f"""
            INSERT INTO pg.silver.silver_table
            (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, source_file, transformed_at)
            WITH cleaned AS (
                SELECT
                    try_cast(event_time AS TIMESTAMP)  AS event_time,
                    lower(trim(event_type))     AS event_type,
                    lower(trim(product_id))     AS product_id,
                    lower(trim(category_id))    AS category_id,
                    lower(trim(category_code))  AS category_code,
                    lower(trim(brand))          AS brand,
                    try_cast(price AS DOUBLE)    AS price,
                    lower(trim(user_id))         AS user_id,
                    lower(trim(user_session))    AS user_session,
                    filename AS source_file,
                    now() AS transformed_at,
                    CASE
                        WHEN trim(user_id) IS NULL OR trim(user_id) = '' THEN 'missing_identifier'
                        WHEN try_cast(event_time AS TIMESTAMP) IS NULL THEN 'type_cast_failure'
                        WHEN try_cast(price AS DOUBLE) IS NULL THEN 'type_cast_failure'
                        ELSE NULL
                    END AS rejection_reason
                FROM read_parquet('{current_file}', filename=True)
            ),
            deduped AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, product_id, event_type, event_time
                        ORDER BY transformed_at DESC
                    ) AS rn
                FROM cleaned
            )
            SELECT event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, source_file, transformed_at
            FROM deduped
            WHERE rn = 1 AND rejection_reason IS NULL
        """)

        # --- Step 8b: bad rows -> dlq_events, original values kept as JSON ---
        con.execute(f"""
            INSERT INTO pg.silver.dlq_events
            (raw_data, rejection_reason, source_file, quarantined_at)
            WITH cleaned AS (
                SELECT
                    event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session,
                    filename AS source_file,
                    now() AS transformed_at,
                    CASE
                        WHEN trim(user_id) IS NULL OR trim(user_id) = '' THEN 'missing_identifier'
                        WHEN try_cast(event_time AS TIMESTAMP) IS NULL THEN 'type_cast_failure'
                        WHEN try_cast(price AS DOUBLE) IS NULL THEN 'type_cast_failure'
                        ELSE NULL
                    END AS rejection_reason
                FROM read_parquet('{current_file}', filename=True)
            ),
            deduped AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, product_id, event_type, event_time
                        ORDER BY transformed_at DESC
                    ) AS rn
                FROM cleaned
            )
            SELECT
                to_json(struct_pack(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)) AS raw_data,
                rejection_reason,
                source_file,
                now() AS quarantined_at
            FROM deduped
            WHERE rn = 1 AND rejection_reason IS NOT NULL
        """)

        # --- Row counts for this file, used for the manifest entry ---
        total_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{current_file}', filename=True)"
        ).fetchone()[0]
        silver_count = con.execute(
            "SELECT count(*) FROM pg.silver.silver_table WHERE source_file = ?;", [current_file]
        ).fetchone()[0]
        dlq_count = con.execute(
            "SELECT count(*) FROM pg.silver.dlq_events WHERE source_file = ?;", [current_file]
        ).fetchone()[0]

        # --- Manifest update: only reached if both inserts above succeeded ---
        con.execute("""
            INSERT INTO pg.meta.silver_processed_files
            (file_name, status, row_count_total, row_count_silver, row_count_dlq, processed_at)
            VALUES (?, 'success', ?, ?, ?, now())
            ON CONFLICT (file_name) DO UPDATE SET
                status = 'success',
                row_count_total = excluded.row_count_total,
                row_count_silver = excluded.row_count_silver,
                row_count_dlq = excluded.row_count_dlq,
                processed_at = excluded.processed_at;
        """, [current_file, total_count, silver_count, dlq_count])

        con.execute("COMMIT;")

        run_summary.append({
            "file": current_file,
            "status": "success",
            "total": total_count,
            "silver": silver_count,
            "dlq": dlq_count,
        })
        print(f"  -> success | total={total_count} silver={silver_count} dlq={dlq_count}")

    except Exception as e:
        # Undo every change made for this file - silver, DLQ, and manifest
        # inserts are all rolled back together, so nothing is half-applied.
        con.execute("ROLLBACK;")
        run_summary.append({"file": current_file, "status": "failed", "error": str(e)})
        print(f"  -> FAILED: {e}")
        # Note: this file is NOT marked in the manifest, so the next run
        # will automatically retry it.

print("\n=== Run Summary ===")
for entry in run_summary:
    print(entry)

succeeded = sum(1 for e in run_summary if e["status"] == "success")
failed = sum(1 for e in run_summary if e["status"] == "failed")
total_silver = sum(e.get("silver", 0) for e in run_summary if e["status"] == "success")
total_dlq = sum(e.get("dlq", 0) for e in run_summary if e["status"] == "success")

print(f"\nFiles succeeded: {succeeded} | Files failed: {failed}")
print(f"Total rows -> silver: {total_silver} | Total rows -> DLQ: {total_dlq}")

con.execute("SELECT * FROM pg.meta.silver_processed_files ORDER BY processed_at;").fetchall()

con.execute("SELECT count(*) FROM pg.silver.silver_table;").fetchall()

con.execute("SELECT count(*) FROM pg.silver.dlq_events;").fetchall()
