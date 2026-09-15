import psycopg2
import os
from dotenv import load_dotenv
load_dotenv(override=True)

try:
    pg_host = os.environ["POSTGRES_HOST"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]
    print(f"Connecting to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except KeyError as e:
    print(f"Environment variable not set: {e}")



try:
    pg_conn = psycopg2.connect(
    dbname=pg_db, user=pg_user, password=pg_password, host=pg_host, port=pg_port
    )
    cur = pg_conn.cursor()
    print(f"Successfully connected to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except Exception as e:
    print(f"Error creating database engine: {e}")



try:
    cur.execute("BEGIN TRANSACTION;")

    # 1. Schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS data_warehouse;")

    # 2. Table structures — created once, columns defined upfront so this
    #    stays safe to re-run (no ALTER TABLE needed later).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_warehouse.products_dim (
            product_key    INTEGER,
            product_id     TEXT UNIQUE,
            category_id    TEXT,
            category_code  TEXT,
            brand          TEXT
        );
    """)
    print("Products dimension table created successfully.")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_warehouse.users_dim (
            user_key  INTEGER,  
            user_id   TEXT UNIQUE
        );
    """)
    print("Users dimension table created successfully.")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_warehouse.sales_transactions_fact (
            sales_trans_key  INTEGER,
            event_time       TIMESTAMP,
            event_type       TEXT,
            price            NUMERIC,
            user_session     TEXT,
            user_key         INTEGER,
            product_key      INTEGER
        );
    """)
    print("Sales transactions fact table created successfully.")


    # 2b. Indexes — speed up the NOT EXISTS lookups below.
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_products_dim_product_id
            ON data_warehouse.products_dim (product_id);
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_dim_user_id
            ON data_warehouse.users_dim (user_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_dedup
            ON data_warehouse.sales_transactions_fact (user_session, event_time, product_key);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_product_id
            ON silver.silver_table (product_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_user_id
            ON silver.silver_table (user_id);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_event_type
            ON silver.silver_table (event_type);
    """)

    pg_conn.commit()
    pg_conn.autocommit = True

    # 3. Procedures to load the dimensions and fact table.
    cur.execute("CALL data_warehouse.load_products_dim_merge();")
    cur.execute("CALL data_warehouse.load_users_dim_merge();")
    cur.execute("CALL data_warehouse.load_sales_fact_merge();")
    pg_conn.commit()
    print("Data warehouse load completed successfully.")
except Exception as e:
    pg_conn.rollback()
    print(f"Data warehouse load failed: {e}")

cur.close()
pg_conn.close()