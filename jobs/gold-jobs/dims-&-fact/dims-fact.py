# Dims & Fact Tables Creation
# In this document im going to create the dims and fact tables from what i have in the silver layer (The big table).

#### Imports:
import duckdb
import os
from dotenv import load_dotenv
load_dotenv(override=True)


#### Database connection details:
try:
    pg_host = os.environ["POSTGRES_HOST"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]
    print(f"Connecting to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except KeyError as e:
    print(f"Environment variable not set: {e}")


#### Create database connection:
try:
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        ATTACH 'dbname={pg_db} user={pg_user} host={pg_host} password={pg_password} port={pg_port}'
        AS pg (TYPE postgres);
    """)
except Exception as e:
    print(f"Error creating database engine: {e}")


try:
    con.execute("BEGIN TRANSACTION;")

    # 1. Schema
    con.execute("CREATE SCHEMA IF NOT EXISTS pg.data_warehouse;")

    # 2. Table structures — created once, columns defined upfront so this
    #    stays safe to re-run (no ALTER TABLE needed later).
    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.data_warehouse.products_dim (
            product_key    INTEGER,
            product_id     TEXT UNIQUE,
            category_id    TEXT,
            category_code  TEXT,
            brand          TEXT
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.data_warehouse.users_dim (
            user_key  INTEGER,
            user_id   TEXT UNIQUE
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.data_warehouse.sales_transactions_fact (
            sales_trans_key  INTEGER,
            event_time       TIMESTAMP,
            event_type       TEXT,
            price            NUMERIC,
            user_session     TEXT,
            user_key         INTEGER,
            product_key      INTEGER
        );
    """)


    # 2a. Indexes — speed up lookups/joins below.
    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_products_dim_product_id
            ON pg.data_warehouse.products_dim (product_id);
    """)

    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_dim_user_id
            ON pg.data_warehouse.users_dim (user_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_fact_dedup
            ON pg.data_warehouse.sales_transactions_fact (user_session, event_time, product_key);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_product_id
            ON pg.silver.silver_table (product_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_user_id
            ON pg.silver.silver_table (user_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_silver_event_type
            ON pg.silver.silver_table (event_type);
    """)


    con.execute("""
        MERGE INTO pg.data_warehouse.products_dim AS target
        USING (
            SELECT
                product_id, category_id, category_code, brand,
                (SELECT COALESCE(MAX(product_key), 0) FROM pg.data_warehouse.products_dim)
                    + ROW_NUMBER() OVER (ORDER BY product_id) AS new_product_key
            FROM (
                SELECT
                    st.product_id, st.category_id, st.category_code, st.brand,
                    ROW_NUMBER() OVER (
                        PARTITION BY st.product_id
                        ORDER BY st.event_time DESC
                    ) AS rn
                FROM pg.silver.silver_table st
                WHERE st.product_id IS NOT NULL
            ) ranked
            WHERE rn = 1
        ) AS source
        ON target.product_id = source.product_id
        WHEN MATCHED AND (
                target.category_id   IS DISTINCT FROM source.category_id
             OR target.category_code IS DISTINCT FROM source.category_code
             OR target.brand         IS DISTINCT FROM source.brand
        ) THEN
            UPDATE SET
                category_id   = source.category_id,
                category_code = source.category_code,
                brand         = source.brand
        WHEN NOT MATCHED THEN
            INSERT (product_key, product_id, category_id, category_code, brand)
            VALUES (
                source.new_product_key, source.product_id,
                source.category_id, source.category_code, source.brand
            );
    """)



    con.execute("""
        MERGE INTO pg.data_warehouse.users_dim AS target
        USING (
            SELECT
                user_id,
                (SELECT COALESCE(MAX(user_key), 0) FROM pg.data_warehouse.users_dim)
                    + ROW_NUMBER() OVER (ORDER BY user_id) AS new_user_key
            FROM (SELECT DISTINCT st.user_id FROM pg.silver.silver_table st WHERE st.user_id IS NOT NULL) u
        ) AS source
        ON target.user_id = source.user_id
        WHEN NOT MATCHED THEN
            INSERT (user_key, user_id)
            VALUES (source.new_user_key, source.user_id);
    """)



    con.execute("""
        MERGE INTO pg.data_warehouse.sales_transactions_fact AS target
        USING (
            SELECT
                event_time, event_type, price, user_session, user_key, product_key,
                (SELECT COALESCE(MAX(sales_trans_key), 0) FROM pg.data_warehouse.sales_transactions_fact)
                    + ROW_NUMBER() OVER (ORDER BY user_session, event_time) AS new_sales_trans_key
            FROM (
                SELECT
                    st.event_time, st.event_type, st.price, st.user_session,
                    us.user_key, pr.product_key
                FROM pg.silver.silver_table st
                LEFT JOIN pg.data_warehouse.users_dim us ON st.user_id = us.user_id
                LEFT JOIN pg.data_warehouse.products_dim pr ON st.product_id = pr.product_id
                WHERE st.event_type = 'purchase'
            ) candidates
        ) AS source
        ON target.user_session = source.user_session
           AND target.event_time = source.event_time
           AND target.product_key = source.product_key
        WHEN NOT MATCHED THEN
            INSERT (sales_trans_key, event_time, event_type, price, user_session, user_key, product_key)
            VALUES (
                source.new_sales_trans_key, source.event_time, source.event_type,
                source.price, source.user_session, source.user_key, source.product_key
            );
    """)

    con.execute("COMMIT;")
    print("Data warehouse load completed successfully.")

except Exception as error:
    con.execute("ROLLBACK;")
    print("Data warehouse load failed:")
    print(error)