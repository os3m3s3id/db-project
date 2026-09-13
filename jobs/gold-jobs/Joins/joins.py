import psycopg2
import os
from dotenv import load_dotenv
load_dotenv(override=True)


# #### Database connection details:
try:
    pg_host = os.environ["POSTGRES_HOST"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]
    print(f"Connecting to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except KeyError as e:
    print(f"Environment variable not set: {e}")


# #### We have to create the primary keys for the dims tables an indexes for the cols in  fact and them joining to reduce the Seq scanning and takes take more time

pg_conn = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_password,
    host=pg_host,
    port=pg_port
)
cur = pg_conn.cursor()

try:
    cur.execute("ALTER TABLE data_warehouse.users_dim ADD PRIMARY KEY (user_key);")
    pg_conn.commit()

    cur.execute("ALTER TABLE data_warehouse.products_dim ADD PRIMARY KEY (product_key);")
    pg_conn.commit()

    cur.execute("""
        ALTER TABLE data_warehouse.sales_transactions_fact
            ADD CONSTRAINT fk_user FOREIGN KEY (user_key)
            REFERENCES data_warehouse.users_dim (user_key);
    """)
    pg_conn.commit()

    cur.execute("""
        ALTER TABLE data_warehouse.sales_transactions_fact
            ADD CONSTRAINT fk_product FOREIGN KEY (product_key)
            REFERENCES data_warehouse.products_dim (product_key);
    """)
    pg_conn.commit()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_user_key ON data_warehouse.sales_transactions_fact (user_key);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_product_key ON data_warehouse.sales_transactions_fact (product_key);")
    pg_conn.commit()

    cur.execute("""
        SELECT
            u.user_id,
            p.product_id,
            p.brand,
            f.price,
            f.event_time
        FROM data_warehouse.sales_transactions_fact f
        INNER JOIN data_warehouse.users_dim u ON f.user_key = u.user_key
        INNER JOIN data_warehouse.products_dim p ON f.product_key = p.product_key
        LIMIT 10;
    """)
    result = cur.fetchall()
    print(result)

    print("Done.")

except Exception as e:
    pg_conn.rollback()
    print(f"Error: {e}")

