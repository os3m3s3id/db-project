#!/usr/bin/env python
# coding: utf-8

# # Procedures

# #### Imports:

# In[29]:


import os
from dotenv import load_dotenv
load_dotenv(override=True)
import psycopg2


# #### Database connection details:

# In[30]:


try:
    pg_host = os.environ["POSTGRES_HOST"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]
    print(f"Connecting to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except KeyError as e:
    print(f"Environment variable not set: {e}")


# #### Create database connection:

# In[31]:


try:
    pg_conn = psycopg2.connect(
    dbname=pg_db, user=pg_user, password=pg_password, host=pg_host, port=pg_port
    )
    cur = pg_conn.cursor()
    print(f"Successfully connected to database {pg_db} at {pg_host}:{pg_port} as user {pg_user}")
except Exception as e:
    print(f"Error creating database engine: {e}")


# #### Procedures creation to merg the data from the silver layer to the dims and fact:

# In[ ]:


try:
    cur.execute("CREATE SCHEMA IF NOT EXISTS data_warehouse;")
    print("Schema data_warehouse created successfully.")
    
    cur.execute("""
        CREATE OR REPLACE PROCEDURE data_warehouse.load_products_dim_merge()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            MERGE INTO data_warehouse.products_dim AS target
            USING (
                SELECT
                    product_id, category_id, category_code, brand,
                    (SELECT COALESCE(MAX(product_key), 0) FROM data_warehouse.products_dim)
                        + ROW_NUMBER() OVER (ORDER BY product_id) AS new_product_key
                FROM (
                    SELECT
                        st.product_id, st.category_id, st.category_code, st.brand,
                        ROW_NUMBER() OVER (
                            PARTITION BY st.product_id
                            ORDER BY st.event_time DESC
                        ) AS rn
                    FROM silver.silver_table st
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

            COMMIT;
        END;
        $$;
    """)

    pg_conn.commit()
    print("Procedure load_products_dim_merge() created successfully.")

except Exception as e:
    pg_conn.rollback()
    print(f"Procedure creation failed: {e}")



#### Procedures creation to merge the user dim table:
try:

    cur.execute("""
        CREATE OR REPLACE PROCEDURE data_warehouse.load_users_dim_merge()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            MERGE INTO data_warehouse.users_dim AS target
            USING (
                SELECT
                    user_id,
                    (SELECT COALESCE(MAX(user_key), 0) FROM data_warehouse.users_dim)
                        + ROW_NUMBER() OVER (ORDER BY user_id) AS new_user_key
                FROM (SELECT DISTINCT st.user_id FROM silver.silver_table st WHERE st.user_id IS NOT NULL) u
            ) AS source
            ON target.user_id = source.user_id
            WHEN NOT MATCHED THEN
                INSERT (user_key, user_id)
                VALUES (source.new_user_key, source.user_id);

            COMMIT;
        END;
        $$;
    """)

    pg_conn.commit()
    print("Procedure load_users_dim_merge() created successfully.")

except Exception as e:
    pg_conn.rollback()
    print(f"Procedure creation failed: {e}")



#### Procedures creation to merge the sales_fact table:
try:
    cur.execute("""
        CREATE OR REPLACE PROCEDURE data_warehouse.load_sales_fact_merge()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            MERGE INTO data_warehouse.sales_transactions_fact AS target
            USING (
                SELECT
                    event_time, event_type, price, user_session, user_key, product_key,
                    (SELECT COALESCE(MAX(sales_trans_key), 0) FROM data_warehouse.sales_transactions_fact)
                        + ROW_NUMBER() OVER (ORDER BY user_session, event_time) AS new_sales_trans_key
                FROM (
                    SELECT
                        st.event_time, st.event_type, st.price, st.user_session,
                        us.user_key, pr.product_key
                    FROM silver.silver_table st
                    LEFT JOIN data_warehouse.users_dim us ON st.user_id = us.user_id
                    LEFT JOIN data_warehouse.products_dim pr ON st.product_id = pr.product_id
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

            COMMIT;
        END;
        $$;
    """)

    pg_conn.commit()
    print("Procedure load_sales_fact_merge() created successfully.")

except Exception as e:
    pg_conn.rollback()
    print(f"Procedure creation failed: {e}")


cur.close()
pg_conn.close()


# In[ ]:




