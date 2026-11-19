"""Confirm that the .env Snowflake settings work.

Run from the project root:
    python -m scripts.check_snowflake
"""
import snowflake.connector

from config.settings import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)


def main():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW",
         enable_server_session_keep_alive_auto_detection=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
            "CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        user, role, warehouse, database, schema = cursor.fetchone()
        print(f"Connected as {user} | role={role} | warehouse={warehouse}")
        print(f"Database.schema = {database}.{schema}")

        cursor.execute("SELECT COUNT(*) FROM RAW.TRANSACTIONS")
        print("Rows in RAW.TRANSACTIONS:", cursor.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
