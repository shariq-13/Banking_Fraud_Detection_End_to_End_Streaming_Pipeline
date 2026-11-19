"""Load Silver Parquet files from S3 into Snowflake RAW.TRANSACTIONS.

Each file is:
  1. read from S3 and cleaned,
  2. written to RAW.TRANSACTIONS_STAGING,
  3. merged into RAW.TRANSACTIONS on TRANSACTION_ID (no duplicates),
  4. recorded in RAW.LOADED_FILES so it is never loaded twice.

Run from the project root:
    python -m loader.load_s3_to_snowflake
"""
import io

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)

SILVER_PREFIX = "silver/transactions/"
STAGING_TABLE = "TRANSACTIONS_STAGING"

STAGING_COLUMNS = [
    "TRANSACTION_ID", "CUSTOMER_ID", "ACCOUNT_ID", "AMOUNT", "CURRENCY",
    "MERCHANT_CATEGORY", "COUNTRY", "TRANSACTION_TYPE", "TRANSACTION_TIMESTAMP",
    "RISK_SCORE", "RISK_LEVEL", "IS_FRAUD_FLAG", "RULE_REASONS",
]

MERGE_SQL = """
MERGE INTO RAW.TRANSACTIONS AS target
USING RAW.TRANSACTIONS_STAGING AS source
    ON target.TRANSACTION_ID = source.TRANSACTION_ID
WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID, CUSTOMER_ID, ACCOUNT_ID, AMOUNT, CURRENCY,
    MERCHANT_CATEGORY, COUNTRY, TRANSACTION_TYPE, TRANSACTION_TIMESTAMP,
    RISK_SCORE, RISK_LEVEL, IS_FRAUD_FLAG, RULE_REASONS
) VALUES (
    source.TRANSACTION_ID, source.CUSTOMER_ID, source.ACCOUNT_ID, source.AMOUNT,
    source.CURRENCY, source.MERCHANT_CATEGORY, source.COUNTRY,
    source.TRANSACTION_TYPE, source.TRANSACTION_TIMESTAMP, source.RISK_SCORE,
    source.RISK_LEVEL, source.IS_FRAUD_FLAG, source.RULE_REASONS
)
"""


def connect():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW",
    )


def list_silver_keys(s3):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SILVER_PREFIX):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".parquet"):
                keys.append(item["Key"])
    return sorted(keys)


def already_loaded(cursor):
    cursor.execute("SELECT S3_KEY FROM RAW.LOADED_FILES")
    return {row[0] for row in cursor.fetchall()}


def join_reasons(value):
    """Silver stores rule_reasons as a list. The warehouse stores it as text."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return ",".join(value)


def read_and_clean(s3, key):
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    frame = pd.read_parquet(io.BytesIO(body))

    frame = frame.drop_duplicates(subset=["transaction_id"]).copy()
    frame["rule_reasons"] = frame["rule_reasons"].apply(join_reasons)
    # Store timestamps as naive UTC. Every timestamp in this project is UTC.
    frame["transaction_timestamp"] = (
        pd.to_datetime(frame["transaction_timestamp"], utc=True).dt.tz_convert(None)
    )
    frame["is_fraud_flag"] = frame["is_fraud_flag"].astype(bool)
    frame.columns = [c.upper() for c in frame.columns]
    return frame[STAGING_COLUMNS]


def load_file(conn, s3, key):
    frame = read_and_clean(s3, key)
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE RAW.{STAGING_TABLE}")
        success, _chunks, _rows, _output = write_pandas(
            conn,
            frame,
            STAGING_TABLE,
            database=SNOWFLAKE_DATABASE,
            schema="RAW",
            quote_identifiers=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed for {key}")

        cursor.execute(MERGE_SQL)
        cursor.execute(
            "INSERT INTO RAW.LOADED_FILES (S3_KEY, ROW_COUNT) VALUES (%s, %s)",
            (key, len(frame)),
        )
    finally:
        cursor.close()
    print(f"Loaded {len(frame)} rows from {key}")


def main():
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET is not set. Check your .env file.")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    keys = list_silver_keys(s3)
    if not keys:
        print("No Silver files found in S3. Run the consumer first.")
        return

    with connect() as conn:
        cursor = conn.cursor()
        loaded = already_loaded(cursor)
        cursor.close()

        pending = [k for k in keys if k not in loaded]
        print(f"{len(keys)} Silver files found, {len(pending)} new to load")
        for key in pending:
            load_file(conn, s3, key)
    print("Load finished")


if __name__ == "__main__":
    main()
