"""Write Bronze (raw JSONL) and Silver (enriched Parquet) batches to S3."""
import io
import json
from datetime import datetime, timezone

import boto3
import pandas as pd

from config.settings import AWS_REGION, S3_BUCKET


def _partition(layer, now):
    return f"{layer}/year={now:%Y}/month={now:%m}/day={now:%d}"


def write_batch(raw_records, processed_records):
    """Upload one batch. One object per layer per batch, never one per event."""
    if not processed_records:
        return
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET is not set. Check your .env file.")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%S%fZ")

    bronze_key = f"{_partition('bronze/transactions', now)}/batch-{stamp}.jsonl"
    bronze_body = "\n".join(json.dumps(r, default=str) for r in raw_records) + "\n"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=bronze_key,
        Body=bronze_body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    silver_key = f"{_partition('silver/transactions', now)}/batch-{stamp}.parquet"
    buffer = io.BytesIO()
    pd.DataFrame(processed_records).to_parquet(buffer, index=False, engine="pyarrow")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=silver_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Uploaded {len(processed_records)} records: {bronze_key} and {silver_key}")
