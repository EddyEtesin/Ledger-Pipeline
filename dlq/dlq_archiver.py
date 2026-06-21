"""
dlq_archiver.py

Continuously archives new rows from the `flagged` (fraud) and `rejected`
(Silver validation failures) Delta tables into MinIO, as Parquet files,
organized by source and date — mirroring how a real S3-based DLQ archive
would be laid out:

  ledger-dlq/
  ├── flagged/date=YYYY-MM-DD/batch_<timestamp>.parquet
  └── rejected/date=YYYY-MM-DD/batch_<timestamp>.parquet

Tracking "what's new": same identity-based approach as fraud_consumer.py —
a persisted set of already-archived transaction_ids (flagged) or a
composite identity (rejected, which has no transaction_id since the
record never parsed far enough to have one — uses
bronze_kafka_partition + bronze_kafka_offset instead, which IS guaranteed
unique per record).
"""

import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from deltalake import DeltaTable

# --- Configuration ---
FLAGGED_TABLE_PATH = os.environ.get("FLAGGED_TABLE_PATH", "/data/fraud/flagged")
REJECTED_TABLE_PATH = os.environ.get("REJECTED_TABLE_PATH", "/data/silver/rejected")

STATE_FILE_PATH = os.environ.get("DLQ_STATE_PATH", "/data/dlq_archiver_state.json")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "ledgeradmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "ledgerpassword123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "ledger-dlq")

POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "30"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO ignores this but boto3 requires a value
    )


def ensure_bucket_exists(client):
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"[dlq] Created bucket '{MINIO_BUCKET}'")


def load_state() -> dict:
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, "r") as f:
            return json.load(f)
    return {"archived_flagged_ids": [], "archived_rejected_ids": []}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def get_new_rows_by_id(table_path: str, id_field_fn, already_archived: set) -> list[dict]:
    """
    Generic identity-based "what's new" reader, consistent with the rest
    of the pipeline's approach to Delta table row-order safety.
    id_field_fn extracts a unique identity string from a row dict.
    """
    if not DeltaTable.is_deltatable(table_path):
        return []
    rows = DeltaTable(table_path).to_pyarrow_table().to_pylist()
    return [r for r in rows if id_field_fn(r) not in already_archived]


def flagged_id(row: dict) -> str:
    return row["transaction_id"]


def rejected_id(row: dict) -> str:
    # Rejected records have no transaction_id (they often failed to parse
    # far enough to have one) — partition+offset from Bronze is the
    # guaranteed-unique identity instead.
    return f"{row.get('bronze_kafka_partition')}:{row.get('bronze_kafka_offset')}"


def archive_rows(client, rows: list[dict], schema: pa.Schema, source_label: str):
    """Writes rows to MinIO as a single Parquet file under source/date=.../batch_<ts>.parquet"""
    if not rows:
        return

    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    key = f"{source_label}/date={today_date_str()}/batch_{int(time.time()*1000)}.parquet"
    client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"[dlq] Archived {len(rows)} {source_label} rows to s3://{MINIO_BUCKET}/{key}")


# Schemas mirror the source Delta tables' columns exactly, since this is
# an archive copy, not a transformation step.
FLAGGED_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("account_id", pa.string()),
    pa.field("counterparty_account_id", pa.string()),
    pa.field("transaction_type", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("timestamp", pa.string()),
    pa.field("is_burst_generated", pa.bool_()),
    pa.field("fraud_score", pa.int32()),
    pa.field("fraud_reasons", pa.string()),
    pa.field("flagged_at", pa.string()),
])

REJECTED_SCHEMA = pa.schema([
    pa.field("raw_value", pa.string()),
    pa.field("rejection_reason", pa.string()),
    pa.field("bronze_kafka_offset", pa.int64()),
    pa.field("bronze_kafka_partition", pa.int32()),
    pa.field("validated_at", pa.string()),
])


def main():
    print(f"[dlq] Watching flagged={FLAGGED_TABLE_PATH}, rejected={REJECTED_TABLE_PATH}")
    print(f"[dlq] Archiving to MinIO bucket '{MINIO_BUCKET}' at {MINIO_ENDPOINT}")

    client = get_minio_client()
    ensure_bucket_exists(client)

    state = load_state()
    archived_flagged = set(state.get("archived_flagged_ids", []))
    archived_rejected = set(state.get("archived_rejected_ids", []))
    print(f"[dlq] Resuming with {len(archived_flagged)} flagged, "
          f"{len(archived_rejected)} rejected already archived")

    while True:
        try:
            new_flagged = get_new_rows_by_id(FLAGGED_TABLE_PATH, flagged_id, archived_flagged)
            new_rejected = get_new_rows_by_id(REJECTED_TABLE_PATH, rejected_id, archived_rejected)

            if new_flagged:
                archive_rows(client, new_flagged, FLAGGED_SCHEMA, "flagged")
                archived_flagged.update(flagged_id(r) for r in new_flagged)

            if new_rejected:
                archive_rows(client, new_rejected, REJECTED_SCHEMA, "rejected")
                archived_rejected.update(rejected_id(r) for r in new_rejected)

            if new_flagged or new_rejected:
                save_state({
                    "archived_flagged_ids": list(archived_flagged),
                    "archived_rejected_ids": list(archived_rejected),
                })
                print(f"[dlq] Cycle complete: {len(new_flagged)} flagged, "
                      f"{len(new_rejected)} rejected newly archived")

        except Exception as e:
            print(f"[dlq] Error during archiving cycle: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
