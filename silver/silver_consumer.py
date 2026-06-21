"""
silver_consumer.py

Reads new rows from the Bronze Delta table, validates them, and splits
them into two destinations:
  - Silver table: structurally valid transactions (regardless of business
    outcome — a failed-insufficient-funds withdrawal is still valid DATA)
  - Rejected table: structurally broken records (bad JSON, missing fields,
    invalid types, nonsensical values)

Tracking "what's new" in Bronze: each Bronze row carries a unique
(kafka_partition, kafka_offset) identity, the same identity Kafka itself
uses. Silver tracks the highest offset it has successfully processed PER
PARTITION, rather than a row count. This is deliberately NOT based on row
position in the table — Delta Lake does not guarantee that to_pyarrow_table()
returns rows in write order, since it reads from underlying Parquet files
whose read order isn't guaranteed. Tracking by identity instead of position
makes this correct regardless of file layout, compaction, or future changes
to how Bronze writes files.

State is only updated after a successful write to Silver/Rejected, mirroring
the same "commit only after a safe write" pattern used in Bronze's Kafka
offset handling.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

# --- Configuration ---
BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "/data/bronze/transactions")
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "/data/silver/transactions")
REJECTED_TABLE_PATH = os.environ.get("REJECTED_TABLE_PATH", "/data/silver/rejected")

STATE_FILE = Path(os.environ.get("SILVER_STATE_PATH", "/data/silver_state.json"))

POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "15"))

VALID_TRANSACTION_TYPES = {"deposit", "withdrawal", "transfer"}
VALID_STATUSES = {"completed", "failed_insufficient_funds"}
REQUIRED_FIELDS = ["transaction_id", "account_id", "transaction_type", "amount", "currency", "timestamp", "status"]

# Silver schema: the Bronze ingestion metadata columns are dropped here —
# Silver represents clean BUSINESS data, not pipeline mechanics. Bronze
# remains the place to go if you ever need that ingestion-level detail.
SILVER_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("account_id", pa.string()),
    pa.field("counterparty_account_id", pa.string()),
    pa.field("transaction_type", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("currency", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("status", pa.string()),
    pa.field("is_burst_generated", pa.bool_()),
    pa.field("validated_at", pa.string()),
])

# Rejected schema: keeps the raw value and a human-readable reason, so a
# rejected row is still fully inspectable later — Silver never just
# "drops" data, it redirects it with an explanation.
REJECTED_SCHEMA = pa.schema([
    pa.field("raw_value", pa.string()),
    pa.field("rejection_reason", pa.string()),
    pa.field("bronze_kafka_offset", pa.int64()),
    pa.field("bronze_kafka_partition", pa.int32()),
    pa.field("validated_at", pa.string()),
])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state() -> dict[str, int]:
    """
    Returns {partition_str: max_offset_processed}. Empty dict on first run.
    Keys are strings because JSON object keys must be strings; partition
    numbers are cast back to int when used.
    """
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f).get("max_offset_by_partition", {})
    return {}


def save_state(max_offset_by_partition: dict[str, int]):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"max_offset_by_partition": max_offset_by_partition}, f, indent=2)


def validate_row(row: dict) -> tuple[bool, str | None]:
    """
    Returns (is_valid, rejection_reason). rejection_reason is None if valid.
    Validates structural/data correctness only — NOT business outcome.
    A failed_insufficient_funds transaction is valid data.
    """
    if row.get("parse_error"):
        return False, f"Bronze parse error: {row['parse_error']}"

    for field in REQUIRED_FIELDS:
        if row.get(field) is None:
            return False, f"Missing required field: {field}"

    if row["transaction_type"] not in VALID_TRANSACTION_TYPES:
        return False, f"Invalid transaction_type: {row['transaction_type']}"

    if row["status"] not in VALID_STATUSES:
        return False, f"Invalid status: {row['status']}"

    try:
        amount = float(row["amount"])
        if amount <= 0:
            return False, f"Non-positive amount: {amount}"
    except (TypeError, ValueError):
        return False, f"Amount is not a valid number: {row.get('amount')}"

    try:
        txn_time = datetime.fromisoformat(row["timestamp"])
        if txn_time > datetime.now(timezone.utc):
            return False, f"Timestamp is in the future: {row['timestamp']}"
    except (TypeError, ValueError):
        return False, f"Unparseable timestamp: {row.get('timestamp')}"

    if row["currency"] != "NGN":
        return False, f"Unexpected currency: {row['currency']}"

    if row["transaction_type"] == "transfer" and not row.get("counterparty_account_id"):
        return False, "Transfer missing counterparty_account_id"

    if row["transaction_type"] != "transfer" and row.get("counterparty_account_id"):
        return False, f"Non-transfer ({row['transaction_type']}) unexpectedly has a counterparty_account_id"

    return True, None


def process_new_rows(bronze_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Splits Bronze rows into (clean_rows, rejected_rows) ready for their target schemas."""
    clean_rows = []
    rejected_rows = []
    timestamp = now_iso()

    for row in bronze_rows:
        is_valid, reason = validate_row(row)
        if is_valid:
            clean_rows.append({
                "transaction_id": row["transaction_id"],
                "account_id": row["account_id"],
                "counterparty_account_id": row.get("counterparty_account_id"),
                "transaction_type": row["transaction_type"],
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "channel": row.get("channel"),
                "timestamp": row["timestamp"],
                "status": row["status"],
                "is_burst_generated": row.get("is_burst_generated"),
                "validated_at": timestamp,
            })
        else:
            rejected_rows.append({
                "raw_value": row.get("raw_value"),
                "rejection_reason": reason,
                "bronze_kafka_offset": row.get("kafka_offset"),
                "bronze_kafka_partition": row.get("kafka_partition"),
                "validated_at": timestamp,
            })

    return clean_rows, rejected_rows


def write_if_any(rows: list[dict], path: str, schema: pa.Schema, label: str):
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=schema)
    write_deltalake(path, table, mode="append")
    print(f"[silver] Wrote {len(rows)} rows to {label} ({path})")


def get_new_rows(bronze_table: pa.Table, max_offset_by_partition: dict[str, int]) -> list[dict]:
    """
    Returns Bronze rows not yet processed, identified by (kafka_partition,
    kafka_offset) rather than row position — safe regardless of how Delta
    orders rows internally.
    """
    if not max_offset_by_partition:
        # Nothing processed yet — everything is new.
        return bronze_table.to_pylist()

    partitions = bronze_table.column("kafka_partition").to_pylist()
    offsets = bronze_table.column("kafka_offset").to_pylist()
    all_rows = bronze_table.to_pylist()

    new_rows = []
    for row, partition, offset in zip(all_rows, partitions, offsets):
        last_seen = max_offset_by_partition.get(str(partition), -1)
        if offset > last_seen:
            new_rows.append(row)
    return new_rows


def update_max_offsets(rows: list[dict], max_offset_by_partition: dict[str, int]) -> dict[str, int]:
    """Returns a new dict with max_offset_by_partition updated to cover the given rows."""
    updated = dict(max_offset_by_partition)
    for row in rows:
        partition_key = str(row.get("kafka_partition"))
        offset = row.get("kafka_offset")
        if offset is None:
            continue
        if offset > updated.get(partition_key, -1):
            updated[partition_key] = offset
    return updated


def main():
    print(f"[silver] Watching Bronze table at {BRONZE_TABLE_PATH}")
    print(f"[silver] Silver -> {SILVER_TABLE_PATH}, Rejected -> {REJECTED_TABLE_PATH}")

    max_offset_by_partition = load_state()
    print(f"[silver] Resuming with max offsets: {max_offset_by_partition}")

    while True:
        try:
            if not DeltaTable.is_deltatable(BRONZE_TABLE_PATH):
                print("[silver] Bronze table doesn't exist yet, waiting...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            bronze_table = DeltaTable(BRONZE_TABLE_PATH).to_pyarrow_table()
            new_rows = get_new_rows(bronze_table, max_offset_by_partition)

            if not new_rows:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            clean_rows, rejected_rows = process_new_rows(new_rows)

            write_if_any(clean_rows, SILVER_TABLE_PATH, SILVER_SCHEMA, "Silver")
            write_if_any(rejected_rows, REJECTED_TABLE_PATH, REJECTED_SCHEMA, "Rejected")

            # Only commit progress after both writes above succeeded
            max_offset_by_partition = update_max_offsets(new_rows, max_offset_by_partition)
            save_state(max_offset_by_partition)

            print(f"[silver] Processed {len(new_rows)} new rows "
                  f"({len(clean_rows)} clean, {len(rejected_rows)} rejected). "
                  f"Offsets now: {max_offset_by_partition}")

        except Exception as e:
            print(f"[silver] Error during processing cycle: {e}")
            # Deliberately do NOT update state on error — next cycle will retry
            # the same rows rather than silently skip them.

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()