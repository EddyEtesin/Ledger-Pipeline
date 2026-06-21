"""
bronze_consumer.py

Reads raw transaction events from the Kafka topic and writes them,
completely untouched, into a Bronze Delta Lake table.

Design principle: Bronze does not validate, clean, or filter anything.
Even malformed JSON gets captured (wrapped with an error marker) rather
than dropped, so nothing is ever silently lost before it reaches storage.

Crash safety: Kafka offsets are committed manually, AFTER a batch has been
successfully written to the Delta table. This means a crash can cause a
small number of duplicate rows to be reprocessed on restart, but can never
cause data loss. Deduplication, if needed, happens downstream in Silver.
"""

import json
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer, KafkaException
from deltalake import write_deltalake

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "bank-transactions")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "bronze-consumer-group")

BRONZE_TABLE_PATH = os.environ.get("BRONZE_TABLE_PATH", "/data/bronze/transactions")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))          # messages per Delta write
BATCH_TIMEOUT_SECONDS = float(os.environ.get("BATCH_TIMEOUT_SECONDS", "10"))  # max wait before flushing a partial batch
POLL_TIMEOUT_SECONDS = float(os.environ.get("POLL_TIMEOUT_SECONDS", "1.0"))

# Explicit schema, defined up front rather than inferred from data.
# This matters because PyArrow infers column types from the values it sees in
# a given batch — if every row in a batch happens to have None for some field,
# PyArrow can't determine that column's type, and Delta Lake rejects the write
# entirely ("Invalid data type for Delta Lake: Null"). Fixing the schema here
# guarantees every batch writes with the same, correct types regardless of
# which values happen to appear.
BRONZE_SCHEMA = pa.schema([
    pa.field("ingested_at", pa.string()),
    pa.field("kafka_topic", pa.string()),
    pa.field("kafka_partition", pa.int32()),
    pa.field("kafka_offset", pa.int64()),
    pa.field("kafka_key", pa.string()),
    pa.field("raw_value", pa.string()),
    pa.field("parse_error", pa.string()),
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
])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_message(msg) -> dict:
    """
    Parses a Kafka message into a Bronze row dict.
    Never raises — malformed JSON is captured as an error record instead
    of being dropped, per the "Bronze never throws data away" principle.
    """
    raw_value = msg.value().decode("utf-8", errors="replace")

    base = {
        "ingested_at": now_iso(),
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
        "raw_value": raw_value,
        "parse_error": None,
    }

    # Bronze fields we expect from a well-formed transaction event.
    # If parsing fails, these stay None and parse_error explains why —
    # the row still gets written, just flagged.
    expected_fields = {
        "transaction_id": None,
        "account_id": None,
        "counterparty_account_id": None,
        "transaction_type": None,
        "amount": None,
        "currency": None,
        "channel": None,
        "timestamp": None,
        "status": None,
        "is_burst_generated": None,
    }

    try:
        parsed = json.loads(raw_value)
        for field in expected_fields:
            expected_fields[field] = parsed.get(field)
    except json.JSONDecodeError as e:
        base["parse_error"] = f"JSONDecodeError: {e}"

    return {**base, **expected_fields}


def write_batch(rows: list[dict]):
    """
    Writes a batch of Bronze rows to the Delta table, using the fixed
    BRONZE_SCHEMA rather than inferring types from the batch's values.
    write_deltalake creates the table automatically on the very first write
    when mode="append" is used against a path that doesn't yet exist.
    """
    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    write_deltalake(BRONZE_TABLE_PATH, table, mode="append")


def main():
    print(f"[bronze] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")
    print(f"[bronze] Writing to Delta table at {BRONZE_TABLE_PATH}")

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",   # on first-ever run, start from the beginning of the topic
        "enable.auto.commit": False,       # we commit manually, only after a successful Delta write
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    batch: list[dict] = []
    batch_start_time = time.time()
    total_written = 0

    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)

            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                batch.append(parse_message(msg))

            batch_is_full = len(batch) >= BATCH_SIZE
            batch_has_timed_out = batch and (time.time() - batch_start_time >= BATCH_TIMEOUT_SECONDS)

            if batch and (batch_is_full or batch_has_timed_out):
                write_batch(batch)
                consumer.commit(asynchronous=False)  # only commit AFTER the write succeeds

                total_written += len(batch)
                print(f"[bronze] Wrote batch of {len(batch)} rows "
                      f"(total: {total_written}). Offsets committed.")

                batch = []
                batch_start_time = time.time()

    except KeyboardInterrupt:
        print("[bronze] Shutting down gracefully...")
    finally:
        # Flush any partial batch left in memory before exiting, so a clean
        # shutdown doesn't waste already-consumed-but-unwritten messages.
        if batch:
            write_batch(batch)
            consumer.commit(asynchronous=False)
            total_written += len(batch)
            print(f"[bronze] Final partial batch of {len(batch)} rows flushed.")
        consumer.close()
        print(f"[bronze] Total rows written this run: {total_written}. Goodbye.")


if __name__ == "__main__":
    main()