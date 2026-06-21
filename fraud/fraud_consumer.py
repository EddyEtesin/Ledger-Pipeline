"""
fraud_consumer.py

Reads new clean transactions from the Silver Delta table and scores each
one against three rule-based fraud checks:

  1. Velocity check: 8+ transactions from one account within 60 seconds
     (this directly targets the burst-mode transactions the producer
     deliberately injects)
  2. Amount anomaly: a transaction far above an account's historical
     average amount
  3. New high-value counterparty: a large transfer to an account this
     sender has never transacted with before

Transactions that trip ANY rule get written to a `flagged` Delta table
with a fraud_score and the specific reason(s). This table is the
conceptual DLQ destination that Phase 5 will archive into MinIO.

Tracking "what's new" in Silver uses the same offset-based approach as
Silver's own Bronze-reading logic (see silver_consumer.py for the full
reasoning): identity-based tracking, not row position, because Delta
table row order is not guaranteed to match write order.

IMPORTANT ORDERING RULE: each transaction is scored against an account's
EXISTING history BEFORE that transaction's own data is folded into the
history. Otherwise a transaction would be compared against itself.
"""

import json
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from account_history import AccountHistory

# --- Configuration ---
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "/data/silver/transactions")
FLAGGED_TABLE_PATH = os.environ.get("FLAGGED_TABLE_PATH", "/data/fraud/flagged")

STATE_FILE_PATH = os.environ.get("FRAUD_TRACKING_STATE_PATH", "/data/fraud_tracking_state.json")

POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "15"))

VELOCITY_THRESHOLD = int(os.environ.get("VELOCITY_THRESHOLD", "8"))
AMOUNT_ANOMALY_MULTIPLIER = float(os.environ.get("AMOUNT_ANOMALY_MULTIPLIER", "5.0"))
MIN_TRANSACTIONS_FOR_BASELINE = int(os.environ.get("MIN_TRANSACTIONS_FOR_BASELINE", "5"))
NEW_COUNTERPARTY_AMOUNT_THRESHOLD = float(os.environ.get("NEW_COUNTERPARTY_AMOUNT_THRESHOLD", "200000"))

FLAGGED_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string()),
    pa.field("account_id", pa.string()),
    pa.field("counterparty_account_id", pa.string()),
    pa.field("transaction_type", pa.string()),
    pa.field("amount", pa.float64()),
    pa.field("timestamp", pa.string()),
    pa.field("is_burst_generated", pa.bool_()),  # ground-truth tag, see producer.py
    pa.field("fraud_score", pa.int32()),
    pa.field("fraud_reasons", pa.string()),  # comma-joined, simplest portable format
    pa.field("flagged_at", pa.string()),
])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_processed_ids() -> set[str]:
    """
    Tracks which transaction_ids have already been scored, persisted to disk.
    Unlike Bronze/Silver's offset-based tracking, Silver rows don't carry a
    Kafka offset (that's Bronze-specific ingestion metadata, intentionally
    dropped when Silver was written). transaction_id is already guaranteed
    unique by the producer (it's a UUID), so a simple persisted set of IDs
    serves the same purpose here: identity-based tracking that's immune to
    Delta's row-ordering not matching write order.
    """
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, "r") as f:
            return set(json.load(f).get("processed_transaction_ids", []))
    return set()


def save_processed_ids(processed_ids: set[str]):
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, "w") as f:
        json.dump({"processed_transaction_ids": list(processed_ids)}, f, indent=2)


def score_transaction(txn: dict, history: AccountHistory) -> tuple[int, list[str]]:
    """
    Scores a single transaction against existing account history.
    Returns (fraud_score, reasons). fraud_score is just a count of rules
    tripped — simple and explainable, not a calibrated probability.
    """
    reasons = []
    account_id = txn["account_id"]
    timestamp = datetime.fromisoformat(txn["timestamp"])
    amount = txn["amount"]

    # Rule 1: velocity
    recent_count = history.get_recent_transaction_count(account_id, timestamp)
    if recent_count >= VELOCITY_THRESHOLD:
        reasons.append(f"velocity: {recent_count} transactions in prior 120s window")

    # Rule 2: amount anomaly (only meaningful once we have a real baseline)
    avg_amount = history.get_avg_amount(account_id)
    record = history.state.get(account_id)
    has_enough_history = record is not None and record["transaction_count"] >= MIN_TRANSACTIONS_FOR_BASELINE
    is_amount_anomalous = (
        has_enough_history and avg_amount and amount > avg_amount * AMOUNT_ANOMALY_MULTIPLIER
    )
    if is_amount_anomalous:
        reasons.append(f"amount_anomaly: {amount:.2f} is {amount/avg_amount:.1f}x account avg ({avg_amount:.2f})")

    # Rule 3: new counterparty, but ONLY in combination with an anomalous amount.
    # A normal-sized payment to someone new is ordinary behavior (most transfers
    # in this dataset go to a counterparty never seen before, since the producer
    # picks recipients at random — that alone isn't suspicious). What's actually
    # suspicious is a LARGE, UNUSUAL payment to someone never paid before.
    counterparty_id = txn.get("counterparty_account_id")
    if txn["transaction_type"] == "transfer" and counterparty_id:
        is_new_counterparty = not history.is_known_counterparty(account_id, counterparty_id)
        if is_new_counterparty and is_amount_anomalous:
            reasons.append(f"new_counterparty_high_value: first-ever transfer to {counterparty_id} for {amount:.2f}")

    return len(reasons), reasons


def write_flagged(rows: list[dict]):
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=FLAGGED_SCHEMA)
    write_deltalake(FLAGGED_TABLE_PATH, table, mode="append")
    print(f"[fraud] Wrote {len(rows)} flagged rows")


def main():
    print(f"[fraud] Watching Silver table at {SILVER_TABLE_PATH}")
    print(f"[fraud] Flagged -> {FLAGGED_TABLE_PATH}")

    history = AccountHistory()
    processed_transaction_ids = load_processed_ids()
    print(f"[fraud] Resuming with {len(processed_transaction_ids)} previously processed transaction IDs")

    while True:
        try:
            if not DeltaTable.is_deltatable(SILVER_TABLE_PATH):
                print("[fraud] Silver table doesn't exist yet, waiting...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            silver_table = DeltaTable(SILVER_TABLE_PATH).to_pyarrow_table()
            all_rows = silver_table.to_pylist()

            new_rows = [r for r in all_rows if r["transaction_id"] not in processed_transaction_ids]

            if not new_rows:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Sort by timestamp so velocity/history checks see transactions
            # in a sensible chronological order within this batch.
            new_rows.sort(key=lambda r: r["timestamp"])

            flagged_rows = []
            timestamp_now = now_iso()

            for txn in new_rows:
                score, reasons = score_transaction(txn, history)

                if score > 0:
                    flagged_rows.append({
                        "transaction_id": txn["transaction_id"],
                        "account_id": txn["account_id"],
                        "counterparty_account_id": txn.get("counterparty_account_id"),
                        "transaction_type": txn["transaction_type"],
                        "amount": txn["amount"],
                        "timestamp": txn["timestamp"],
                        "is_burst_generated": txn.get("is_burst_generated"),
                        "fraud_score": score,
                        "fraud_reasons": "; ".join(reasons),
                        "flagged_at": timestamp_now,
                    })

                # Update history AFTER scoring, never before (see module docstring)
                history.record_transaction(
                    txn["account_id"],
                    txn["timestamp"],
                    txn["amount"],
                    txn.get("counterparty_account_id"),
                )
                processed_transaction_ids.add(txn["transaction_id"])

            write_flagged(flagged_rows)
            history.save()
            save_processed_ids(processed_transaction_ids)

            print(f"[fraud] Processed {len(new_rows)} new transactions, "
                  f"{len(flagged_rows)} flagged")

        except Exception as e:
            print(f"[fraud] Error during processing cycle: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()