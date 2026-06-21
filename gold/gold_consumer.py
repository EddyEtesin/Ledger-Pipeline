"""
gold_consumer.py

Builds business-ready aggregate (Gold) tables from Silver and Flagged data.
Unlike Bronze/Silver/Fraud, Gold tables are RECOMPUTED from scratch each
cycle, not appended to — a "daily total" or "current balance" only makes
sense as the latest full computation, not as something you incrementally
add rows to. Each cycle: read all of Silver (+ Flagged for fraud rate),
recompute all four summary tables, and OVERWRITE them.

Four tables:
  1. daily_volume       — per-day transaction counts/amounts by type
  2. account_balances    — current balance per account, reported directly
                           from the producer's live state (NOT reconstructed
                           from Silver — see PRODUCER_ACCOUNTS_STATE_PATH and
                           compute_account_balances docstrings for why)
  3. daily_fraud_rate    — per-day flagged count/rate/amount
  4. top_accounts        — per-account total transaction count and volume

Only `status == "completed"` transactions move money in account_balances —
failed_insufficient_funds transactions are real, valid DATA (per the
Silver validation decision) but they never actually moved any money, so
they must not affect computed balances.
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# --- Configuration ---
SILVER_TABLE_PATH = os.environ.get("SILVER_TABLE_PATH", "/data/silver/transactions")
FLAGGED_TABLE_PATH = os.environ.get("FLAGGED_TABLE_PATH", "/data/fraud/flagged")

DAILY_VOLUME_PATH = os.environ.get("DAILY_VOLUME_PATH", "/data/gold/daily_volume")
ACCOUNT_BALANCES_PATH = os.environ.get("ACCOUNT_BALANCES_PATH", "/data/gold/account_balances")
DAILY_FRAUD_RATE_PATH = os.environ.get("DAILY_FRAUD_RATE_PATH", "/data/gold/daily_fraud_rate")
TOP_ACCOUNTS_PATH = os.environ.get("TOP_ACCOUNTS_PATH", "/data/gold/top_accounts")

# KNOWN LAYERING SHORTCUT: account starting balances are never emitted as
# events anywhere in the pipeline (accounts.py assigns them once, in-memory,
# before any transaction occurs) — so Bronze/Silver have no record of them,
# and Gold cannot reconstruct true balances from transaction history alone.
# The architecturally correct fix would be an "account_opened" event from
# the producer that flows through Bronze/Silver like any other transaction.
# Instead, Gold reads the producer's accounts.json directly as a starting
# point. This couples Gold to the producer's internal state file, breaking
# the normal Bronze/Silver/Gold boundary — a deliberate, acknowledged
# shortcut for this project's scope, not the production-correct approach.
PRODUCER_ACCOUNTS_STATE_PATH = os.environ.get("PRODUCER_ACCOUNTS_STATE_PATH", "/producer_data/accounts.json")

POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "30"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def date_of(timestamp_str: str) -> str:
    return datetime.fromisoformat(timestamp_str).strftime("%Y-%m-%d")


# --- Schemas ---

DAILY_VOLUME_SCHEMA = pa.schema([
    pa.field("date", pa.string()),
    pa.field("total_transaction_count", pa.int64()),
    pa.field("total_amount", pa.float64()),
    pa.field("deposit_count", pa.int64()),
    pa.field("deposit_amount", pa.float64()),
    pa.field("withdrawal_count", pa.int64()),
    pa.field("withdrawal_amount", pa.float64()),
    pa.field("transfer_count", pa.int64()),
    pa.field("transfer_amount", pa.float64()),
    pa.field("computed_at", pa.string()),
])

ACCOUNT_BALANCES_SCHEMA = pa.schema([
    pa.field("account_id", pa.string()),
    pa.field("computed_balance", pa.float64()),
    pa.field("completed_transaction_count", pa.int64()),
    pa.field("computed_at", pa.string()),
])

DAILY_FRAUD_RATE_SCHEMA = pa.schema([
    pa.field("date", pa.string()),
    pa.field("total_transaction_count", pa.int64()),
    pa.field("flagged_count", pa.int64()),
    pa.field("fraud_rate_pct", pa.float64()),
    pa.field("flagged_amount", pa.float64()),
    pa.field("computed_at", pa.string()),
])

TOP_ACCOUNTS_SCHEMA = pa.schema([
    pa.field("account_id", pa.string()),
    pa.field("total_transaction_count", pa.int64()),
    pa.field("total_amount", pa.float64()),
    pa.field("rank", pa.int64()),
    pa.field("computed_at", pa.string()),
])


def compute_daily_volume(silver_rows: list[dict]) -> list[dict]:
    by_date = defaultdict(lambda: {
        "total_transaction_count": 0, "total_amount": 0.0,
        "deposit_count": 0, "deposit_amount": 0.0,
        "withdrawal_count": 0, "withdrawal_amount": 0.0,
        "transfer_count": 0, "transfer_amount": 0.0,
    })

    for row in silver_rows:
        d = date_of(row["timestamp"])
        bucket = by_date[d]
        bucket["total_transaction_count"] += 1
        bucket["total_amount"] += row["amount"]
        txn_type = row["transaction_type"]
        bucket[f"{txn_type}_count"] += 1
        bucket[f"{txn_type}_amount"] += row["amount"]

    timestamp = now_iso()
    return [
        {"date": d, **stats, "computed_at": timestamp}
        for d, stats in sorted(by_date.items())
    ]


def load_producer_balances() -> dict[str, float]:
    """
    Reads the producer's current account balances directly. See the
    PRODUCER_ACCOUNTS_STATE_PATH comment above for why account_balances
    is sourced this way rather than reconstructed from Silver.
    """
    if not os.path.exists(PRODUCER_ACCOUNTS_STATE_PATH):
        print(f"[gold] WARNING: producer accounts state not found at "
              f"{PRODUCER_ACCOUNTS_STATE_PATH}, account_balances table will be empty")
        return {}
    with open(PRODUCER_ACCOUNTS_STATE_PATH, "r") as f:
        return json.load(f)


def compute_account_balances(silver_rows: list[dict], starting_balances: dict[str, float]) -> list[dict]:
    """
    Reports the producer's LIVE balances directly, rather than attempting
    to reconstruct balances from Silver's transaction history.

    Why: true reconstruction would require knowing each account's ORIGINAL
    starting balance before any transactions occurred. accounts.py never
    persists that separately — it overwrites the balance in place as
    transactions happen, so only the producer itself has an accurate
    balance per account. Re-deriving it from Silver is both redundant
    (the producer is already the enforced source of truth — it's the
    thing that actually rejects withdrawals that would go negative) and,
    without the true starting balance, mathematically impossible to get
    right for any account that has ever transacted.

    This does mean Gold's account_balances table is coupled to the
    producer's internal state file — a known, deliberate layering
    shortcut for this project's scope (see PRODUCER_ACCOUNTS_STATE_PATH
    above). The completed_transaction_count column IS still computed
    independently from Silver, so it remains a genuine cross-check on
    transaction volume even though the balance itself is not re-derived.
    """
    counts = defaultdict(int)
    for row in silver_rows:
        if row["status"] == "completed":
            counts[row["account_id"]] += 1
            if row["transaction_type"] == "transfer":
                counterparty = row.get("counterparty_account_id")
                if counterparty:
                    counts[counterparty] += 1

    timestamp = now_iso()
    return [
        {
            "account_id": acc,
            "computed_balance": round(balance, 2),
            "completed_transaction_count": counts.get(acc, 0),
            "computed_at": timestamp,
        }
        for acc, balance in starting_balances.items()
    ]


def compute_daily_fraud_rate(silver_rows: list[dict], flagged_rows: list[dict]) -> list[dict]:
    total_by_date = defaultdict(int)
    for row in silver_rows:
        total_by_date[date_of(row["timestamp"])] += 1

    flagged_by_date = defaultdict(lambda: {"count": 0, "amount": 0.0})
    for row in flagged_rows:
        d = date_of(row["timestamp"])
        flagged_by_date[d]["count"] += 1
        flagged_by_date[d]["amount"] += row["amount"]

    timestamp = now_iso()
    results = []
    for d, total in sorted(total_by_date.items()):
        flagged = flagged_by_date.get(d, {"count": 0, "amount": 0.0})
        rate = (flagged["count"] / total * 100) if total > 0 else 0.0
        results.append({
            "date": d,
            "total_transaction_count": total,
            "flagged_count": flagged["count"],
            "fraud_rate_pct": round(rate, 3),
            "flagged_amount": round(flagged["amount"], 2),
            "computed_at": timestamp,
        })
    return results


def compute_top_accounts(silver_rows: list[dict], top_n: int = 50) -> list[dict]:
    counts = defaultdict(int)
    amounts = defaultdict(float)

    for row in silver_rows:
        counts[row["account_id"]] += 1
        amounts[row["account_id"]] += row["amount"]

    ranked = sorted(amounts.items(), key=lambda kv: kv[1], reverse=True)[:top_n]

    timestamp = now_iso()
    return [
        {
            "account_id": acc,
            "total_transaction_count": counts[acc],
            "total_amount": round(amt, 2),
            "rank": i + 1,
            "computed_at": timestamp,
        }
        for i, (acc, amt) in enumerate(ranked)
    ]


def overwrite_table(rows: list[dict], path: str, schema: pa.Schema, label: str):
    if not rows:
        print(f"[gold] No rows to write for {label}, skipping")
        return
    table = pa.Table.from_pylist(rows, schema=schema)
    write_deltalake(path, table, mode="overwrite")
    print(f"[gold] Wrote {len(rows)} rows to {label}")


def main():
    print(f"[gold] Watching Silver={SILVER_TABLE_PATH}, Flagged={FLAGGED_TABLE_PATH}")

    while True:
        try:
            if not DeltaTable.is_deltatable(SILVER_TABLE_PATH):
                print("[gold] Silver table doesn't exist yet, waiting...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            silver_rows = DeltaTable(SILVER_TABLE_PATH).to_pyarrow_table().to_pylist()
            flagged_rows = (
                DeltaTable(FLAGGED_TABLE_PATH).to_pyarrow_table().to_pylist()
                if DeltaTable.is_deltatable(FLAGGED_TABLE_PATH) else []
            )
            producer_balances = load_producer_balances()

            daily_volume = compute_daily_volume(silver_rows)
            account_balances = compute_account_balances(silver_rows, producer_balances)
            daily_fraud_rate = compute_daily_fraud_rate(silver_rows, flagged_rows)
            top_accounts = compute_top_accounts(silver_rows)

            overwrite_table(daily_volume, DAILY_VOLUME_PATH, DAILY_VOLUME_SCHEMA, "daily_volume")
            overwrite_table(account_balances, ACCOUNT_BALANCES_PATH, ACCOUNT_BALANCES_SCHEMA, "account_balances")
            overwrite_table(daily_fraud_rate, DAILY_FRAUD_RATE_PATH, DAILY_FRAUD_RATE_SCHEMA, "daily_fraud_rate")
            overwrite_table(top_accounts, TOP_ACCOUNTS_PATH, TOP_ACCOUNTS_SCHEMA, "top_accounts")

            print(f"[gold] Cycle complete. Silver rows: {len(silver_rows)}, "
                  f"Flagged rows: {len(flagged_rows)}")

        except Exception as e:
            print(f"[gold] Error during computation cycle: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()