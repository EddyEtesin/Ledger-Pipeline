"""
producer.py

Generates synthetic Nigerian bank transactions (deposits, withdrawals, transfers)
and streams them continuously to a Kafka topic.

Two modes of generation:
  - Normal transactions: realistic random activity across the account pool,
    respecting account balances.
  - Burst mode: periodically picks one account and fires a rapid sequence of
    transactions in a short window, simulating fraudulent velocity patterns.
    These bursts are what the fraud detection layer (Phase 4) is designed to catch.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from accounts import AccountPool

# --- Configuration (overridable via environment variables) ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "bank-transactions")

MIN_DELAY_SECONDS = float(os.environ.get("MIN_DELAY_SECONDS", "1.0"))
MAX_DELAY_SECONDS = float(os.environ.get("MAX_DELAY_SECONDS", "4.0"))

BURST_CHECK_INTERVAL_SECONDS = float(os.environ.get("BURST_CHECK_INTERVAL_SECONDS", "120"))
BURST_PROBABILITY = float(os.environ.get("BURST_PROBABILITY", "0.5"))
BURST_MIN_TXNS = int(os.environ.get("BURST_MIN_TXNS", "8"))
BURST_MAX_TXNS = int(os.environ.get("BURST_MAX_TXNS", "12"))
BURST_DELAY_SECONDS = float(os.environ.get("BURST_DELAY_SECONDS", "2.0"))

# Probability, per normal transaction cycle, of sending a deliberately malformed
# message instead — proves Bronze/Silver's "never lose broken data" handling
# against the live pipeline, not just unit tests.
MALFORMED_MESSAGE_PROBABILITY = float(os.environ.get("MALFORMED_MESSAGE_PROBABILITY", "0.01"))

CHANNELS = ["mobile", "atm", "branch", "online"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer"]

# Weighting: transfers and deposits are more common day-to-day than withdrawals
TRANSACTION_TYPE_WEIGHTS = [0.35, 0.25, 0.40]  # deposit, withdrawal, transfer


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_transaction(
    account_pool: AccountPool,
    transaction_type: str = None,
    account_id: str = None,
    amount: float = None,
    is_burst: bool = False,
) -> dict | None:
    """
    Builds a single transaction dict and applies its effect to account balances.
    Returns None if the transaction couldn't be applied (e.g. insufficient funds)
    — caller decides whether to still emit it as a "failed" event or skip it.
    """
    txn_type = transaction_type or random.choices(TRANSACTION_TYPES, weights=TRANSACTION_TYPE_WEIGHTS, k=1)[0]
    acc = account_id or account_pool.random_account()

    txn = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": acc,
        "counterparty_account_id": None,
        "transaction_type": txn_type,
        "amount": None,
        "currency": "NGN",
        "channel": random.choice(CHANNELS),
        "timestamp": now_iso(),
        "status": "pending",
        "is_burst_generated": is_burst,  # metadata, not a "real" field — useful for later validation of our own fraud rules
    }

    if txn_type == "deposit":
        amt = amount or round(random.uniform(1_000, 200_000), 2)
        account_pool.deposit(acc, amt)
        txn["amount"] = amt
        txn["status"] = "completed"

    elif txn_type == "withdrawal":
        amt = amount or round(random.uniform(1_000, 150_000), 2)
        success = account_pool.withdraw(acc, amt)
        txn["amount"] = amt
        txn["status"] = "completed" if success else "failed_insufficient_funds"

    elif txn_type == "transfer":
        counterparty = account_pool.random_account_excluding(acc)
        amt = amount or round(random.uniform(1_000, 300_000), 2)
        success = account_pool.transfer(acc, counterparty, amt)
        txn["counterparty_account_id"] = counterparty
        txn["amount"] = amt
        txn["status"] = "completed" if success else "failed_insufficient_funds"

    return txn


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] Delivery failed for record {msg.key()}: {err}")
    # Success case intentionally quiet to avoid log spam at high volume


def send_transaction(producer: Producer, txn: dict):
    producer.produce(
        KAFKA_TOPIC,
        key=txn["account_id"],
        value=json.dumps(txn),
        callback=delivery_report,
    )
    producer.poll(0)  # trigger delivery callbacks without blocking


def send_malformed_message(producer: Producer):
    """
    Sends a deliberately broken message straight to Kafka, bypassing
    build_transaction entirely (it's not a real transaction, so it must
    never touch account balances). Picks randomly between a few different
    categories of brokenness, to exercise Silver's validation rules.
    """
    kind = random.choice([
        "invalid_json",
        "missing_required_field",
        "negative_amount",
        "bad_transaction_type",
    ])

    if kind == "invalid_json":
        raw_value = '{"account_id": "1234567890", "amount": 5000, "currency": "NGN" '  # truncated/broken JSON
    elif kind == "missing_required_field":
        raw_value = json.dumps({
            "transaction_id": str(uuid.uuid4()),
            # account_id deliberately omitted
            "transaction_type": "deposit",
            "amount": 10000,
            "currency": "NGN",
            "timestamp": now_iso(),
            "status": "completed",
        })
    elif kind == "negative_amount":
        raw_value = json.dumps({
            "transaction_id": str(uuid.uuid4()),
            "account_id": "0000000000",
            "transaction_type": "deposit",
            "amount": -500,
            "currency": "NGN",
            "timestamp": now_iso(),
            "status": "completed",
        })
    else:  # bad_transaction_type
        raw_value = json.dumps({
            "transaction_id": str(uuid.uuid4()),
            "account_id": "0000000000",
            "transaction_type": "teleport",
            "amount": 5000,
            "currency": "NGN",
            "timestamp": now_iso(),
            "status": "completed",
        })

    producer.produce(
        KAFKA_TOPIC,
        key="malformed-test",
        value=raw_value,
        callback=delivery_report,
    )
    producer.poll(0)
    print(f"[producer] ⚠️  Sent deliberately malformed message (kind={kind})")


def run_burst(producer: Producer, account_pool: AccountPool):
    target_account = account_pool.random_account()
    num_txns = random.randint(BURST_MIN_TXNS, BURST_MAX_TXNS)
    burst_type = random.choice(["withdrawal", "transfer"])

    print(f"[producer] 🚨 BURST starting: account={target_account}, "
          f"type={burst_type}, count={num_txns}")

    for i in range(num_txns):
        txn = build_transaction(
            account_pool,
            transaction_type=burst_type,
            account_id=target_account,
            amount=round(random.uniform(5_000, 50_000), 2),
            is_burst=True,
        )
        send_transaction(producer, txn)
        print(f"[producer]   burst txn {i+1}/{num_txns}: {txn['transaction_id']} "
              f"({txn['status']})")
        time.sleep(BURST_DELAY_SECONDS)

    print(f"[producer] Burst complete for account={target_account}")


def main():
    print(f"[producer] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ledger-transaction-producer",
    }
    producer = Producer(producer_conf)

    account_pool = AccountPool()

    last_burst_check = time.time()
    txn_count = 0

    try:
        while True:
            # Occasionally send a deliberately broken message instead of a real transaction
            if random.random() < MALFORMED_MESSAGE_PROBABILITY:
                send_malformed_message(producer)
            else:
                # Normal transaction
                txn = build_transaction(account_pool)
                send_transaction(producer, txn)
                txn_count += 1

                if txn_count % 20 == 0:
                    print(f"[producer] {txn_count} transactions sent so far...")

            # Periodically consider triggering a burst
            if time.time() - last_burst_check >= BURST_CHECK_INTERVAL_SECONDS:
                last_burst_check = time.time()
                if random.random() < BURST_PROBABILITY:
                    run_burst(producer, account_pool)

            time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))

    except KeyboardInterrupt:
        print("[producer] Shutting down gracefully...")
    finally:
        producer.flush(timeout=10)
        print(f"[producer] Final count: {txn_count} transactions sent. Goodbye.")


if __name__ == "__main__":
    main()