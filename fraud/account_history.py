"""
account_history.py

Tracks per-account BEHAVIORAL state needed by fraud detection rules —
distinct from accounts.py's balance tracking in the producer.

Three kinds of memory, each shaped for what its rule actually needs:
  - recent_timestamps: short rolling window (pruned), feeds the velocity check
  - avg_amount / transaction_count: a running average, feeds the amount
    anomaly check, never pruned
  - known_counterparties: an ever-growing set, feeds the new-counterparty
    check, never pruned

State persists to a JSON file so restarts don't forget an account's history.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATE_FILE = Path(os.environ.get("FRAUD_STATE_PATH", "/data/fraud_account_history.json"))

# How far back "recent" means for the velocity check.
VELOCITY_WINDOW_SECONDS = int(os.environ.get("VELOCITY_WINDOW_SECONDS", "120"))


class AccountHistory:
    def __init__(self):
        self.state: dict[str, dict] = {}
        self._load()

    def _load(self):
        if STATE_FILE.exists():
            with open(STATE_FILE, "r") as f:
                self.state = json.load(f)
            print(f"[account_history] Loaded history for {len(self.state)} accounts from {STATE_FILE}")
        else:
            print(f"[account_history] No existing state found, starting fresh")

    def save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=2)

    def _get_or_create(self, account_id: str) -> dict:
        if account_id not in self.state:
            self.state[account_id] = {
                "recent_timestamps": [],
                "avg_amount": 0.0,
                "transaction_count": 0,
                "known_counterparties": [],
            }
        return self.state[account_id]

    def get_recent_transaction_count(self, account_id: str, as_of: datetime) -> int:
        """
        Returns how many transactions this account has made within the
        velocity window, counting up to (but not including) `as_of`.
        Also prunes timestamps older than the window as a side effect.
        """
        record = self._get_or_create(account_id)
        cutoff = as_of - timedelta(seconds=VELOCITY_WINDOW_SECONDS)

        kept = [
            ts for ts in record["recent_timestamps"]
            if datetime.fromisoformat(ts) >= cutoff
        ]
        record["recent_timestamps"] = kept
        return len(kept)

    def get_avg_amount(self, account_id: str) -> float | None:
        """Returns the account's running average transaction amount, or None if no history yet."""
        record = self.state.get(account_id)
        if record is None or record["transaction_count"] == 0:
            return None
        return record["avg_amount"]

    def is_known_counterparty(self, account_id: str, counterparty_id: str) -> bool:
        record = self.state.get(account_id)
        if record is None:
            return False
        return counterparty_id in record["known_counterparties"]

    def record_transaction(self, account_id: str, timestamp: str, amount: float, counterparty_id: str | None):
        """
        Updates an account's history AFTER a transaction has been evaluated
        by the fraud rules — this must run after scoring, not before, since
        the rules need to compare the new transaction against PRIOR history,
        not against itself.
        """
        record = self._get_or_create(account_id)

        record["recent_timestamps"].append(timestamp)

        old_avg = record["avg_amount"]
        old_count = record["transaction_count"]
        new_count = old_count + 1
        record["avg_amount"] = (old_avg * old_count + amount) / new_count
        record["transaction_count"] = new_count

        if counterparty_id and counterparty_id not in record["known_counterparties"]:
            record["known_counterparties"].append(counterparty_id)
