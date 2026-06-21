"""
accounts.py

Manages the pool of synthetic bank accounts and their balances.
State is persisted to a JSON file so balances survive container restarts.
"""

import json
import os
import random
import uuid
from pathlib import Path

STATE_FILE = Path(os.environ.get("ACCOUNT_STATE_PATH", "/data/accounts.json"))

NUM_ACCOUNTS = int(os.environ.get("NUM_ACCOUNTS", "300"))

# Realistic-ish NGN balance distribution: most accounts low/mid balance,
# a smaller number of higher-balance accounts.
def _generate_starting_balance() -> float:
    tier = random.choices(
        population=["low", "mid", "high"],
        weights=[0.55, 0.35, 0.10],
        k=1,
    )[0]
    if tier == "low":
        return round(random.uniform(5_000, 100_000), 2)
    elif tier == "mid":
        return round(random.uniform(100_000, 750_000), 2)
    else:
        return round(random.uniform(750_000, 2_000_000), 2)


def _generate_account_id() -> str:
    # 10-digit NUBAN-style account number, as used by Nigerian banks
    return "".join(str(random.randint(0, 9)) for _ in range(10))


class AccountPool:
    """
    Holds account_id -> balance, with load/save to a JSON file
    so state persists across producer restarts.
    """

    def __init__(self):
        self.balances: dict[str, float] = {}
        self._load_or_create()

    def _load_or_create(self):
        if STATE_FILE.exists():
            with open(STATE_FILE, "r") as f:
                self.balances = json.load(f)
            print(f"[accounts] Loaded {len(self.balances)} existing accounts from {STATE_FILE}")
        else:
            for _ in range(NUM_ACCOUNTS):
                acc_id = _generate_account_id()
                while acc_id in self.balances:
                    acc_id = _generate_account_id()
                self.balances[acc_id] = _generate_starting_balance()
            self._save()
            print(f"[accounts] Created {len(self.balances)} new accounts, saved to {STATE_FILE}")

    def _save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(self.balances, f, indent=2)

    def random_account(self) -> str:
        return random.choice(list(self.balances.keys()))

    def random_account_excluding(self, exclude: str) -> str:
        choices = [a for a in self.balances if a != exclude]
        return random.choice(choices)

    def get_balance(self, account_id: str) -> float:
        return self.balances.get(account_id, 0.0)

    def deposit(self, account_id: str, amount: float):
        self.balances[account_id] = round(self.balances.get(account_id, 0.0) + amount, 2)
        self._save()

    def withdraw(self, account_id: str, amount: float) -> bool:
        """Returns True if withdrawal succeeded, False if insufficient funds."""
        current = self.balances.get(account_id, 0.0)
        if current < amount:
            return False
        self.balances[account_id] = round(current - amount, 2)
        self._save()
        return True

    def transfer(self, from_account: str, to_account: str, amount: float) -> bool:
        """Returns True if transfer succeeded, False if insufficient funds."""
        current = self.balances.get(from_account, 0.0)
        if current < amount:
            return False
        self.balances[from_account] = round(current - amount, 2)
        self.balances[to_account] = round(self.balances.get(to_account, 0.0) + amount, 2)
        self._save()
        return True
