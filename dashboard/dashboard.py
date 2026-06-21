"""
dashboard.py

Streamlit dashboard reading from the four Gold tables, with three tabs:
  - Overview: headline metrics, daily volume trend, transaction type split
  - Accounts: searchable balance table, top accounts by activity
  - Fraud Monitoring: daily fraud rate trend, recent flagged transactions,
    breakdown by which rule triggered

Each tab is built as a separate @st.fragment(run_every=...) so it
auto-refreshes independently on its own timer, reflecting the fact that
every layer feeding Gold is a continuously running pipeline.
"""

import os

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

# --- Configuration ---
DAILY_VOLUME_PATH = os.environ.get("DAILY_VOLUME_PATH", "/data/gold/daily_volume")
ACCOUNT_BALANCES_PATH = os.environ.get("ACCOUNT_BALANCES_PATH", "/data/gold/account_balances")
DAILY_FRAUD_RATE_PATH = os.environ.get("DAILY_FRAUD_RATE_PATH", "/data/gold/daily_fraud_rate")
TOP_ACCOUNTS_PATH = os.environ.get("TOP_ACCOUNTS_PATH", "/data/gold/top_accounts")
FLAGGED_TABLE_PATH = os.environ.get("FLAGGED_TABLE_PATH", "/data/fraud/flagged")

REFRESH_INTERVAL_SECONDS = int(os.environ.get("REFRESH_INTERVAL_SECONDS", "15"))

st.set_page_config(page_title="Ledger Pipeline Dashboard", layout="wide")

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
    --bg: #0B0E14;
    --surface: #141925;
    --surface-border: #232938;
    --text-primary: #E8EAED;
    --text-muted: #8B93A7;
    --accent: #00C076;
    --danger: #FF5C5C;
}

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background-color: var(--bg);
    color: var(--text-primary);
}

/* Headers use Inter, tight tracking, accent underline on h2/h3 for section markers */
h1, h2, h3 {
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    letter-spacing: -0.01em;
}

h1 {
    color: var(--text-primary);
    border-bottom: 2px solid var(--accent);
    padding-bottom: 0.4rem;
    display: inline-block;
}

h2, h3 {
    color: var(--text-primary);
}

/* All numeric/metric content in monospace for ledger-like precision */
[data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    color: var(--accent);
    overflow: visible;
    white-space: nowrap;
    font-size: clamp(1.1rem, 2.2vw, 1.8rem);
}

[data-testid="stMetricLabel"] {
    font-family: 'Inter', sans-serif;
    color: var(--text-muted);
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

[data-testid="stMetric"] {
    background-color: var(--surface);
    border: 1px solid var(--surface-border);
    border-radius: 6px;
    padding: 1rem 1.2rem;
}

/* Tabs: quiet by default, accent underline on active tab, no emoji-driven decoration */
.stTabs [data-baseweb="tab-list"] {
    gap: 0.5rem;
    border-bottom: 1px solid var(--surface-border);
}

.stTabs [data-baseweb="tab"] {
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    color: var(--text-muted);
    background-color: transparent;
}

.stTabs [aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
}

/* Dataframes / tables: monospace numbers, dark surface */
[data-testid="stDataFrame"] {
    font-family: 'JetBrains Mono', monospace;
}

/* Captions (the "Last refreshed" lines) stay quiet/muted */
.stCaption, [data-testid="stCaptionContainer"] {
    color: var(--text-muted) !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
}

/* Text input search box */
.stTextInput input {
    background-color: var(--surface);
    border: 1px solid var(--surface-border);
    color: var(--text-primary);
    font-family: 'JetBrains Mono', monospace;
}
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def safe_load_table(path: str) -> pd.DataFrame:
    """Returns an empty DataFrame instead of crashing if a Gold table isn't ready yet."""
    try:
        if not DeltaTable.is_deltatable(path):
            return pd.DataFrame()
        return DeltaTable(path).to_pyarrow_table().to_pandas()
    except Exception as e:
        st.warning(f"Could not load table at {path}: {e}")
        return pd.DataFrame()


def format_naira(value: float) -> str:
    return f"₦{value:,.2f}"


def format_naira_compact(value: float) -> str:
    """
    Compact form for metric cards, where space is tight and full precision
    isn't the point — e.g. ₦270.6M instead of ₦270,643,xxx.xx.
    """
    abs_value = abs(value)
    sign = "-" if value < 0 else ""
    if abs_value >= 1_000_000_000:
        return f"{sign}₦{abs_value / 1_000_000_000:,.2f}B"
    if abs_value >= 1_000_000:
        return f"{sign}₦{abs_value / 1_000_000:,.2f}M"
    if abs_value >= 1_000:
        return f"{sign}₦{abs_value / 1_000:,.1f}K"
    return f"{sign}₦{abs_value:,.2f}"


# ============================================================
# OVERVIEW TAB
# ============================================================

@st.fragment(run_every=REFRESH_INTERVAL_SECONDS)
def render_overview():
    daily_volume = safe_load_table(DAILY_VOLUME_PATH)
    balances = safe_load_table(ACCOUNT_BALANCES_PATH)
    fraud_rate = safe_load_table(DAILY_FRAUD_RATE_PATH)

    if daily_volume.empty or balances.empty or fraud_rate.empty:
        st.info("Waiting for pipeline data...")
        return

    daily_volume = daily_volume.sort_values("date")
    latest_day = daily_volume.iloc[-1]
    latest_fraud = fraud_rate.sort_values("date").iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Accounts", f"{len(balances):,}")
    col2.metric(
        "Total Balance in System",
        format_naira_compact(balances["computed_balance"].sum()),
        help=format_naira(balances["computed_balance"].sum()),
    )
    col3.metric(f"Transactions ({latest_day['date']})", f"{int(latest_day['total_transaction_count']):,}")
    col4.metric(f"Fraud Rate ({latest_fraud['date']})", f"{latest_fraud['fraud_rate_pct']:.2f}%")

    st.subheader("Daily Transaction Volume")
    st.bar_chart(daily_volume.set_index("date")["total_amount"])

    st.subheader("Transaction Type Breakdown (Total)")
    type_totals = pd.DataFrame({
        "type": ["Deposit", "Withdrawal", "Transfer"],
        "count": [
            daily_volume["deposit_count"].sum(),
            daily_volume["withdrawal_count"].sum(),
            daily_volume["transfer_count"].sum(),
        ],
        "amount": [
            daily_volume["deposit_amount"].sum(),
            daily_volume["withdrawal_amount"].sum(),
            daily_volume["transfer_amount"].sum(),
        ],
    })
    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("By count")
        st.bar_chart(type_totals.set_index("type")["count"])
    with col_b:
        st.caption("By amount (₦)")
        st.bar_chart(type_totals.set_index("type")["amount"])

    st.caption(f"Last refreshed: {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}")


# ============================================================
# ACCOUNTS TAB
# ============================================================

@st.fragment(run_every=REFRESH_INTERVAL_SECONDS)
def render_accounts():
    balances = safe_load_table(ACCOUNT_BALANCES_PATH)
    top_accounts = safe_load_table(TOP_ACCOUNTS_PATH)

    if balances.empty:
        st.info("Waiting for pipeline data...")
        return

    st.subheader("Top 10 Accounts by Activity")
    if not top_accounts.empty:
        top10 = top_accounts.sort_values("rank").head(10)
        st.bar_chart(top10.set_index("account_id")["total_amount"])

    st.subheader("All Account Balances")
    search = st.text_input("Search by account ID", key="account_search")
    display_df = balances[["account_id", "computed_balance", "completed_transaction_count"]].copy()
    display_df.columns = ["Account ID", "Balance (₦)", "Completed Transactions"]
    display_df = display_df.sort_values("Balance (₦)", ascending=False)

    if search:
        display_df = display_df[display_df["Account ID"].str.contains(search, na=False)]

    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.caption(f"Last refreshed: {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}")


# ============================================================
# FRAUD MONITORING TAB
# ============================================================

@st.fragment(run_every=REFRESH_INTERVAL_SECONDS)
def render_fraud():
    fraud_rate = safe_load_table(DAILY_FRAUD_RATE_PATH)
    flagged = safe_load_table(FLAGGED_TABLE_PATH)

    if fraud_rate.empty:
        st.info("Waiting for pipeline data...")
        return

    fraud_rate = fraud_rate.sort_values("date")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Daily Fraud Rate (%)")
        st.line_chart(fraud_rate.set_index("date")["fraud_rate_pct"])
    with col2:
        st.subheader("Daily Flagged Count")
        st.bar_chart(fraud_rate.set_index("date")["flagged_count"])

    if not flagged.empty:
        st.subheader("Flagged Transactions by Rule")
        # fraud_reasons is a "; "-joined string of one or more reasons per row;
        # split and count each rule type separately so a row tripping 2 rules
        # counts toward both.
        rule_counts = {}
        for reasons_str in flagged["fraud_reasons"].dropna():
            for reason in reasons_str.split(";"):
                rule_name = reason.split(":")[0].strip()
                rule_counts[rule_name] = rule_counts.get(rule_name, 0) + 1
        if rule_counts:
            rule_df = pd.DataFrame(list(rule_counts.items()), columns=["Rule", "Count"]).sort_values("Count", ascending=False)
            st.bar_chart(rule_df.set_index("Rule")["Count"])

        st.subheader("Most Recent Flagged Transactions")
        recent = flagged.sort_values("flagged_at", ascending=False).head(25)
        display_cols = ["transaction_id", "account_id", "transaction_type", "amount", "fraud_score", "fraud_reasons", "flagged_at"]
        st.dataframe(recent[display_cols], use_container_width=True, hide_index=True)

    st.caption(f"Last refreshed: {pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d %H:%M:%S UTC')}")


# ============================================================
# MAIN LAYOUT
# ============================================================

st.title("Ledger Pipeline")
st.caption("Live transaction monitoring — Kafka → Bronze → Silver → Fraud Detection → Gold")

tab1, tab2, tab3 = st.tabs(["Overview", "Accounts", "Fraud Monitoring"])

with tab1:
    render_overview()

with tab2:
    render_accounts()

with tab3:
    render_fraud()