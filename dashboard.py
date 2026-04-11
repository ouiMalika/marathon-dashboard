"""
dashboard.py
------------
Tiny live dashboard. Polls SQLite every 2 seconds and shows the four metrics.
Run with:  streamlit run dashboard.py
"""
import sqlite3
import time
import pandas as pd
import streamlit as st
from config import DB_PATH

st.set_page_config(page_title="Marathon Live Analytics", layout="wide")
st.title("🏃 Marathon Live Analytics")
st.caption("Streaming pipeline: Kafka → Python consumer → SQLite")

placeholder = st.empty()


def fetch(q):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(q, conn)


def fmt_elapsed(s):
    s = int(s)
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def fmt_pace(s):
    return f"{int(s // 60)}:{int(s % 60):02d}"


while True:
    with placeholder.container():
        try:
            total = fetch("SELECT COUNT(*) AS n FROM raw_events").iloc[0]["n"]
        except Exception:
            st.warning("DB not ready yet — is the consumer running?")
            time.sleep(2)
            continue

        comp = fetch("SELECT checkpoint_label, checkpoint_km, runner_count "
                     "FROM completion ORDER BY checkpoint_km")

        c1, c2 = st.columns(2)
        c1.metric("Events processed", f"{total:,}")
        if not comp.empty:
            past_40k = comp.loc[comp["checkpoint_label"] == "40K", "runner_count"].sum()
            c2.metric("Runners past 40K", int(past_40k))

        st.subheader("🏆 Current leader per checkpoint")
        lb = fetch("""SELECT checkpoint_label, checkpoint_km, runner_name, runner_id,
                             elapsed_seconds FROM leaderboard
                      ORDER BY checkpoint_km""")
        if not lb.empty:
            lb = lb.copy()
            lb["elapsed"] = lb["elapsed_seconds"].map(fmt_elapsed)
            st.dataframe(
                lb[["checkpoint_label", "runner_name", "runner_id", "elapsed"]],
                use_container_width=True, hide_index=True,
            )

        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("📊 Runners crossed per checkpoint")
            if not comp.empty:
                st.dataframe(
                    comp[["checkpoint_label", "runner_count"]],
                    use_container_width=True, hide_index=True,
                )

        with col_b:
            st.subheader("⏱️ Rolling avg pace per segment (sec/km)")
            pace = fetch("SELECT segment, avg_pace_sec_per_km, sample_size "
                         "FROM segment_pace")
            if not pace.empty:
                pace = pace.copy()
                pace["min/km"] = pace["avg_pace_sec_per_km"].map(fmt_pace)
                st.dataframe(
                    pace[["segment", "min/km", "sample_size"]],
                    use_container_width=True, hide_index=True,
                )

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")
    time.sleep(2)