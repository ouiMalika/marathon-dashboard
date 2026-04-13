"""Streamlit dashboard for the marathon analytics pipeline.

Polls the SQLite database every two seconds and renders the latest
aggregates produced by the consumer. Read-only: no data is written.

Run with:
    streamlit run dashboard.py
"""
import sqlite3
import time

import pandas as pd
import streamlit as st

from config import DB_PATH

REFRESH_SECONDS = 2

st.set_page_config(page_title="Marathon Live Analytics", layout="wide")
st.title("🏃 Marathon Live Analytics")
st.caption("Streaming pipeline: Kafka → Python consumer → SQLite")

placeholder = st.empty()


def fetch(query: str) -> pd.DataFrame:
    """Run a read-only query against the SQLite database and return a DataFrame."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)


def format_elapsed(seconds: int) -> str:
    """Format a number of seconds as H:MM:SS."""
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}:{minutes:02d}:{seconds % 60:02d}"


def format_pace(seconds_per_km: float) -> str:
    """Format a pace in seconds per kilometer as M:SS."""
    return f"{int(seconds_per_km // 60)}:{int(seconds_per_km % 60):02d}"


while True:
    with placeholder.container():
        try:
            total_events = fetch("SELECT COUNT(*) AS n FROM raw_events").iloc[0]["n"]
        except Exception:
            st.warning("Database is not ready yet. Make sure the consumer is running.")
            time.sleep(REFRESH_SECONDS)
            continue

        completion = fetch(
            "SELECT checkpoint_label, checkpoint_km, runner_count "
            "FROM completion ORDER BY checkpoint_km"
        )

        # Top-level summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Events processed", f"{total_events:,}")

        if not completion.empty:
            past_40k = int(
                completion.loc[
                    completion["checkpoint_label"] == "40K", "runner_count"
                ].sum()
            )
            col2.metric("Runners past 40K", past_40k)

        try:
            issues_total = fetch(
                "SELECT COUNT(*) AS n FROM quality_issues"
            ).iloc[0]["n"]
            col3.metric("Quality issues flagged", f"{int(issues_total):,}")
        except Exception:
            pass

        try:
            finishers = fetch(
                "SELECT SUM(runner_count) AS n FROM pace_ratio"
            ).iloc[0]["n"]
            col4.metric("Finishers classified", f"{int(finishers or 0):,}")
        except Exception:
            pass

        # Fastest runner observed at each checkpoint
        st.subheader("🏆 Current leader per checkpoint")
        leaderboard = fetch(
            """SELECT checkpoint_label, checkpoint_km, runner_name, runner_id,
                      elapsed_seconds
               FROM leaderboard
               ORDER BY checkpoint_km"""
        )
        if not leaderboard.empty:
            leaderboard = leaderboard.copy()
            leaderboard["elapsed"] = leaderboard["elapsed_seconds"].map(format_elapsed)
            st.dataframe(
                leaderboard[["checkpoint_label", "runner_name", "runner_id", "elapsed"]],
                use_container_width=True,
                hide_index=True,
            )

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("📊 Runners crossed per checkpoint")
            if not completion.empty:
                st.dataframe(
                    completion[["checkpoint_label", "runner_count"]],
                    use_container_width=True,
                    hide_index=True,
                )
        with col_right:
            st.subheader("⏱️ Rolling average pace per segment")
            segment_pace = fetch(
                "SELECT segment, avg_pace_sec_per_km, sample_size FROM segment_pace"
            )
            if not segment_pace.empty:
                segment_pace = segment_pace.copy()
                segment_pace["min/km"] = segment_pace["avg_pace_sec_per_km"].map(
                    format_pace
                )
                st.dataframe(
                    segment_pace[["segment", "min/km", "sample_size"]],
                    use_container_width=True,
                    hide_index=True,
                )

        # Classification of finishers by whether their second half was faster,
        # equal to, or slower than their first half
        st.subheader("🎯 Pace ratio: second-span vs first-half (finishers only)")
        ratios = fetch("SELECT category, runner_count FROM pace_ratio")
        if not ratios.empty:
            ratios = ratios.copy()
            display_order = {"faster_second": 0, "even": 1, "slower_second": 2}
            ratios["order"] = ratios["category"].map(display_order)
            ratios = ratios.sort_values("order").drop(columns="order")
            total_classified = ratios["runner_count"].sum()
            ratios["pct"] = (ratios["runner_count"] / total_classified * 100).round(1)
            st.dataframe(ratios, use_container_width=True, hide_index=True)
            st.caption(
                "faster_second: the runner's Half to 40K pace was faster than "
                "their 0 to Half pace. even: within one percent. "
                "slower_second: the runner slowed down in the second span."
            )
        else:
            st.caption("Waiting for runners to cross the 40K mark.")

        # Average pace per segment grouped by runner cohort
        st.subheader("📉 Pace decay by cohort (minutes per kilometer)")
        cohort_pace = fetch(
            """SELECT cohort, segment, avg_pace_sec_per_km, runner_count
               FROM cohort_pace"""
        )
        if not cohort_pace.empty:
            cohort_pace = cohort_pace.copy()
            cohort_pace["min/km"] = cohort_pace["avg_pace_sec_per_km"].map(format_pace)
            pivot = cohort_pace.pivot_table(
                index="cohort",
                columns="segment",
                values="min/km",
                aggfunc="first",
            )
            segment_display_order = [
                "5K->10K", "10K->15K", "15K->20K", "20K->Half", "Half->25K",
                "25K->30K", "30K->35K", "35K->40K",
            ]
            pivot = pivot[[s for s in segment_display_order if s in pivot.columns]]
            cohort_display_order = [
                "elite", "competitive", "recreational", "back_of_pack",
            ]
            pivot = pivot.reindex(
                [c for c in cohort_display_order if c in pivot.index]
            )
            st.dataframe(pivot, use_container_width=True)
            st.caption(
                "Each row shows one cohort's average pace across the race, "
                "left to right. Flat rows indicate consistent pacing; rows "
                "that grow rightward indicate fatigue."
            )
        else:
            st.caption("Cohort statistics will appear once enough runners have crossed the 5K mark.")

        # Events flagged as physically implausible
        st.subheader("🚨 Data quality issues")
        issue_counts = fetch(
            "SELECT reason, COUNT(*) AS n FROM quality_issues GROUP BY reason"
        )
        if not issue_counts.empty:
            st.dataframe(issue_counts, use_container_width=True, hide_index=True)
            recent_issues = fetch(
                """SELECT runner_name, runner_id, segment,
                          ROUND(pace_sec_per_km, 1) AS pace_sec_per_km, reason
                   FROM quality_issues
                   ORDER BY detected_at DESC
                   LIMIT 5"""
            )
            if not recent_issues.empty:
                st.caption("Most recent flagged events:")
                st.dataframe(
                    recent_issues, use_container_width=True, hide_index=True
                )
        else:
            st.caption("No anomalies detected yet.")

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")

    time.sleep(REFRESH_SECONDS)