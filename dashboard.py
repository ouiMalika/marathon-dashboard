"""Streamlit dashboard for the marathon analytics pipeline.

Renders two sections:

  Live race monitor: side-by-side view of each race year as the producer
  streams events into Kafka and the consumer aggregates them. Updates every
  two seconds. Displays a race clock derived from the most recent event
  timestamp processed by the consumer.

  Year-over-year analysis: comparative analytics that answer five questions
  about how the Boston Marathon field has changed across race years.

Read-only against the SQLite database produced by consumer.py.

Run with:
    streamlit run dashboard.py
"""
import sqlite3
import time

import pandas as pd
import streamlit as st

from config import DB_PATH, RACE_START_EPOCH, SPEEDUP

REFRESH_SECONDS = 2

# Display order for cohorts and segments, used to sort dashboard tables
COHORT_DISPLAY_ORDER = ["elite", "competitive", "recreational", "back_of_pack"]
SEGMENT_DISPLAY_ORDER = [
    "5K->10K", "10K->15K", "15K->20K", "20K->Half", "Half->25K",
    "25K->30K", "30K->35K", "35K->40K",
]

# Approximate full marathon time in seconds, used to display race progress
RACE_DURATION_SECONDS = 4 * 3600 + 30 * 60  # 4h 30m, covers the back of the field

st.set_page_config(page_title="Marathon Analytics", layout="wide")
st.title("🏃 Boston Marathon Analytics")
st.caption(
    "Streaming pipeline replaying three years of race data through Kafka, "
    "aggregated in real time and compared year over year."
)

placeholder = st.empty()


def fetch(query: str, params: tuple = ()) -> pd.DataFrame:
    """Run a read-only query against the SQLite database and return a DataFrame."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        return pd.read_sql_query(query, conn, params=params)


def format_elapsed(seconds) -> str:
    """Format a number of seconds as H:MM:SS."""
    if seconds is None or pd.isna(seconds):
        return "-"
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}:{minutes:02d}:{seconds % 60:02d}"


def format_pace(seconds_per_km) -> str:
    """Format a pace in seconds per kilometer as M:SS."""
    if seconds_per_km is None or pd.isna(seconds_per_km):
        return "-"
    return f"{int(seconds_per_km // 60)}:{int(seconds_per_km % 60):02d}"


def race_label(race_id: str) -> str:
    """Convert an internal race id like 'boston_2015' to a display label."""
    parts = race_id.split("_")
    if len(parts) == 2:
        return f"{parts[0].title()} {parts[1]}"
    return race_id


def fetch_race_clock() -> int:
    """Return the current race clock in seconds.

    The race clock is the largest event_timestamp processed so far, minus the
    synthetic race start epoch. This means it advances as the consumer ingests
    events, not as wall-clock time passes, so it reflects what data is
    actually visible on the dashboard.
    """
    df = fetch("SELECT MAX(event_timestamp) AS t FROM raw_events")
    if df.empty or pd.isna(df.iloc[0]["t"]):
        return 0
    return max(0, int(df.iloc[0]["t"] - RACE_START_EPOCH))


def render_race_clock(race_clock_seconds: int):
    """Render the race clock and progress bar at the top of the dashboard."""
    progress = min(1.0, race_clock_seconds / RACE_DURATION_SECONDS)

    clock_col, info_col = st.columns([1, 2])
    with clock_col:
        st.metric(
            "Race clock",
            format_elapsed(race_clock_seconds),
            help=(
                "Elapsed time since race start, derived from the most recent "
                "event processed by the consumer. Advances in wall-clock "
                f"time multiplied by the SPEEDUP factor (currently {SPEEDUP}x)."
            ),
        )
    with info_col:
        st.markdown("**Race progress**")
        st.progress(progress)
        if race_clock_seconds == 0:
            st.caption("Waiting for the first events.")
        elif progress >= 1.0:
            st.caption("All runners have finished.")
        else:
            st.caption(
                f"Replay running at {SPEEDUP}x speed. "
                f"{format_elapsed(race_clock_seconds)} into the race."
            )


def render_live_race_column(race_id: str, race_clock_seconds: int):
    """Render one column of the live multi-race monitor for a single race."""
    st.markdown(f"### {race_label(race_id)}")

    field = fetch(
        "SELECT runner_count FROM field_size WHERE race_id = ?", (race_id,)
    )
    field_count = int(field.iloc[0]["runner_count"]) if not field.empty else 0

    completion = fetch(
        """SELECT checkpoint_label, runner_count FROM completion
           WHERE race_id = ? ORDER BY checkpoint_km""",
        (race_id,),
    )
    past_40k = 0
    if not completion.empty:
        match = completion.loc[completion["checkpoint_label"] == "40K", "runner_count"]
        if not match.empty:
            past_40k = int(match.iloc[0])

    progress = past_40k / field_count if field_count else 0
    st.metric("Runners in field", f"{field_count:,}")
    st.metric("Past 40K", f"{past_40k:,}", f"{progress * 100:.1f}%")

    leader = fetch(
        """SELECT checkpoint_label, runner_name, elapsed_seconds
           FROM leaderboard WHERE race_id = ?
           ORDER BY checkpoint_km""",
        (race_id,),
    )
    if not leader.empty:
        leader = leader.copy()
        leader.loc[:, "elapsed"] = leader["elapsed_seconds"].map(format_elapsed)
        leader = leader.rename(columns={
            "checkpoint_label": "Checkpoint",
            "runner_name": "Leader",
        })
        st.dataframe(
            leader[["Checkpoint", "Leader", "elapsed"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("No checkpoints crossed yet.")


def render_field_size_section():
    """Question 1: how big was each year's field?"""
    st.markdown("##### How big was each year's field?")
    field = fetch(
        """SELECT race_id, runner_count
           FROM field_size ORDER BY race_id"""
    )
    if field.empty:
        st.caption("No data yet.")
        return
    field = field.copy()
    field.loc[:, "Year"] = field["race_id"].map(race_label)
    field = field.rename(columns={"runner_count": "Runners"})
    st.dataframe(
        field[["Year", "Runners"]],
        use_container_width=True,
        hide_index=True,
    )


def render_finish_time_section():
    """Question 2: did the field get faster?"""
    st.markdown("##### Did the field get faster? (median finish time per cohort)")
    ft = fetch(
        """SELECT race_id, cohort, median_seconds, runner_count
           FROM finish_time"""
    )
    if ft.empty:
        st.caption("Waiting for runners to cross 40K.")
        return

    ft = ft.copy()
    ft.loc[:, "median"] = ft["median_seconds"].map(format_elapsed)
    ft.loc[:, "Year"] = ft["race_id"].map(race_label)

    pivot = ft.pivot_table(
        index="cohort", columns="Year", values="median", aggfunc="first"
    )
    pivot = pivot.reindex([c for c in COHORT_DISPLAY_ORDER if c in pivot.index])
    st.dataframe(pivot, use_container_width=True)
    st.caption(
        "Each cell is the median finish time for that cohort in that year. "
        "Slower years stand out as larger values across every cohort row."
    )


def render_wall_section():
    """Question 3: where does the field hit the wall?"""
    st.markdown(
        "##### Where does the field hit the wall? (recreational cohort pace per segment)"
    )
    cp = fetch(
        """SELECT race_id, segment, avg_pace_sec_per_km
           FROM cohort_pace WHERE cohort = 'recreational'"""
    )
    if cp.empty:
        st.caption("Waiting for cohort statistics to build.")
        return

    cp = cp.copy()
    cp.loc[:, "min/km"] = cp["avg_pace_sec_per_km"].map(format_pace)
    cp.loc[:, "Year"] = cp["race_id"].map(race_label)

    pivot = cp.pivot_table(
        index="Year", columns="segment", values="min/km", aggfunc="first"
    )
    pivot = pivot[[s for s in SEGMENT_DISPLAY_ORDER if s in pivot.columns]]
    st.dataframe(pivot, use_container_width=True)
    st.caption(
        "Segments where pace climbs sharply mark the point in the race where "
        "fatigue takes over. Compare across years to see whether the wall "
        "moves earlier or later from one race to the next."
    )


def render_pace_ratio_section():
    """Question 4: has pacing discipline improved?"""
    st.markdown("##### Has pacing discipline improved? (finishers by pace ratio)")
    ratios = fetch(
        """SELECT race_id, category, runner_count
           FROM pace_ratio"""
    )
    if ratios.empty:
        st.caption("Waiting for runners to cross 40K.")
        return

    ratios = ratios.copy()
    ratios.loc[:, "Year"] = ratios["race_id"].map(race_label)

    totals = ratios.groupby("Year")["runner_count"].transform("sum")
    ratios.loc[:, "pct"] = (ratios["runner_count"] / totals * 100).round(1)
    ratios.loc[:, "value"] = ratios.apply(
        lambda r: f"{r['runner_count']:,} ({r['pct']}%)", axis=1
    )

    pivot = ratios.pivot_table(
        index="category", columns="Year", values="value", aggfunc="first"
    )
    category_order = ["faster_second", "even", "slower_second"]
    pivot = pivot.reindex([c for c in category_order if c in pivot.index])
    st.dataframe(pivot, use_container_width=True)
    st.caption(
        "faster_second runners ran the second span faster than the first half. "
        "A higher faster_second percentage suggests more disciplined pacing "
        "across the field."
    )


def render_quality_section():
    """Question 5: data quality across years."""
    st.markdown("##### How clean was each year's data? (anomalies flagged per race)")
    quality = fetch(
        """SELECT race_id, reason, COUNT(*) AS n
           FROM quality_issues GROUP BY race_id, reason"""
    )
    if quality.empty:
        st.caption("No anomalies detected yet.")
        return

    quality = quality.copy()
    quality.loc[:, "Year"] = quality["race_id"].map(race_label)
    pivot = quality.pivot_table(
        index="reason", columns="Year", values="n", aggfunc="first", fill_value=0
    )
    st.dataframe(pivot, use_container_width=True)
    st.caption(
        "Counts of segment events flagged as physically implausible. "
        "Higher counts in older years often reflect timing-mat hardware that "
        "has since been upgraded."
    )


while True:
    with placeholder.container():
        try:
            total_events = fetch(
                "SELECT COUNT(*) AS n FROM raw_events"
            ).iloc[0]["n"]
        except Exception:
            st.warning("Database is not ready yet. Make sure the consumer is running.")
            time.sleep(REFRESH_SECONDS)
            continue

        race_clock_seconds = fetch_race_clock()

        try:
            races_df = fetch(
                "SELECT DISTINCT race_id FROM field_size ORDER BY race_id"
            )
        except Exception:
            races_df = pd.DataFrame(columns=["race_id"])

        race_ids = races_df["race_id"].tolist()

        # ---------- Race clock ----------
        render_race_clock(race_clock_seconds)

        # ---------- Top-level summary ----------
        col1, col2, col3 = st.columns(3)
        col1.metric("Events processed", f"{int(total_events):,}")
        col2.metric("Race years streaming", len(race_ids))
        try:
            finishers = fetch(
                "SELECT SUM(runner_count) AS n FROM pace_ratio"
            ).iloc[0]["n"]
            col3.metric("Total finishers classified", f"{int(finishers or 0):,}")
        except Exception:
            pass

        # ---------- Section 1: live multi-race monitor ----------
        st.header("Live race monitor")
        st.caption(
            "Each column is one race year, populated as the producer streams "
            "events into Kafka. Numbers update every two seconds."
        )
        if race_ids:
            columns = st.columns(len(race_ids))
            for column, race_id in zip(columns, race_ids):
                with column:
                    render_live_race_column(race_id, race_clock_seconds)
        else:
            st.caption("Waiting for the first events to arrive.")

        # ---------- Section 2: year-over-year analysis ----------
        st.header("Year over year analysis")
        st.caption(
            "Five comparative views computed from the same streaming aggregates. "
            "Every metric is updated incrementally as events flow through the pipeline."
        )

        render_field_size_section()
        render_finish_time_section()
        render_wall_section()
        render_pace_ratio_section()
        render_quality_section()

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")

    time.sleep(REFRESH_SECONDS)