"""
Marathon analytics streaming consumer.

Reads runner checkpoint events from a Kafka topic, maintains race aggregates
in memory, and persists both the raw event log and the aggregates to SQLite.

Tables written:
  raw_events             append-only log of every event received
  leaderboard            fastest runner observed at each checkpoint
  checkpoint_throughput  number of runners crossing each checkpoint per
                         five-minute window
  segment_pace           rolling average pace across the most recent runners
                         for each segment between checkpoints
  completion             total runners who have crossed each checkpoint
  pace_ratio             classification of finishers by whether their second
                         half was faster than, equal to, or slower than their
                         first half
  cohort_pace            average pace per segment grouped by runner cohort
  quality_issues         events flagged as physically implausible

Duplicate events are ignored on raw_events via a uniqueness constraint, and
aggregate tables are updated using upsert statements.
"""
import json
import sqlite3
import time
from collections import defaultdict, deque

from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP, TOPIC, DB_PATH, RACE_ID,
    SEGMENT_ORDER, CHECKPOINTS,
    EVEN_SPLIT_TOLERANCE, COHORTS,
    ANOMALY_FAST_PACE_SEC_PER_KM, ANOMALY_SLOW_PACE_SEC_PER_KM,
)

# How often aggregates are written to the database
FLUSH_INTERVAL = 2.0

# Width of the throughput tumbling window in seconds
THROUGHPUT_WINDOW_SIZE = 300

# Number of recent runners included in each segment's rolling pace average
PACE_ROLLING_N = 50

# Distance of the first half of the course in kilometers
FIRST_HALF_KM = CHECKPOINTS["Half"]

# Distance from the half marathon mark to the 40K timing mat
SECOND_SPAN_KM = 40.0 - FIRST_HALF_KM


# ---------- Database setup ----------

def init_db():
    """Open a connection to the SQLite database and create any missing tables."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS raw_events (
        race_id TEXT, runner_id TEXT, runner_name TEXT,
        checkpoint_label TEXT, checkpoint_km REAL,
        elapsed_seconds INTEGER, event_timestamp REAL, replay_timestamp REAL,
        ingested_at REAL,
        UNIQUE(race_id, runner_id, checkpoint_label)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_label ON raw_events(checkpoint_label);

    CREATE TABLE IF NOT EXISTS leaderboard (
        race_id TEXT, checkpoint_label TEXT, checkpoint_km REAL,
        runner_id TEXT, runner_name TEXT,
        elapsed_seconds INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, checkpoint_label)
    );

    CREATE TABLE IF NOT EXISTS checkpoint_throughput (
        race_id TEXT, checkpoint_label TEXT, window_start INTEGER,
        runner_count INTEGER,
        PRIMARY KEY (race_id, checkpoint_label, window_start)
    );

    CREATE TABLE IF NOT EXISTS segment_pace (
        race_id TEXT, segment TEXT,
        avg_pace_sec_per_km REAL, sample_size INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, segment)
    );

    CREATE TABLE IF NOT EXISTS completion (
        race_id TEXT, checkpoint_label TEXT, checkpoint_km REAL,
        runner_count INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, checkpoint_label)
    );

    CREATE TABLE IF NOT EXISTS pace_ratio (
        race_id TEXT, category TEXT,
        runner_count INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, category)
    );

    CREATE TABLE IF NOT EXISTS cohort_pace (
        race_id TEXT, cohort TEXT, segment TEXT,
        avg_pace_sec_per_km REAL, runner_count INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, cohort, segment)
    );

    CREATE TABLE IF NOT EXISTS quality_issues (
        race_id TEXT, runner_id TEXT, runner_name TEXT,
        segment TEXT, pace_sec_per_km REAL, reason TEXT, detected_at REAL
    );
    CREATE INDEX IF NOT EXISTS idx_issues_reason ON quality_issues(reason);
    """)
    conn.commit()
    return conn


def cohort_for_pace(pace_sec_per_km: float):
    """Return the cohort name for a given pace, or None if the pace falls
    outside every defined cohort range."""
    for name, lo, hi in COHORTS:
        if lo <= pace_sec_per_km < hi:
            return name
    return None


# ---------- In-memory state ----------

class State:
    """Holds all aggregate state for the current consumer process.

    Each instance accumulates results across events and is written to SQLite
    by the flush function on a fixed interval.
    """

    def __init__(self):
        # Fastest runner seen at each checkpoint, keyed by (race, checkpoint label)
        self.leaders = {}

        # Number of crossings per (race, checkpoint, time window)
        self.throughput = defaultdict(int)

        # Bounded queue of recent segment paces, keyed by (race, segment name)
        self.segment_paces = defaultdict(
            lambda: deque(maxlen=PACE_ROLLING_N)
        )

        # All splits seen so far for each runner, keyed by runner id
        self.runner_splits = {}

        # Total runners past each checkpoint, keyed by (race, checkpoint label)
        self.completion = defaultdict(int)

        # Pace-ratio classification recorded once per runner at the 40K mark
        self.pace_ratio_class = {}
        self.pace_ratio_counts = defaultdict(int)

        # Cohort assigned to each runner at their 5K split, plus the running
        # sum and count used to compute average pace per (cohort, segment)
        self.runner_cohort = {}
        self.cohort_pace_agg = defaultdict(lambda: [0.0, 0])

        # Anomalies waiting to be written, plus a running total that survives flushes
        self.pending_issues = []
        self.total_issues_flagged = 0

    def update(self, ev):
        """Apply a single checkpoint event to all in-memory aggregates."""
        race = ev["race_id"]
        rid = ev["runner_id"]
        label = ev["checkpoint_label"]
        km = ev["checkpoint_km"]
        elapsed = ev["elapsed_seconds"]

        # Update the fastest runner seen at this checkpoint
        lb_key = (race, label)
        cur = self.leaders.get(lb_key)
        if cur is None or elapsed < cur["elapsed_seconds"]:
            self.leaders[lb_key] = {
                "runner_id": rid,
                "runner_name": ev["runner_name"],
                "elapsed_seconds": elapsed,
                "km": km,
            }

        # Increment the count for the throughput window this event falls into
        window_start = int(
            (ev["event_timestamp"] // THROUGHPUT_WINDOW_SIZE) * THROUGHPUT_WINDOW_SIZE
        )
        self.throughput[(race, label, window_start)] += 1

        # Increment the total number of runners past this checkpoint
        self.completion[(race, label)] += 1

        # Compute this runner's pace for the segment ending at this checkpoint.
        # The result is reused below for the rolling average, the cohort
        # aggregation, and anomaly detection.
        splits = self.runner_splits.setdefault(rid, {})
        idx = SEGMENT_ORDER.index(label)

        segment_pace = None
        segment_name = None
        if idx > 0:
            prev_label = SEGMENT_ORDER[idx - 1]
            prev_elapsed = splits.get(prev_label)
            if prev_elapsed is not None:
                dist = km - CHECKPOINTS[prev_label]
                dtime = elapsed - prev_elapsed
                if dist > 0 and dtime > 0:
                    segment_pace = dtime / dist
                    segment_name = f"{prev_label}->{label}"
                    self.segment_paces[(race, segment_name)].append(segment_pace)

        # Flag this segment if its pace falls outside the plausible range
        if segment_pace is not None:
            reason = None
            if segment_pace < ANOMALY_FAST_PACE_SEC_PER_KM:
                reason = "impossibly_fast"
            elif segment_pace > ANOMALY_SLOW_PACE_SEC_PER_KM:
                reason = "impossibly_slow"
            if reason is not None:
                self.pending_issues.append({
                    "race_id": race,
                    "runner_id": rid,
                    "runner_name": ev["runner_name"],
                    "segment": segment_name,
                    "pace": segment_pace,
                    "reason": reason,
                })
                self.total_issues_flagged += 1

        # Assign this runner to a cohort the first time their 5K split is seen
        if label == "5K":
            first_5k_pace = elapsed / CHECKPOINTS["5K"]
            cohort = cohort_for_pace(first_5k_pace)
            if cohort is not None:
                self.runner_cohort[rid] = cohort

        # Add this segment's pace to its cohort's running average, but only if
        # the pace is within the plausible range so anomalies do not skew it
        if segment_pace is not None and segment_name is not None:
            cohort = self.runner_cohort.get(rid)
            if cohort is not None and (
                ANOMALY_FAST_PACE_SEC_PER_KM
                <= segment_pace
                <= ANOMALY_SLOW_PACE_SEC_PER_KM
            ):
                agg = self.cohort_pace_agg[(cohort, segment_name)]
                agg[0] += segment_pace
                agg[1] += 1

        # Store this split so later checkpoints can compute their own segments
        splits[label] = elapsed

        # Once a runner reaches 40K, compare their first-half pace to their
        # second-span pace and classify the result. Each runner is classified
        # only once.
        if label == "40K" and rid not in self.pace_ratio_class:
            first_half_elapsed = splits.get("Half")
            if first_half_elapsed is not None and first_half_elapsed > 0:
                first_half_pace = first_half_elapsed / FIRST_HALF_KM
                second_span_time = elapsed - first_half_elapsed
                if second_span_time > 0:
                    second_span_pace = second_span_time / SECOND_SPAN_KM
                    ratio = (second_span_pace - first_half_pace) / first_half_pace

                    if abs(ratio) <= EVEN_SPLIT_TOLERANCE:
                        category = "even"
                    elif ratio < 0:
                        category = "faster_second"
                    else:
                        category = "slower_second"

                    self.pace_ratio_class[rid] = category
                    self.pace_ratio_counts[category] += 1


# ---------- Database flush ----------

def flush(conn, state: State):
    """Write the current in-memory aggregates to SQLite using upserts."""
    now = time.time()
    c = conn.cursor()

    for (race, label), v in state.leaders.items():
        c.execute("""
            INSERT INTO leaderboard VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(race_id, checkpoint_label) DO UPDATE SET
                checkpoint_km = excluded.checkpoint_km,
                runner_id = excluded.runner_id,
                runner_name = excluded.runner_name,
                elapsed_seconds = excluded.elapsed_seconds,
                updated_at = excluded.updated_at
        """, (race, label, v["km"], v["runner_id"], v["runner_name"],
              v["elapsed_seconds"], now))

    for (race, label, ws), count in state.throughput.items():
        c.execute("""
            INSERT INTO checkpoint_throughput VALUES (?,?,?,?)
            ON CONFLICT(race_id, checkpoint_label, window_start) DO UPDATE SET
                runner_count = excluded.runner_count
        """, (race, label, ws, count))

    for (race, seg), samples in state.segment_paces.items():
        if not samples:
            continue
        avg = sum(samples) / len(samples)
        c.execute("""
            INSERT INTO segment_pace VALUES (?,?,?,?,?)
            ON CONFLICT(race_id, segment) DO UPDATE SET
                avg_pace_sec_per_km = excluded.avg_pace_sec_per_km,
                sample_size = excluded.sample_size,
                updated_at = excluded.updated_at
        """, (race, seg, avg, len(samples), now))

    for (race, label), count in state.completion.items():
        km = state.leaders.get((race, label), {}).get(
            "km", CHECKPOINTS.get(label, 0.0)
        )
        c.execute("""
            INSERT INTO completion VALUES (?,?,?,?,?)
            ON CONFLICT(race_id, checkpoint_label) DO UPDATE SET
                checkpoint_km = excluded.checkpoint_km,
                runner_count = excluded.runner_count,
                updated_at = excluded.updated_at
        """, (race, label, km, count, now))

    for category, count in state.pace_ratio_counts.items():
        c.execute("""
            INSERT INTO pace_ratio VALUES (?,?,?,?)
            ON CONFLICT(race_id, category) DO UPDATE SET
                runner_count = excluded.runner_count,
                updated_at = excluded.updated_at
        """, (RACE_ID, category, count, now))

    for (cohort, segment), (total, n) in state.cohort_pace_agg.items():
        if n == 0:
            continue
        avg = total / n
        c.execute("""
            INSERT INTO cohort_pace VALUES (?,?,?,?,?,?)
            ON CONFLICT(race_id, cohort, segment) DO UPDATE SET
                avg_pace_sec_per_km = excluded.avg_pace_sec_per_km,
                runner_count = excluded.runner_count,
                updated_at = excluded.updated_at
        """, (RACE_ID, cohort, segment, avg, n, now))

    if state.pending_issues:
        c.executemany("""
            INSERT INTO quality_issues
                (race_id, runner_id, runner_name, segment, pace_sec_per_km, reason, detected_at)
            VALUES (?,?,?,?,?,?,?)
        """, [(i["race_id"], i["runner_id"], i["runner_name"],
               i["segment"], i["pace"], i["reason"], now)
              for i in state.pending_issues])
        state.pending_issues.clear()

    conn.commit()


def main():
    """Run the consumer loop until interrupted."""
    conn = init_db()
    state = State()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="marathon-analytics",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    last_flush = time.time()
    processed = 0

    print(f"consumer=started group=marathon-analytics topic={TOPIC}")
    try:
        for msg in consumer:
            ev = msg.value
            state.update(ev)

            conn.execute("""
                INSERT OR IGNORE INTO raw_events VALUES (?,?,?,?,?,?,?,?,?)
            """, (ev["race_id"], ev["runner_id"], ev["runner_name"],
                  ev["checkpoint_label"], ev["checkpoint_km"],
                  ev["elapsed_seconds"], ev["event_timestamp"],
                  ev["replay_timestamp"], time.time()))

            processed += 1
            if time.time() - last_flush >= FLUSH_INTERVAL:
                flush(conn, state)
                last_flush = time.time()

                half_leader = state.leaders.get((RACE_ID, "Half"))
                half_str = (
                    f"{half_leader['runner_name']}({half_leader['elapsed_seconds']}s)"
                    if half_leader else "none"
                )
                print(
                    f"processed={processed} "
                    f"half_leader={half_str} "
                    f"pace_ratio={dict(state.pace_ratio_counts)} "
                    f"issues_total={state.total_issues_flagged}"
                )
    except KeyboardInterrupt:
        print("consumer=interrupted flushing_final_state")
    finally:
        if processed > 0:
            flush(conn, state)
        print(f"consumer=stopped processed={processed}")
        conn.close()


if __name__ == "__main__":
    main()