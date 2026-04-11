"""
consumer.py
-----------
Stateful streaming consumer:

  1. Reads events from Kafka.
  2. Writes each event to raw_events.
  3. Maintains in-memory state for four live metrics:
       - leaderboard       (fastest runner per checkpoint)
       - throughput        (5-min tumbling window per checkpoint)
       - segment_pace      (rolling avg pace between consecutive checkpoints)
       - (completion count is derivable from leaderboard/raw, but we also track it)
  4. Upserts aggregates to SQLite every FLUSH_INTERVAL seconds.

Idempotency: raw_events has a UNIQUE constraint on (race_id, runner_id, checkpoint_km)
so re-delivery of the same event is a no-op. Metrics are recomputed from in-memory
state, so duplicate inputs collapse naturally.
"""
import json
import sqlite3
import time
from collections import defaultdict, deque
from kafka import KafkaConsumer
from config import KAFKA_BOOTSTRAP, TOPIC, DB_PATH, SEGMENT_ORDER

FLUSH_INTERVAL = 2.0          # seconds
WINDOW_SIZE = 300             # 5-minute tumbling windows for throughput
PACE_ROLLING_N = 50           # rolling window size for segment pace


# ---------- DB setup ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS raw_events (
        race_id TEXT, runner_id TEXT, runner_name TEXT,
        checkpoint_label TEXT, checkpoint_km REAL,
        elapsed_seconds INTEGER, event_timestamp REAL, replay_timestamp REAL,
        ingested_at REAL,
        UNIQUE(race_id, runner_id, checkpoint_km)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_cp ON raw_events(checkpoint_km);

    CREATE TABLE IF NOT EXISTS leaderboard (
        race_id TEXT, checkpoint_km REAL, checkpoint_label TEXT,
        runner_id TEXT, runner_name TEXT,
        elapsed_seconds INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, checkpoint_km)
    );

    CREATE TABLE IF NOT EXISTS checkpoint_throughput (
        race_id TEXT, checkpoint_km REAL, window_start INTEGER,
        runner_count INTEGER,
        PRIMARY KEY (race_id, checkpoint_km, window_start)
    );

    CREATE TABLE IF NOT EXISTS segment_pace (
        race_id TEXT, segment TEXT,
        avg_pace_sec_per_km REAL, sample_size INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, segment)
    );

    CREATE TABLE IF NOT EXISTS completion (
        race_id TEXT, checkpoint_km REAL, checkpoint_label TEXT,
        runner_count INTEGER, updated_at REAL,
        PRIMARY KEY (race_id, checkpoint_km)
    );
    """)
    conn.commit()
    return conn


# ---------- In-memory state ----------
class State:
    def __init__(self):
        # (race_id, km) -> {"runner_id","runner_name","elapsed_seconds"}
        self.leaders: dict = {}
        # (race_id, km, window_start) -> count
        self.throughput: dict = defaultdict(int)
        # (race_id, segment) -> deque of pace samples (sec/km)
        self.segment_paces: dict = defaultdict(lambda: deque(maxlen=PACE_ROLLING_N))
        # runner_id -> {checkpoint_label: elapsed_seconds} for segment calc
        self.runner_splits: dict = defaultdict(dict)
        # (race_id, km) -> count
        self.completion: dict = defaultdict(int)

    def update(self, ev):
        race, rid, km = ev["race_id"], ev["runner_id"], ev["checkpoint_km"]
        label, elapsed = ev["checkpoint_label"], ev["elapsed_seconds"]

        # Leaderboard
        key = (race, km)
        cur = self.leaders.get(key)
        if cur is None or elapsed < cur["elapsed_seconds"]:
            self.leaders[key] = {
                "runner_id": rid, "runner_name": ev["runner_name"],
                "elapsed_seconds": elapsed, "label": label,
            }

        # Throughput: tumbling window on event_timestamp
        window_start = int(ev["event_timestamp"] // WINDOW_SIZE) * WINDOW_SIZE
        self.throughput[(race, km, window_start)] += 1

        # Completion
        self.completion[(race, km)] += 1

        # Segment pace: need the previous checkpoint for this runner
        prev = self.runner_splits[rid]
        try:
            idx = SEGMENT_ORDER.index(label)
        except ValueError:
            idx = -1
        if idx > 0:
            prev_label = SEGMENT_ORDER[idx - 1]
            if prev_label in prev:
                from config import CHECKPOINTS
                dist = km - CHECKPOINTS[prev_label]
                dtime = elapsed - prev[prev_label]
                if dist > 0 and dtime > 0:
                    pace = dtime / dist  # seconds per km
                    self.segment_paces[(race, f"{prev_label}->{label}")].append(pace)
        prev[label] = elapsed


# ---------- Flush state to DB ----------
def flush(conn, state: State):
    now = time.time()
    c = conn.cursor()

    # Leaderboard
    for (race, km), v in state.leaders.items():
        c.execute("""
            INSERT INTO leaderboard VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(race_id, checkpoint_km) DO UPDATE SET
              runner_id=excluded.runner_id,
              runner_name=excluded.runner_name,
              elapsed_seconds=excluded.elapsed_seconds,
              updated_at=excluded.updated_at
        """, (race, km, v["label"], v["runner_id"], v["runner_name"],
              v["elapsed_seconds"], now))

    # Throughput
    for (race, km, ws), count in state.throughput.items():
        c.execute("""
            INSERT INTO checkpoint_throughput VALUES (?,?,?,?)
            ON CONFLICT(race_id, checkpoint_km, window_start) DO UPDATE SET
              runner_count=excluded.runner_count
        """, (race, km, ws, count))

    # Segment pace
    for (race, seg), samples in state.segment_paces.items():
        if not samples:
            continue
        avg = sum(samples) / len(samples)
        c.execute("""
            INSERT INTO segment_pace VALUES (?,?,?,?,?)
            ON CONFLICT(race_id, segment) DO UPDATE SET
              avg_pace_sec_per_km=excluded.avg_pace_sec_per_km,
              sample_size=excluded.sample_size,
              updated_at=excluded.updated_at
        """, (race, seg, avg, len(samples), now))

    # Completion
    for (race, km), count in state.completion.items():
        # Find a label — any leader entry at this km has it
        label = state.leaders.get((race, km), {}).get("label", "")
        c.execute("""
            INSERT INTO completion VALUES (?,?,?,?,?)
            ON CONFLICT(race_id, checkpoint_km) DO UPDATE SET
              runner_count=excluded.runner_count,
              updated_at=excluded.updated_at
        """, (race, km, label, count, now))

    conn.commit()


def main():
    conn = init_db()
    state = State()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="marathon-analytics",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10_000,  # exit 10s after stream ends — convenient for demo
    )

    last_flush = time.time()
    processed = 0

    print("Consumer started. Waiting for events...")
    for msg in consumer:
        ev = msg.value
        state.update(ev)

        # Persist raw event (ignore duplicates via UNIQUE constraint)
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
            # Quick CLI heartbeat
            leader_at_half = state.leaders.get(("boston_2015", 21.0975))
            leader_str = f"{leader_at_half['runner_name']} @ Half in {leader_at_half['elapsed_seconds']}s" \
                         if leader_at_half else "no one at Half yet"
            print(f"[{processed:,} events] leader: {leader_str}")

    flush(conn, state)
    print(f"Stream ended. Total processed: {processed:,}")
    conn.close()


if __name__ == "__main__":
    main()
