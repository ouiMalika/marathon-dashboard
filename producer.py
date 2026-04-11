"""
producer.py
-----------
Replay preprocessed events into Kafka in simulated real time.

- Reads data/events.csv (already sorted by elapsed_seconds).
- Sleeps between events proportional to the elapsed-time gap, divided by SPEEDUP.
- Keys messages by race_id so all events for one race land on the same partition,
  preserving checkpoint ordering (crucial for leaderboard correctness).
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP, TOPIC, RACE_START_EPOCH, SPEEDUP

EVENTS_CSV = "data/events.csv"


def main():
    df = pd.read_csv(EVENTS_CSV)
    print(f"Loaded {len(df):,} events. Replaying at {SPEEDUP}x speed.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",           # wait for leader ack — safer, no throughput concern here
        linger_ms=5,          # tiny batching window
    )

    prev_elapsed = 0
    sent = 0
    start_wall = time.time()

    for _, row in df.iterrows():
        gap = max(0, int(row["elapsed_seconds"]) - prev_elapsed)
        if gap:
            time.sleep(gap / SPEEDUP)
        prev_elapsed = int(row["elapsed_seconds"])

        event = {
            "race_id": row["race_id"],
            "runner_id": str(row["runner_id"]),
            "runner_name": row["runner_name"],
            "age": None if pd.isna(row["age"]) else int(row["age"]),
            "gender": row["gender"],
            "checkpoint_label": row["checkpoint_label"],
            "checkpoint_km": float(row["checkpoint_km"]),
            "elapsed_seconds": int(row["elapsed_seconds"]),
            "event_timestamp": RACE_START_EPOCH + int(row["elapsed_seconds"]),
            "replay_timestamp": time.time(),
        }

        producer.send(TOPIC, key=event["race_id"], value=event)
        sent += 1
        if sent % 1000 == 0:
            rate = sent / (time.time() - start_wall)
            print(f"  sent {sent:,} events ({rate:.0f}/s, race_clock={prev_elapsed}s)")

    producer.flush()
    producer.close()
    print(f"Done. Sent {sent:,} events in {time.time() - start_wall:.1f}s.")


if __name__ == "__main__":
    main()
